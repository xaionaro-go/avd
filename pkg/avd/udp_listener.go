package avd

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/go-ng/xatomic"
)

// UDPListener implements a virtual listener for UDP "connections" (sessions).
type UDPListener struct {
	*net.UDPConn
	connections map[string]*UDPConn
	acceptChan  chan net.Conn
	locker      sync.Mutex
	ctx         context.Context
	cancel      context.CancelFunc
}

func NewUDPListener(network, laddr string) (*UDPListener, error) {
	addr, err := net.ResolveUDPAddr(network, laddr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP(network, addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	l := &UDPListener{
		UDPConn:     conn,
		connections: make(map[string]*UDPConn),
		acceptChan:  make(chan net.Conn, 1024),
		ctx:         ctx,
		cancel:      cancel,
	}
	go l.serve()
	return l, nil
}

func (l *UDPListener) serve() {
	buf := make([]byte, 65535)
	for {
		n, addr, err := l.ReadFromUDP(buf)
		if err != nil {
			logger.Tracef(l.ctx, "UDPListener.serve: ReadFromUDP error: %v", err)
			return
		}
		logger.Tracef(l.ctx, "UDPListener.serve: read %d bytes from %v", n, addr)

		l.locker.Lock()
		conn, ok := l.connections[addr.String()]
		if !ok {
			logger.Tracef(l.ctx, "UDPListener.serve: new connection from %v", addr)
			conn = &UDPConn{
				parent:     l,
				remoteAddr: addr,
				readChan:   make(chan []byte, 1000),
			}
			conn.deadlineUpdated.Store(make(chan struct{}))
			l.connections[addr.String()] = conn
			select {
			case l.acceptChan <- conn:
			default:
				logger.Errorf(l.ctx, "acceptChan full, dropping new connection from %v", addr)
				delete(l.connections, addr.String())
				conn = nil
			}
			l.locker.Unlock()
		} else {
			l.locker.Unlock()
		}
		if conn == nil {
			continue
		}

		data := make([]byte, n)
		copy(data, buf[:n])
		select {
		case conn.readChan <- data:
			logger.Tracef(l.ctx, "UDPListener.serve: queued %d bytes for %v", n, addr)
		default:
			logger.Tracef(l.ctx, "UDPListener.serve: readChan full for %v, dropping packet", addr)
		}
	}
}

func (l *UDPListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.acceptChan:
		logger.Debugf(l.ctx, "UDPListener.Accept: returning connection from %v", conn.RemoteAddr())
		return conn, nil
	case <-l.ctx.Done():
		return nil, l.ctx.Err()
	}
}

func (l *UDPListener) Close() error {
	l.cancel()
	return l.UDPConn.Close()
}

func (l *UDPListener) Addr() net.Addr {
	return l.UDPConn.LocalAddr()
}

type UDPConn struct {
	parent          *UDPListener
	remoteAddr      *net.UDPAddr
	readChan        chan []byte
	readBuf         []byte
	closed          bool
	locker          sync.Mutex
	readDeadline    time.Time
	writeDeadline   time.Time
	deadlineUpdated xatomic.Value[chan struct{}]
}

func (c *UDPConn) Read(b []byte) (int, error) {
	logger.Tracef(c.parent.ctx, "UDPConn.Read called for %v", c.remoteAddr)

	for {
		c.locker.Lock()
		if c.closed {
			c.locker.Unlock()
			return 0, io.EOF
		}
		if len(c.readBuf) > 0 {
			n := copy(b, c.readBuf)
			c.readBuf = c.readBuf[n:]
			c.locker.Unlock()
			return n, nil
		}
		updatedCh := c.deadlineUpdated.Load()
		deadline := c.readDeadline
		c.locker.Unlock()

		ctx, cancel := context.WithCancel(c.parent.ctx)
		if !deadline.IsZero() {
			cancel()
			ctx, cancel = context.WithDeadline(c.parent.ctx, deadline)
		}

		select {
		case data, ok := <-c.readChan:
			cancel()
			if !ok {
				return 0, io.EOF
			}
			logger.Tracef(c.parent.ctx, "UDPConn.Read got %d bytes for %v", len(data), c.remoteAddr)
			n := copy(b, data)
			if n < len(data) {
				c.locker.Lock()
				c.readBuf = append(c.readBuf, data[n:]...)
				c.locker.Unlock()
			}
			return n, nil
		case <-ctx.Done():
			err := ctx.Err()
			cancel()
			return 0, err
		case <-updatedCh:
			cancel()
			// Deadline updated, loop again to pick up new deadline
			continue
		}
	}
}

func (c *UDPConn) Write(b []byte) (int, error) {
	c.locker.Lock()
	deadline := c.writeDeadline
	c.locker.Unlock()

	if !deadline.IsZero() && time.Now().After(deadline) {
		return 0, context.DeadlineExceeded
	}

	logger.Tracef(c.parent.ctx, "UDPConn.Write sending %d bytes to %v", len(b), c.remoteAddr)
	return c.parent.WriteToUDP(b, c.remoteAddr)
}

func (c *UDPConn) Close() error {
	c.locker.Lock()
	defer c.locker.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	c.parent.locker.Lock()
	delete(c.parent.connections, c.remoteAddr.String())
	c.parent.locker.Unlock()
	return nil
}

func (c *UDPConn) LocalAddr() net.Addr  { return c.parent.LocalAddr() }
func (c *UDPConn) RemoteAddr() net.Addr { return c.remoteAddr }

func (c *UDPConn) pulseDeadlineInternal() {
	close(c.deadlineUpdated.Swap(make(chan struct{})))
}

func (c *UDPConn) SetDeadline(t time.Time) error {
	c.locker.Lock()
	defer c.locker.Unlock()
	c.readDeadline = t
	c.writeDeadline = t
	c.pulseDeadlineInternal()
	return nil
}
func (c *UDPConn) SetReadDeadline(t time.Time) error {
	c.locker.Lock()
	defer c.locker.Unlock()
	c.readDeadline = t
	c.pulseDeadlineInternal()
	return nil
}
func (c *UDPConn) SetWriteDeadline(t time.Time) error {
	c.locker.Lock()
	defer c.locker.Unlock()
	c.writeDeadline = t
	c.pulseDeadlineInternal()
	return nil
}
