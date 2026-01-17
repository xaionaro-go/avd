//go:build !linux && !darwin && !windows

package avd

import (
	"net"
)

func isLocalUDPPortBound(port uint16) (bool, error) {
	// Fallback for non-supported OSes: use the old "bind and see" method
	addr := &net.UDPAddr{Port: int(port)}
	l, err := net.ListenUDP("udp", addr)
	if err == nil {
		l.Close()
		return false, nil
	}
	return true, nil
}
