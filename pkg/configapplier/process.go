// process.go manages the execution of external processes.

package configapplier

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/avd/pkg/config"
	"github.com/xaionaro-go/observability"
	"github.com/xaionaro-go/xsync"
)

const (
	minRestartInterval = time.Second
)

type process struct {
	Locker     xsync.Mutex
	CancelFunc context.CancelFunc
	Config     config.Command
	Cmd        *exec.Cmd
}

func newProcess(
	_ context.Context,
	cfg config.Command,
) *process {
	return &process{
		Config: cfg,
	}
}

func (p *process) Start(
	ctx context.Context,
) {
	ctx, cancelFn := context.WithCancel(ctx)
	p.CancelFunc = cancelFn
	observability.Go(ctx, func(ctx context.Context) {
		err := p.runLoop(ctx)
		if err != nil {
			logger.Errorf(ctx, "unable to run the command %#+v: %v", p.Config, err)
		}
	})
}

func (p *process) runLoop(
	ctx context.Context,
) (_err error) {
	logger.Debugf(ctx, "running command %v", p.Config)
	defer func() { logger.Debugf(ctx, "finished running command %v: %v", p.Config, _err) }()
	t := time.NewTicker(minRestartInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
		var cmd *exec.Cmd
		p.Locker.Do(ctx, func() {
			cmd = exec.CommandContext(ctx, p.Config.Command[0], p.Config.Command[1:]...)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			err := cmd.Start()
			if err != nil {
				logger.Errorf(ctx, "unable to execute command %#+v: %v", p.Config, err)
				if cmd.Process != nil {
					cmd.Process.Kill()
				}
				return
			}
			p.Cmd = cmd
		})
		cmdString := strings.Join(p.Config.Command, " ")
		logger.Infof(ctx, "launched command '%s'", cmdString)
		err := cmd.Wait()
		if err != nil {
			err = fmt.Errorf("command '%s' finished with an error: %w", cmdString, err)
			logger.Errorf(ctx, "%v", err)
		}
		switch p.Config.Restart {
		case config.RestartPolicyNever:
			return err
		}
	}
}

func (p *process) Close(
	ctx context.Context,
) {
	p.Locker.Do(ctx, func() {
		if p.CancelFunc != nil {
			p.CancelFunc()
			p.CancelFunc = nil
		}
		if p.Cmd != nil {
			if p.Cmd.Process != nil {
				err := p.Cmd.Process.Kill()
				logger.Debugf(ctx, "kill result: %v", err)
			}
			p.Cmd = nil
		}
	})
}
