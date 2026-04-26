// main.go is the entry point for the avd server.

// Package main is the entry point for the avd server.
package main

import (
	"context"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"time"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/spf13/pflag"
	"github.com/xaionaro-go/avd/pkg/avd"
	"github.com/xaionaro-go/avd/pkg/config"
	"github.com/xaionaro-go/avd/pkg/configapplier"
	"github.com/xaionaro-go/avd/pkg/configfile"
	"github.com/xaionaro-go/observability"
)

func main() {
	loggerLevel := logger.LevelInfo
	pflag.Var(&loggerLevel, "log-level", "Log level")
	configPathsFlag := pflag.String(
		"config-path",
		"~/.avd.conf:/etc/avd/avd.conf",
		"the path to the config file",
	)
	netPprofAddr := pflag.String(
		"go-net-pprof-addr",
		"",
		"address to listen to for net/pprof requests",
	)
	generateConfig := pflag.Bool(
		"generate-config",
		false,
		"",
	)
	versionFlag := pflag.Bool(
		"version",
		false,
		"print version and exit",
	)
	pflag.Parse()
	ctx := withLogger(context.Background(), loggerLevel)

	if *generateConfig {
		config.Default().WriteTo(os.Stdout)
		os.Exit(0)
	}

	if *versionFlag {
		printBuildInfo(ctx, os.Stdout)
		os.Exit(0)
	}

	if *netPprofAddr != "" {
		observability.Go(ctx, func(ctx context.Context) {
			logger.Infof(ctx, "starting to listen for net/pprof requests at '%s'", *netPprofAddr)
			logger.Error(ctx, http.ListenAndServe(*netPprofAddr, nil))
		})
	}

	configPaths := strings.Split(*configPathsFlag, ":")

	var cfg config.Config
	for _, configPath := range configPaths {
		exists, err := configfile.Read(ctx, configPath, &cfg, getConfigVars())
		if !exists {
			continue
		}
		assertNoError(ctx, err)
		break
	}

	srv := avd.NewServer(ctx)
	err := configapplier.ApplyConfig(ctx, cfg, srv)
	assertNoError(ctx, err)

	// just to keep some concurrent logs before message "started": which is not necessary, but just makes a bit easier to read the logs:
	time.Sleep(time.Millisecond)

	logger.Infof(ctx, "started...")
	srv.Wait(ctx)
	logger.Errorf(ctx, "the server exited")
}

func getConfigVars() map[string]any {
	envs := map[string]string{}
	for _, env := range os.Environ() {
		parts := strings.SplitN(env, "=", 2)
		if len(parts) != 2 {
			continue
		}
		envs[parts[0]] = parts[1]
	}
	return map[string]any{
		"env": envs,
	}
}
