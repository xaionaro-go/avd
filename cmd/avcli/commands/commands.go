package commands

import (
	"context"
	"encoding/json"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/spf13/cobra"
	"github.com/xaionaro-go/avd/pkg/management/grpc/client"
	"github.com/xaionaro-go/observability"
)

var (
	// Access these variables only from a main package:

	Root = &cobra.Command{
		Use: os.Args[0],
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			ctx := cmd.Context()
			l := logger.FromCtx(ctx).WithLevel(LoggerLevel)
			ctx = logger.CtxWithLogger(ctx, l)
			cmd.SetContext(ctx)
			logger.Debugf(ctx, "log-level: %v", LoggerLevel)

			netPprofAddr, err := cmd.Flags().GetString("go-net-pprof-addr")
			if err != nil {
				l.Error("unable to get the value of the flag 'go-net-pprof-addr': %v", err)
			}
			if netPprofAddr != "" {
				observability.Go(ctx, func(ctx context.Context) {
					if netPprofAddr == "" {
						netPprofAddr = "localhost:0"
					}
					l.Infof("starting to listen for net/pprof requests at '%s'", netPprofAddr)
					l.Error(http.ListenAndServe(netPprofAddr, nil))
				})
			}
		},
		PersistentPostRun: func(cmd *cobra.Command, args []string) {
			ctx := cmd.Context()
			logger.Debug(ctx, "end")
		},
	}

	Publishers = &cobra.Command{
		Use: "publishers",
	}

	PublishersList = &cobra.Command{
		Use:  "list",
		Args: cobra.ExactArgs(0),
		Run:  publishersList,
	}

	Consumers = &cobra.Command{
		Use: "consumers",
	}

	ConsumersList = &cobra.Command{
		Use:  "list",
		Args: cobra.ExactArgs(0),
		Run:  consumersList,
	}

	LoggerLevel = logger.LevelWarning
)

func init() {
	Root.AddCommand(Publishers)
	Publishers.AddCommand(PublishersList)

	Root.AddCommand(Consumers)
	Consumers.AddCommand(ConsumersList)

	Root.PersistentFlags().Var(&LoggerLevel, "log-level", "")
	Root.PersistentFlags().String("remote-addr", "localhost:3594", "the path to the config file")
	Root.PersistentFlags().String("go-net-pprof-addr", "", "address to listen to for net/pprof requests")
}

func assertNoError(ctx context.Context, err error) {
	if err != nil {
		logger.Panic(ctx, err)
	}
}

func publishersList(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()

	remoteAddr, err := cmd.Flags().GetString("remote-addr")
	assertNoError(ctx, err)
	avdClient, err := client.New(ctx, remoteAddr)
	assertNoError(ctx, err)

	resp, err := avdClient.ListPublishers(ctx)
	assertNoError(ctx, err)

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	err = enc.Encode(resp)
	assertNoError(ctx, err)
}

func consumersList(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()

	remoteAddr, err := cmd.Flags().GetString("remote-addr")
	assertNoError(ctx, err)
	avdClient, err := client.New(ctx, remoteAddr)
	assertNoError(ctx, err)

	resp, err := avdClient.ListConsumers(ctx)
	assertNoError(ctx, err)

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	err = enc.Encode(resp)
	assertNoError(ctx, err)
}
