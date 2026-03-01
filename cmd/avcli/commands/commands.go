// commands.go defines the CLI commands for avcli.

// Package commands provides the CLI commands for avcli.
package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/spf13/cobra"
	"github.com/xaionaro-go/avd/pkg/management/grpc/client"
	"github.com/xaionaro-go/avd/pkg/management/grpc/proto/avdmanagementgrpc"
	"github.com/xaionaro-go/avpipeline/monitor"
	avpipeline_proto "github.com/xaionaro-go/avpipeline/protobuf/avpipeline"
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

	Routes = &cobra.Command{
		Use: "routes",
	}

	RoutesList = &cobra.Command{
		Use:  "list",
		Args: cobra.ExactArgs(0),
		Run:  routesList,
	}

	Monitor = &cobra.Command{
		Use:  "monitor",
		Args: cobra.RangeArgs(1, 2),
		Run:  monitorCommand,
	}

	PrivacyBlur = &cobra.Command{
		Use: "privacy-blur",
	}

	PrivacyBlurSet = &cobra.Command{
		Use:  "set <route-path> <forwarding-index>",
		Args: cobra.ExactArgs(2),
		Run:  privacyBlurSetCommand,
	}

	PrivacyBlurGet = &cobra.Command{
		Use:  "get <route-path> <forwarding-index>",
		Args: cobra.ExactArgs(2),
		Run:  privacyBlurGetCommand,
	}

	LoggerLevel = logger.LevelWarning
)

func init() {
	Root.AddCommand(Publishers)
	Publishers.AddCommand(PublishersList)

	Root.AddCommand(Consumers)
	Consumers.AddCommand(ConsumersList)

	Root.AddCommand(Routes)
	Routes.AddCommand(RoutesList)

	Root.AddCommand(PrivacyBlur)
	PrivacyBlur.AddCommand(PrivacyBlurSet)
	PrivacyBlur.AddCommand(PrivacyBlurGet)
	PrivacyBlurSet.Flags().Bool("enabled", false, "enable or disable privacy blur")
	PrivacyBlurSet.Flags().Bool("no-enabled", false, "explicitly set enabled to false")
	PrivacyBlurSet.Flags().Float64("blur-radius", 0, "Gaussian blur radius")
	PrivacyBlurSet.Flags().Int64("pixelate-block-size", 0, "pixelation block size (0 = use Gaussian)")

	Root.AddCommand(Monitor)
	Monitor.Flags().Bool("include-packet-payload", false, "include packet payloads in monitor events")
	Monitor.Flags().Bool("include-frame-payload", false, "include frame payloads in monitor events")
	Monitor.Flags().Bool("do-decode", false, "do decode of packets/frames for monitor events")
	Monitor.Flags().Duration("highlight-discontinuity", 0, "highlight discontinuities (if the gap is greater than the specified duration)")
	Monitor.Flags().IntSlice("stream-indices", nil, "filter by stream indices")
	Monitor.Flags().String("format", "plaintext", "output format (plaintext|json)")

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

func routesList(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()

	remoteAddr, err := cmd.Flags().GetString("remote-addr")
	assertNoError(ctx, err)
	avdClient, err := client.New(ctx, remoteAddr)
	assertNoError(ctx, err)

	resp, err := avdClient.ListRoutes(ctx)
	assertNoError(ctx, err)

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	err = enc.Encode(resp)
	assertNoError(ctx, err)
}

func monitorCommand(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()

	remoteAddr, err := cmd.Flags().GetString("remote-addr")
	assertNoError(ctx, err)
	client, err := client.New(ctx, remoteAddr)
	assertNoError(ctx, err)

	objID, err := strconv.ParseUint(args[0], 10, 64)
	assertNoError(ctx, err)

	evenType := avpipeline_proto.MonitorEventType_EVENT_TYPE_SEND
	if len(args) >= 2 {
		var err error
		evenType, err = monitor.ParseEventType(args[1])
		assertNoError(ctx, err)
	}

	includePacketPayload, err := cmd.Flags().GetBool("include-packet-payload")
	assertNoError(ctx, err)
	includeFramePayload, err := cmd.Flags().GetBool("include-frame-payload")
	assertNoError(ctx, err)
	doDecode, err := cmd.Flags().GetBool("do-decode")
	assertNoError(ctx, err)
	format, err := cmd.Flags().GetString("format")
	assertNoError(ctx, err)
	highlightDiscontinuity, err := cmd.Flags().GetDuration("highlight-discontinuity")
	assertNoError(ctx, err)
	streamIndices, err := cmd.Flags().GetIntSlice("stream-indices")
	assertNoError(ctx, err)
	if len(streamIndices) == 0 {
		streamIndices = nil
	}

	eventsCh, err := client.Monitor(ctx, objID, evenType, includePacketPayload, includeFramePayload, doDecode)
	assertNoError(ctx, err)

	logger.Infof(ctx, "monitoring started for object ID %d, event type %s", objID, evenType.String())
	err = monitor.PrintMonitorEvents(ctx, os.Stdout, eventsCh, monitor.PrintOptions{
		Format:                 format,
		HighlightDiscontinuity: highlightDiscontinuity,
		StreamIndices:          streamIndices,
	})
	assertNoError(ctx, err)
}

func privacyBlurSetCommand(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()

	remoteAddr, err := cmd.Flags().GetString("remote-addr")
	assertNoError(ctx, err)
	avdClient, err := client.New(ctx, remoteAddr)
	assertNoError(ctx, err)

	fwdIndex, err := strconv.ParseInt(args[1], 10, 32)
	assertNoError(ctx, err)

	req := &avdmanagementgrpc.SetPrivacyBlurRequest{
		RoutePath:       args[0],
		ForwardingIndex: int32(fwdIndex),
	}

	if cmd.Flags().Changed("enabled") || cmd.Flags().Changed("no-enabled") {
		v, err := cmd.Flags().GetBool("enabled")
		assertNoError(ctx, err)
		if cmd.Flags().Changed("no-enabled") {
			v = false
		}
		req.Enabled = &v
	}
	if cmd.Flags().Changed("blur-radius") {
		v, err := cmd.Flags().GetFloat64("blur-radius")
		assertNoError(ctx, err)
		req.BlurRadius = &v
	}
	if cmd.Flags().Changed("pixelate-block-size") {
		v, err := cmd.Flags().GetInt64("pixelate-block-size")
		assertNoError(ctx, err)
		req.PixelateBlockSize = &v
	}

	_, err = avdClient.SetPrivacyBlur(ctx, req)
	assertNoError(ctx, err)
	fmt.Println("OK")
}

func privacyBlurGetCommand(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()

	remoteAddr, err := cmd.Flags().GetString("remote-addr")
	assertNoError(ctx, err)
	avdClient, err := client.New(ctx, remoteAddr)
	assertNoError(ctx, err)

	fwdIndex, err := strconv.ParseInt(args[1], 10, 32)
	assertNoError(ctx, err)

	resp, err := avdClient.GetPrivacyBlur(ctx, &avdmanagementgrpc.GetPrivacyBlurRequest{
		RoutePath:       args[0],
		ForwardingIndex: int32(fwdIndex),
	})
	assertNoError(ctx, err)

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	err = enc.Encode(resp)
	assertNoError(ctx, err)
}
