package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/spf13/cobra"
	"github.com/xaionaro-go/avd/pkg/management/grpc/client"
	avpipeline_proto "github.com/xaionaro-go/avpipeline/protobuf/avpipeline"
	goconvlibav "github.com/xaionaro-go/avpipeline/protobuf/goconv/libav"
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
		Run:  monitor,
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

	Root.AddCommand(Monitor)
	Monitor.Flags().Bool("include-packet-payload", false, "include packet payloads in monitor events")
	Monitor.Flags().Bool("include-frame-payload", false, "include frame payloads in monitor events")
	Monitor.Flags().Bool("do-decode", false, "do decode of packets/frames for monitor events")
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

func monitor(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()

	remoteAddr, err := cmd.Flags().GetString("remote-addr")
	assertNoError(ctx, err)
	client, err := client.New(ctx, remoteAddr)
	assertNoError(ctx, err)

	objID, err := strconv.ParseUint(args[0], 10, 64)
	assertNoError(ctx, err)

	evenType := avpipeline_proto.MonitorEventType_EVENT_TYPE_SEND
	if len(args) >= 2 {
		switch strings.ToLower(args[1]) {
		case "send":
			evenType = avpipeline_proto.MonitorEventType_EVENT_TYPE_SEND
		case "receive":
			evenType = avpipeline_proto.MonitorEventType_EVENT_TYPE_RECEIVE
		case "kernel_output_send":
			evenType = avpipeline_proto.MonitorEventType_EVENT_TYPE_KERNEL_OUTPUT_SEND
		default:
			logger.Panicf(ctx, "unknown event type: %q", args[1])
		}
	}

	includePacketPayload, err := cmd.Flags().GetBool("include-packet-payload")
	assertNoError(ctx, err)
	includeFramePayload, err := cmd.Flags().GetBool("include-frame-payload")
	assertNoError(ctx, err)
	doDecode, err := cmd.Flags().GetBool("do-decode")
	assertNoError(ctx, err)
	format, err := cmd.Flags().GetString("format")
	assertNoError(ctx, err)

	const eventFormatString = "%-21s %-10s %-10s %-14s %-10s %-14s %-10s %-14s %-10s %-10s %-10s %-10s\n"
	switch format {
	case "plaintext":
		fmt.Printf(eventFormatString, "TS", "streamIdx", "PTS", "PTS", "DTS", "DTS", "dur", "dur", "size", "type", "frameFlags", "picType")
	case "json":
	default:
		logger.Panicf(ctx, "unknown format: %q", format)
	}

	eventsCh, err := client.Monitor(ctx, objID, evenType, includePacketPayload, includeFramePayload, doDecode)
	assertNoError(ctx, err)

	logger.Infof(ctx, "monitoring started for object ID %d, event type %s", objID, evenType.String())
	streamSeen := map[int]struct{}{}
	for ev := range eventsCh {
		if _, ok := streamSeen[int(ev.Stream.Index)]; !ok {
			fmt.Printf("= new stream: %d; codec: 0x%X: time_base: %s\n", ev.Stream.Index, ev.Stream.CodecParameters.CodecId, ev.Stream.TimeBase)
			streamSeen[int(ev.Stream.Index)] = struct{}{}
		}
		switch format {
		case "plaintext":
			timeBase := goconvlibav.RationalFromProtobuf(ev.Stream.GetTimeBase())
			if ev.Packet != nil && len(ev.Frames) == 0 {
				pkt := ev.Packet
				fmt.Printf(eventFormatString,
					fmt.Sprintf("%d", ev.GetTimestampNs()),
					fmt.Sprintf("%d", ev.Stream.Index),
					fmt.Sprintf("%d", pkt.Pts),
					avconvDuration(pkt.Pts, timeBase),
					fmt.Sprintf("%d", pkt.Dts),
					avconvDuration(pkt.Dts, timeBase),
					fmt.Sprintf("%d", pkt.Duration),
					avconvDuration(pkt.Duration, timeBase),
					fmt.Sprintf("%d", pkt.DataSize),
					fmt.Sprintf("%d", ev.Stream.CodecParameters.GetCodecType()),
					"-",
					"-",
				)
			}
			for _, frame := range ev.Frames {
				fmt.Printf(eventFormatString,
					fmt.Sprintf("%d", ev.GetTimestampNs()),
					fmt.Sprintf("%d", ev.Stream.Index),
					fmt.Sprintf("%d", frame.Pts),
					avconvDuration(frame.Pts, timeBase),
					fmt.Sprintf("%d", frame.PktDts),
					avconvDuration(frame.PktDts, timeBase),
					fmt.Sprintf("%d", frame.Duration),
					avconvDuration(frame.Duration, timeBase),
					fmt.Sprintf("%d", frame.DataSize),
					fmt.Sprintf("%d", ev.Stream.CodecParameters.GetCodecType()),
					fmt.Sprintf("0x%08X", frame.Flags),
					fmt.Sprintf("0x%08X", frame.PictType),
				)
			}
		case "json":
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			err = enc.Encode(ev)
			assertNoError(ctx, err)
		}
	}
}

func avconvDuration(pts int64, timeBase *goconvlibav.Rational) time.Duration {
	return time.Duration(int64(time.Second) * pts * timeBase.N / timeBase.D)
}
