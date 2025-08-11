package config

import (
	"github.com/xaionaro-go/avd/pkg/avd/types"
	"github.com/xaionaro-go/avpipeline/codec"
	transcodertypes "github.com/xaionaro-go/avpipeline/preset/transcoderwithpassthrough/types"
)

func Default() Config {
	return Config{
		ServicePorts: []ServicePortConfig{
			{
				Address: "tcp:127.0.0.1:1722",
				Service: ServiceConfig{
					Management: &ManagementServiceConfig{
						ServiceProtocol: ServiceProtocolGRPC,
					},
				},
			},
		},
		StreamingPorts: []StreamingPortConfig{
			{
				Address: "tcp:127.0.0.1:1936",
				Mode:    types.PortModePublishers,
				ProtocolHandler: StreamingProtocolHandlerConfig{
					RTMP: &RTMPConfig{},
				},
				CustomOptions: DictionaryItems{},
			},
			{
				Address: "tcp:0.0.0.0:1935",
				Mode:    types.PortModeConsumers,
				ProtocolHandler: StreamingProtocolHandlerConfig{
					RTMP: &RTMPConfig{},
				},
				CustomOptions: DictionaryItems{},
				OnEnd:         types.OnEndActionCloseConsumers,
			},
			{
				Address: "tcp:0.0.0.0:1937",
				Mode:    types.PortModeConsumers,
				ProtocolHandler: StreamingProtocolHandlerConfig{
					RTMP: &RTMPConfig{},
				},
				CustomOptions: DictionaryItems{},
				OnEnd:         types.OnEndActionWaitForNewPublisher,
			},
			{
				Address: "tcp:127.0.0.1:8555",
				Mode:    types.PortModePublishers,
				ProtocolHandler: StreamingProtocolHandlerConfig{
					RTSP: &RTSPConfig{},
				},
				CustomOptions: DictionaryItems{},
			},
			{
				Address:          "udp:127.0.0.1:4445",
				Mode:             types.PortModePublishers,
				DefaultRoutePath: "mystream",
				ProtocolHandler: StreamingProtocolHandlerConfig{
					MPEGTS: &MPEGTSConfig{},
				},
				CustomOptions: DictionaryItems{},
			},
		},
		Endpoints: map[types.RoutePath]EndpointConfig{
			"mystream": {
				OnPublisherAdded: &Command{
					Command: []string{"date", "+%s"},
					Restart: RestartPolicyNever,
				},
				Forwardings: []ForwardConfig{{
					Recoding: &transcodertypes.RecoderConfig{
						AudioTrackConfigs: []transcodertypes.AudioTrackConfig{{
							InputTrackIDs:  []int{0, 1, 2, 3, 4, 5, 6, 7},
							OutputTrackIDs: []int{0},
							CodecName:      codec.CodecNameCopy,
							CustomOptions:  transcodertypes.DictionaryItems{},
						}},
						VideoTrackConfigs: []transcodertypes.VideoTrackConfig{{
							InputTrackIDs:  []int{0, 1, 2, 3, 4, 5, 6, 7},
							OutputTrackIDs: []int{1},
							CodecName:      codec.CodecNameCopy,
							CustomOptions:  transcodertypes.DictionaryItems{},
						}},
					},
				}},
			},
		},
	}
}
