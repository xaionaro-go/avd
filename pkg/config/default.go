package config

import (
	"github.com/xaionaro-go/avd/pkg/avd/types"
	transcodertypes "github.com/xaionaro-go/avpipeline/preset/transcoderwithpassthrough/types"
)

func Default() Config {
	return Config{
		Ports: []PortConfig{
			{
				Address: "tcp:127.0.0.1:1936",
				Mode:    types.PortModePublishers,
				ProtocolHandler: ProtocolHandlerConfig{
					RTMP: &RTMPConfig{},
				},
				CustomOptions: DictionaryItems{},
			},
			{
				Address: "tcp:0.0.0.0:1935",
				Mode:    types.PortModeConsumers,
				ProtocolHandler: ProtocolHandlerConfig{
					RTMP: &RTMPConfig{},
				},
				CustomOptions: DictionaryItems{},
				OnEnd:         types.OnEndActionCloseConsumers,
			},
			{
				Address: "tcp:0.0.0.0:1937",
				Mode:    types.PortModeConsumers,
				ProtocolHandler: ProtocolHandlerConfig{
					RTMP: &RTMPConfig{},
				},
				CustomOptions: DictionaryItems{},
				OnEnd:         types.OnEndActionWaitForNewPublisher,
			},
			{
				Address: "tcp:127.0.0.1:8555",
				Mode:    types.PortModePublishers,
				ProtocolHandler: ProtocolHandlerConfig{
					RTSP: &RTSPConfig{},
				},
				CustomOptions: DictionaryItems{},
			},
			{
				Address:          "udp:127.0.0.1:4445",
				Mode:             types.PortModePublishers,
				DefaultRoutePath: "mystream",
				ProtocolHandler: ProtocolHandlerConfig{
					MPEGTS: &MPEGTSConfig{},
				},
				CustomOptions: DictionaryItems{},
			},
		},
		Endpoints: map[types.RoutePath]EndpointConfig{
			"mystream": {
				Forwardings: []ForwardConfig{{
					Recoding: &transcodertypes.RecoderConfig{
						AudioTracks: []transcodertypes.TrackConfig{{
							InputTrackIDs: []int{0, 1, 2, 3, 4, 5, 6, 7},
							CodecName:     "copy",
							CustomOptions: transcodertypes.DictionaryItems{},
						}},
						VideoTracks: []transcodertypes.TrackConfig{{
							InputTrackIDs: []int{0, 1, 2, 3, 4, 5, 6, 7},
							CodecName:     "copy",
							CustomOptions: transcodertypes.DictionaryItems{},
						}},
					},
				}},
			},
		},
	}
}
