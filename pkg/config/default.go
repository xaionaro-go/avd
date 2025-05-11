package config

import (
	"github.com/xaionaro-go/avd/pkg/avd/types"
	transcodertypes "github.com/xaionaro-go/avpipeline/chain/transcoderwithpassthrough/types"
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
				Address: "tcp:0.0.0.0:8554",
				Mode:    types.PortModeConsumers,
				ProtocolHandler: ProtocolHandlerConfig{
					RTSP: &RTSPConfig{},
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
						}},
						VideoTracks: []transcodertypes.TrackConfig{{
							InputTrackIDs: []int{0, 1, 2, 3, 4, 5, 6, 7},
							CodecName:     "copy",
						}},
					},
				}},
			},
		},
	}
}
