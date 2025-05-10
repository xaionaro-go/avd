package config

import (
	"github.com/xaionaro-go/avd/pkg/avd/types"
	"github.com/xaionaro-go/recoder"
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
					Recoding: &recoder.EncodersConfig{},
				}},
			},
		},
	}
}
