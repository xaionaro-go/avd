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
				RTMP: &RTMPConfig{
					Mode: types.RTMPModePublishers,
				},
			},
			{
				Address: "tcp:0.0.0.0:1935",
				RTMP: &RTMPConfig{
					Mode: types.RTMPModeConsumers,
				},
			},
		},
		Endpoints: map[string]EndpointConfig{
			"mystream": {
				Forwardings: []ForwardConfig{{
					Recoding: &recoder.EncodersConfig{},
				}},
			},
		},
	}
}
