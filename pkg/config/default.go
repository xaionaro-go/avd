package config

import "github.com/xaionaro-go/recoder"

func Default() Config {
	return Config{
		Ports: []PortConfig{
			{
				Address: "tcp:127.0.0.1:1936",
				RTMP: &RTMPConfig{
					Mode: RTMPModePublishers,
				},
			},
			{
				Address: "tcp:0.0.0.0:1935",
				RTMP: &RTMPConfig{
					Mode: RTMPModeConsumers,
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
