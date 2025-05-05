package config

import (
	"github.com/xaionaro-go/recoder"
)

type Destination struct {
	URL   string `yaml:"url"`
	Route string `yaml:"route"`
}

type ForwardConfig struct {
	Destination Destination             `yaml:"destination"`
	Recoding    *recoder.EncodersConfig `yaml:"recoding"`
}

type EndpointConfig struct {
	Forwardings []ForwardConfig `yaml:"forwardings"`
}

type PortConfig struct {
	Address string      `yaml:"address"`
	RTMP    *RTMPConfig `yaml:"rtmp"`
}

type Config struct {
	Ports     []PortConfig              `yaml:"ports"`
	Endpoints map[string]EndpointConfig `yaml:"endpoints"`
}
