package config

import (
	"github.com/xaionaro-go/avd/pkg/avd/types"
)

type Config struct {
	ServicePorts   []ServicePortConfig                `yaml:"ports_service"`
	StreamingPorts []StreamingPortConfig              `yaml:"ports_streaming"`
	Endpoints      map[types.RoutePath]EndpointConfig `yaml:"endpoints"`
}
