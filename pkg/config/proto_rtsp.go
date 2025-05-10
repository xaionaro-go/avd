package config

import (
	"github.com/xaionaro-go/avd/pkg/avd/types"
)

type RTSPConfig struct {
	TransportProtocol types.TransportProtocol `yaml:"transport_protocol"`
}
