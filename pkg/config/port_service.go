// port_service.go defines the configuration for service ports (e.g., management).

package config

import (
	"github.com/xaionaro-go/avd/pkg/avd/types"
)

type ServicePortConfig struct {
	Address PortAddress   `yaml:"address"`
	Service ServiceConfig `yaml:"service"`
}

type ServiceConfig struct {
	Management *ManagementServiceConfig `yaml:"management,omitempty"`
}

type ManagementServiceConfig struct {
	ServiceProtocol ServiceProtocol `yaml:"protocol"`
}

type ServiceProtocol = types.ServiceProtocol

const (
	ServiceProtocolGRPC = types.ServiceProtocolGRPC
)
