package avd

import (
	"github.com/xaionaro-go/avd/pkg/avd/types"
)

type PortMode = types.PortMode

const (
	UndefinedPortMode  = types.UndefinedPortMode
	PortModePublishers = types.PortModePublishers
	PortModeConsumers  = types.PortModeConsumers
)

type Protocol = types.Protocol

const (
	UndefinedProtocol = types.UndefinedProtocol
	ProtocolRTMP      = types.ProtocolRTMP
	ProtocolRTSP      = types.ProtocolRTSP
	ProtocolSRT       = types.ProtocolSRT
)

func SupportedProtocols() []Protocol {
	return types.SupportedProtocols()
}

type ListenConfig = types.ListenConfig
type ListenOption = types.ListenOption
type ListenOptions = types.ListenOptions
type ListenOptionDefaultAppName = types.ListenOptionDefaultRoutePath

type TransportProtocol = types.TransportProtocol

const (
	UndefinedTransportProtocol = types.UndefinedTransportProtocol
	TransportProtocolTCP       = types.TransportProtocolTCP
	TransportProtocolUDP       = types.TransportProtocolUDP
)
