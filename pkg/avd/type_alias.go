// type_alias.go provides type aliases for types defined in the types subpackage.

package avd

import (
	"github.com/xaionaro-go/avd/pkg/avd/types"
)

type PortMode = types.StreamingPortMode

const (
	UndefinedPortMode  = types.UndefinedPortMode
	PortModePublishers = types.PortModePublishers
	PortModeConsumers  = types.PortModeConsumers
)

type Protocol = types.StreamingProtocol

const (
	UndefinedProtocol = types.UndefinedProtocol
	ProtocolRTMP      = types.ProtocolRTMP
	ProtocolRTSP      = types.ProtocolRTSP
	ProtocolSRT       = types.ProtocolSRT
	ProtocolMPEGTSUDP = types.ProtocolMPEGTS
)

func SupportedProtocols() []Protocol {
	return types.SupportedProtocols()
}

type (
	ListenConfig               = types.ListenConfig
	ListenOption               = types.ListenOption
	ListenOptions              = types.ListenOptions
	ListenOptionDefaultAppName = types.ListenOptionDefaultRoutePath
)

type TransportProtocol = types.TransportProtocol

const (
	UndefinedTransportProtocol = types.UndefinedTransportProtocol
	TransportProtocolTCP       = types.TransportProtocolTCP
	TransportProtocolUDP       = types.TransportProtocolUDP
)

type (
	RoutePath   = types.RoutePath
	PortAddress = types.PortAddress
)
