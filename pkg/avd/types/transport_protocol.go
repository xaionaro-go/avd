package types

import (
	"encoding/json"
	"fmt"
	"strings"
)

type TransportProtocol int

const (
	UndefinedTransportProtocol = TransportProtocol(iota)
	TransportProtocolTCP
	TransportProtocolUDP
	EndOfTransportProtocol
)

func (c TransportProtocol) String() string {
	switch c {
	case TransportProtocolTCP:
		return "tcp"
	case TransportProtocolUDP:
		return "udp"
	default:
		return ""
	}
}

func (c *TransportProtocol) UnmarshalYAML(b []byte) error {
	var modeString string
	if err := json.Unmarshal(b, &modeString); err != nil {
		return err
	}

	modeString = strings.Trim(strings.ToLower(modeString), " ")
	for candidate := TransportProtocol(0); candidate < EndOfTransportProtocol; candidate++ {
		if candidate.String() == modeString {
			*c = candidate
			return nil
		}
	}

	return fmt.Errorf("unknown port mode: '%s'", modeString)
}

func (c TransportProtocol) MarshalYAML() ([]byte, error) {
	return json.Marshal(c.String())
}
