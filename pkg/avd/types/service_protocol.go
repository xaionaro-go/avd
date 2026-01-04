// service_protocol.go defines the ServiceProtocol type for management services.

package types

import (
	"encoding/json"
	"fmt"
	"strings"
)

type ServiceProtocol int

const (
	UndefinedServiceProtocol = ServiceProtocol(iota)
	ServiceProtocolGRPC
	EndOfServiceProtocol
)

func (c ServiceProtocol) String() string {
	switch c {
	case ServiceProtocolGRPC:
		return "gRPC"
	default:
		return ""
	}
}

func (c *ServiceProtocol) UnmarshalYAML(b []byte) error {
	var modeString string
	if err := json.Unmarshal(b, &modeString); err != nil {
		return err
	}

	modeString = strings.Trim(strings.ToLower(modeString), " ")
	for candidate := range EndOfServiceProtocol {
		if strings.ToLower(candidate.String()) == modeString {
			*c = candidate
			return nil
		}
	}

	return fmt.Errorf("unknown service protocol: '%s'", modeString)
}

func (c ServiceProtocol) MarshalYAML() ([]byte, error) {
	return json.Marshal(c.String())
}
