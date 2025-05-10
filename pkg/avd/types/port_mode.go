package types

import (
	"encoding/json"
	"fmt"
	"strings"
)

type PortMode int

const (
	UndefinedPortMode = PortMode(iota)
	PortModeConsumers
	PortModePublishers
	EndOfPortMode
)

func (c PortMode) String() string {
	switch c {
	case PortModeConsumers:
		return "consumers"
	case PortModePublishers:
		return "publishers"
	default:
		return ""
	}
}

func (c *PortMode) UnmarshalYAML(b []byte) error {
	var modeString string
	if err := json.Unmarshal(b, &modeString); err != nil {
		return err
	}

	modeString = strings.Trim(strings.ToLower(modeString), " ")
	for candidate := PortMode(0); candidate < EndOfPortMode; candidate++ {
		if candidate.String() == modeString {
			*c = candidate
			return nil
		}
	}

	return fmt.Errorf("unknown port mode: '%s'", modeString)
}

func (c PortMode) MarshalYAML() ([]byte, error) {
	return json.Marshal(c.String())
}
