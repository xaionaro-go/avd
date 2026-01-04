// streaming_port_mode.go defines the StreamingPortMode type, distinguishing between publishers and consumers.

package types

import (
	"encoding/json"
	"fmt"
	"strings"
)

type StreamingPortMode int

const (
	UndefinedPortMode = StreamingPortMode(iota)
	PortModeConsumers
	PortModePublishers
	EndOfPortMode
)

func (c StreamingPortMode) String() string {
	switch c {
	case PortModeConsumers:
		return "consumers"
	case PortModePublishers:
		return "publishers"
	default:
		return ""
	}
}

func (c *StreamingPortMode) UnmarshalYAML(b []byte) error {
	var modeString string
	if err := json.Unmarshal(b, &modeString); err != nil {
		return err
	}

	modeString = strings.Trim(strings.ToLower(modeString), " ")
	for candidate := StreamingPortMode(0); candidate < EndOfPortMode; candidate++ {
		if candidate.String() == modeString {
			*c = candidate
			return nil
		}
	}

	return fmt.Errorf("unknown port mode: '%s'", modeString)
}

func (c StreamingPortMode) MarshalYAML() ([]byte, error) {
	return json.Marshal(c.String())
}
