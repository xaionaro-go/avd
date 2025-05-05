package config

import (
	"encoding/json"
	"fmt"
	"strings"
)

type RTMPMode int

const (
	RTMPModeConsumers = RTMPMode(iota)
	RTMPModePublishers
	EndOfRTMPMode
)

type RTMPConfig struct {
	Mode           RTMPMode `yaml:"mode"`
	DefaultAppName string   `yaml:"default_app_name"`
}

func (c RTMPMode) String() string {
	switch c {
	case RTMPModeConsumers:
		return "consumers"
	case RTMPModePublishers:
		return "publishers"
	default:
		return ""
	}
}

func (c *RTMPMode) UnmarshalYAML(b []byte) error {
	var modeString string
	if err := json.Unmarshal(b, &modeString); err != nil {
		return err
	}

	modeString = strings.Trim(strings.ToLower(modeString), " ")
	for candidate := RTMPMode(0); candidate < EndOfRTMPMode; candidate++ {
		if candidate.String() == modeString {
			*c = candidate
			return nil
		}
	}

	return fmt.Errorf("unknown RTMP port mode: '%s'", modeString)
}

func (c RTMPMode) MarshalYAML() ([]byte, error) {
	return json.Marshal(c.String())
}
