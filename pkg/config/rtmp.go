package config

import (
	"github.com/xaionaro-go/avd/pkg/avd/types"
)

type RTMPMode = types.RTMPMode

type RTMPConfig struct {
	Mode           RTMPMode `yaml:"mode"`
	DefaultAppName string   `yaml:"default_app_name"`
}
