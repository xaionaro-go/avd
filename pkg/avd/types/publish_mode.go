package types

import (
	"fmt"

	"github.com/goccy/go-yaml"
	"github.com/xaionaro-go/avpipeline/router"
)

type PublishMode router.PublishMode

func (mode *PublishMode) UnmarshalYAML(b []byte) error {
	var s string
	err := yaml.Unmarshal(b, &s)
	if err != nil {
		return err
	}
	for publishMode := router.PublishMode(0); publishMode < router.EndOfPublishMode; publishMode++ {
		if publishMode.String() == s {
			*mode = PublishMode(publishMode)
			return nil
		}
	}
	return fmt.Errorf("unknown publish mode '%s'", s)
}

func (mode PublishMode) MarshalYAML() ([]byte, error) {
	return yaml.Marshal(router.PublishMode(mode).String())
}
