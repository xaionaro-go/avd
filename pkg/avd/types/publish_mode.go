// publish_mode.go provides YAML marshaling/unmarshaling for the PublishMode type.

package types

import (
	"context"
	"fmt"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/goccy/go-yaml"
	"github.com/xaionaro-go/avpipeline/router"
)

type PublishMode router.PublishMode

func (mode PublishMode) String() string {
	return router.PublishMode(mode).String()
}

func (mode *PublishMode) UnmarshalYAML(b []byte) error {
	var s string
	err := yaml.Unmarshal(b, &s)
	if err != nil {
		return err
	}
	ctx := context.TODO()
	logger.Tracef(ctx, "PublishMode string: '%s'", s)
	for publishMode := range router.EndOfPublishMode {
		if publishMode.String() == s {
			logger.Tracef(ctx, "PublishMode result: '%s'", publishMode)
			*mode = PublishMode(publishMode)
			return nil
		}
	}
	return fmt.Errorf("unknown publish mode '%s'", s)
}

func (mode PublishMode) MarshalYAML() ([]byte, error) {
	return yaml.Marshal(router.PublishMode(mode).String())
}
