// restart_policy.go defines the restart policy for commands executed by AVD.

package config

import (
	"context"
	"fmt"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/goccy/go-yaml"
)

type RestartPolicy int

const (
	RestartPolicyNever = RestartPolicy(iota)
	RestartPolicyAlways
	EndOfRestartPolicy
)

func (p RestartPolicy) String() string {
	switch p {
	case RestartPolicyAlways:
		return "always"
	case RestartPolicyNever:
		return "never"
	default:
		return fmt.Sprintf("unexpected_restart_policy_%d", int(p))
	}
}

func (p *RestartPolicy) UnmarshalYAML(b []byte) error {
	var s string
	err := yaml.Unmarshal(b, &s)
	if err != nil {
		return err
	}
	ctx := context.TODO()
	logger.Tracef(ctx, "RestartPolicy string: '%s'", s)
	for candidate := RestartPolicy(0); candidate < EndOfRestartPolicy; candidate++ {
		if candidate.String() == s {
			logger.Tracef(ctx, "RestartPolicy result: '%s'", candidate)
			*p = RestartPolicy(candidate)
			return nil
		}
	}
	return fmt.Errorf("unknown restart policy '%s'", s)
}

func (p RestartPolicy) MarshalYAML() ([]byte, error) {
	return yaml.Marshal(p.String())
}
