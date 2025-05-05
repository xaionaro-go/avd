package avd

import (
	"context"
	"fmt"
	"strings"

	"github.com/xaionaro-go/avpipeline"
)

type Publisher interface {
	fmt.Stringer
	GetInputNode(ctx context.Context) avpipeline.AbstractNode
	GetOutputRoute(ctx context.Context) *Route
}

type Publishers []Publisher

func (s Publishers) String() string {
	switch len(s) {
	case 0:
		return "NONE"
	case 1:
		return s[0].String()
	}

	var result []string
	for _, publisher := range s {
		result = append(result, publisher.String())
	}

	return "[" + strings.Join(result, ",") + "]"
}
