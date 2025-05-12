package types

import (
	"context"
	"fmt"
	"strings"
)

type PortAddress string

func (p PortAddress) Parse(
	ctx context.Context,
) (string, string, error) {
	words := strings.SplitN(string(p), ":", 2)
	switch len(words) {
	case 1:
		if strings.HasPrefix(words[0], "/") || strings.HasSuffix(words[0], "sock") {
			return "unix", words[0], nil
		}
		return "", "", fmt.Errorf("protocol is not set in '%s'; please use format: protocol:address; examples: unix:/tmp/mysock.sock, tcp:0.0.0.0:1935", p)
	case 2:
		return words[0], words[1], nil
	default:
		return "", "", fmt.Errorf("internal error: too many words: %d", len(words))
	}
}
