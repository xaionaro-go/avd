package avd

import (
	"os"
	"strings"
	"testing"
)

func TestChunkRateConnectionProxyLogsAreNotDebug(t *testing.T) {
	t.Parallel()

	srcBytes, err := os.ReadFile("connection_proxied.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(srcBytes)

	for _, message := range []string{
		"forwarding %d bytes from internal AV handler to client",
		"received %d bytes from client",
		"forwarded %d client bytes to internal AV handler",
		"forwarding %d client bytes to internal AV handler",
	} {
		forbidden := `logger.Debugf(ctx, "` + message
		required := `logger.Tracef(ctx, "` + message
		if strings.Contains(src, forbidden) {
			t.Fatalf("chunk-rate log %q must not be Debugf", message)
		}
		if !strings.Contains(src, required) {
			t.Fatalf("chunk-rate log %q must remain logged at Tracef", message)
		}
	}
}
