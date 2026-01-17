//go:build linux

package avd

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
)

func isLocalUDPPortBound(port uint16) (bool, error) {
	portHex := []byte(fmt.Sprintf(":%04X", port))
	for _, path := range []string{"/proc/net/udp", "/proc/net/udp6"} {
		f, err := os.Open(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return false, err
		}

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			fields := bytes.Fields(scanner.Bytes())
			if len(fields) < 2 {
				continue
			}
			if bytes.HasSuffix(fields[1], portHex) {
				f.Close()
				return true, nil
			}
		}
		f.Close()
	}
	return false, nil
}
