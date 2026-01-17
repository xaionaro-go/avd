//go:build windows

package avd

import (
	"bufio"
	"bytes"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

func isLocalUDPPortBound(port uint16) (bool, error) {
	out, err := exec.Command("netstat", "-an", "-p", "UDP").Output()
	if err != nil {
		return false, fmt.Errorf("failed to run netstat: %w", err)
	}

	// Windows netstat output for UDP:
	//   UDP    0.0.0.0:12345          *:*
	//   UDP    [::]:12345             *:*
	// Local address is field 2.
	portSuffix := ":" + strconv.Itoa(int(port))
	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		if fields[0] != "UDP" {
			continue
		}
		if strings.HasSuffix(fields[1], portSuffix) {
			return true, nil
		}
	}
	return false, nil
}
