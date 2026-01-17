//go:build darwin

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
	// Try lsof first as it's more precise
	portStr := strconv.Itoa(int(port))
	out, err := exec.Command("lsof", "-nP", "-iUDP:"+portStr).Output()
	if err == nil && len(out) > 0 {
		return true, nil
	}

	// Fallback to netstat
	out, err = exec.Command("netstat", "-an", "-p", "udp").Output()
	if err != nil {
		return false, fmt.Errorf("failed to run netstat: %w", err)
	}

	// Darwin netstat output for UDP:
	// udp4       0      0  *.12345                *.*
	// udp6       0      0  *.12345                *.*
	// Local address is typically field 4.
	portSuffix := "." + portStr
	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		if !strings.HasPrefix(fields[0], "udp") {
			continue
		}
		if strings.HasSuffix(fields[3], portSuffix) {
			return true, nil
		}
	}
	return false, nil
}
