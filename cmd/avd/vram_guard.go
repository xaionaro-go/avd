package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/facebookincubator/go-belt/tool/logger"
	"github.com/xaionaro-go/observability"
)

const (
	defaultVRAMGuardPollInterval = 2 * time.Second
	vramGuardExitCode            = 137
)

func startVRAMGuard(
	ctx context.Context,
	limitMiB uint64,
	pollInterval time.Duration,
	exit func(),
) {
	if limitMiB == 0 {
		return
	}

	pid := os.Getpid()
	logger.Warnf(ctx, "starting hacky VRAM guard: pid=%d limit_mib=%d", pid, limitMiB)
	observability.Go(ctx, func(ctx context.Context) {
		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}

			usedMiB, found, err := pollNvidiaSMIUsedMemoryMiB(ctx, pid)
			if err != nil {
				logger.Warnf(ctx, "unable to poll NVIDIA VRAM usage for pid %d: %v", pid, err)
				continue
			}
			if !found {
				continue
			}
			if usedMiB <= limitMiB {
				continue
			}

			logger.Errorf(
				ctx,
				"VRAM guard limit exceeded for pid %d: used_mib=%d limit_mib=%d; terminating avd",
				pid,
				usedMiB,
				limitMiB,
			)
			exit()
			return
		}
	})
}

func pollNvidiaSMIUsedMemoryMiB(
	ctx context.Context,
	pid int,
) (uint64, bool, error) {
	cmd := exec.CommandContext(
		ctx,
		"nvidia-smi",
		"--query-compute-apps=pid,used_gpu_memory",
		"--format=csv,noheader,nounits",
	)
	output, err := cmd.Output()
	if err != nil {
		return 0, false, fmt.Errorf("nvidia-smi query failed: %w", err)
	}

	return parseNvidiaSMIUsedMemoryMiB(output, pid)
}

func parseNvidiaSMIUsedMemoryMiB(
	output []byte,
	pid int,
) (uint64, bool, error) {
	var usedMiB uint64
	var found bool
	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.Contains(line, "No running processes found") {
			continue
		}

		parts := strings.Split(line, ",")
		if len(parts) != 2 {
			return 0, false, fmt.Errorf("unexpected nvidia-smi row %q", line)
		}

		rowPID, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			return 0, false, fmt.Errorf("unable to parse nvidia-smi pid from %q: %w", line, err)
		}
		if rowPID != pid {
			continue
		}

		memoryText := strings.TrimSpace(parts[1])
		memoryText = strings.TrimSuffix(memoryText, "MiB")
		memoryText = strings.TrimSpace(memoryText)
		memoryMiB, err := strconv.ParseUint(memoryText, 10, 64)
		if err != nil {
			return 0, false, fmt.Errorf("unable to parse nvidia-smi memory from %q: %w", line, err)
		}

		usedMiB += memoryMiB
		found = true
	}
	if err := scanner.Err(); err != nil {
		return 0, false, fmt.Errorf("unable to scan nvidia-smi output: %w", err)
	}

	return usedMiB, found, nil
}
