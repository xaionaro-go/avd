package types

import (
	"time"
)

type ListenConfig struct {
	DefaultRoutePath RoutePath

	// not all protocols respects these, but some do:
	BufferDuration   time.Duration
	MaxBufferSize    uint32
	ReorderQueueSize uint32
	Timeout          time.Duration

	RTSP ListenConfigRTSP
}

func (cfg ListenConfig) GetBufferDuration() time.Duration {
	if cfg.BufferDuration == 0 {
		return time.Second
	}
	return cfg.BufferDuration
}

type ListenOption interface {
	apply(*ListenConfig)
}

type ListenOptions []ListenOption

func (opts ListenOptions) Config() ListenConfig {
	var cfg ListenConfig
	opts.apply(&cfg)
	return cfg
}

func (opts ListenOptions) apply(cfg *ListenConfig) {
	for _, opt := range opts {
		opt.apply(cfg)
	}
}

type ListenOptionDefaultRoutePath string

func (opt ListenOptionDefaultRoutePath) apply(cfg *ListenConfig) {
	cfg.DefaultRoutePath = RoutePath(opt)
}

type ListenOptionBufferDuration time.Duration

func (opt ListenOptionBufferDuration) apply(cfg *ListenConfig) {
	cfg.BufferDuration = time.Duration(opt)
}
