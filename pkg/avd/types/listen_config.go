package types

import (
	"fmt"
	"time"

	routertypes "github.com/xaionaro-go/avpipeline/router/types"
)

type RoutePath = routertypes.RoutePath

type ListenConfig struct {
	DefaultRoutePath          RoutePath
	OnEndAction               OnEndAction
	WaitUntilVideoTracksCount uint
	WaitUntilAudioTracksCount uint
	CustomOptions             DictionaryItems

	// not all protocols respects these, but some do:
	BufferDuration   time.Duration
	MaxBufferSize    uint32
	ReorderQueueSize uint32
	Timeout          time.Duration

	// protocol-specific stuff
	RTSP ListenConfigRTSP
}

func (cfg ListenConfig) GetBufferDuration() time.Duration {
	if cfg.BufferDuration == 0 {
		return time.Second
	}
	return cfg.BufferDuration
}

func (cfg ListenConfig) DictionaryItems(
	protocol Protocol,
	mode PortMode,
) DictionaryItems {
	customOpts := DictionaryItems{
		{Key: "listen", Value: "1"},
	}
	if cfg.MaxBufferSize != 0 {
		customOpts = append(customOpts, DictionaryItem{
			Key: "buffer_size", Value: fmt.Sprintf("%d", cfg.MaxBufferSize),
		})
	}
	if cfg.ReorderQueueSize != 0 {
		customOpts = append(customOpts, DictionaryItem{
			Key: "reorder_queue_size", Value: fmt.Sprintf("%d", cfg.ReorderQueueSize),
		})
	}
	if cfg.Timeout != 0 {
		customOpts = append(customOpts, DictionaryItem{
			Key: "timeout", Value: fmt.Sprintf("%d", cfg.Timeout.Microseconds()),
		})
	}
	switch protocol {
	case ProtocolRTMP:
		customOpts = append(customOpts, DictionaryItems{
			{Key: "rtmp_live", Value: "live"},
			{Key: "rtmp_buffer", Value: fmt.Sprintf("%d", cfg.GetBufferDuration().Milliseconds())},
		}...)
		if cfg.DefaultRoutePath != "" {
			customOpts = append(customOpts, DictionaryItem{
				Key: "rtmp_app", Value: string(cfg.DefaultRoutePath),
			})
		}
	case ProtocolRTSP:
		customOpts = append(customOpts, DictionaryItems{
			{Key: "rtsp_flags", Value: "listen"},
		}...)
		if cfg.RTSP.PacketSize != 0 {
			customOpts = append(customOpts, DictionaryItem{
				Key: "pkt_size", Value: fmt.Sprintf("%d", cfg.RTSP.PacketSize),
			})
		}
		if cfg.RTSP.TransportProtocol == TransportProtocolUDP {
			panic(fmt.Errorf("we do not support UDP transport protocol for RTSP, yet"))
		} else {
			customOpts = append(customOpts, DictionaryItem{
				Key: "rtsp_transport", Value: TransportProtocolTCP.String(),
			})
		}
	case ProtocolSRT:
		customOpts = append(customOpts, DictionaryItems{
			{Key: "smoother", Value: "live"},
			{Key: "transtype", Value: "live"},
		}...)
	}

	switch mode {
	case PortModeConsumers:
		customOpts = append(customOpts, DictionaryItem{Key: "f", Value: protocol.FormatName()})
	}

	customOpts = append(customOpts, cfg.CustomOptions...)

	return customOpts
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

type ListenOptionOnEndAction OnEndAction

func (opt ListenOptionOnEndAction) apply(cfg *ListenConfig) {
	cfg.OnEndAction = OnEndAction(opt)
}

type ListenOptionWaitUntilVideoTracksCount uint

func (opt ListenOptionWaitUntilVideoTracksCount) apply(cfg *ListenConfig) {
	cfg.WaitUntilVideoTracksCount = uint(opt)
}

type ListenOptionWaitUntilAudioTracksCount uint

func (opt ListenOptionWaitUntilAudioTracksCount) apply(cfg *ListenConfig) {
	cfg.WaitUntilAudioTracksCount = uint(opt)
}
