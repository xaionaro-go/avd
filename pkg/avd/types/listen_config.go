// listen_config.go defines the configuration for listening ports.

package types

import (
	"fmt"
	"time"

	"github.com/xaionaro-go/avpipeline/router"
	routertypes "github.com/xaionaro-go/avpipeline/router/types"
)

type RoutePath = routertypes.RoutePath

type ListenConfig struct {
	DefaultRoutePath             RoutePath          `yaml:"default_route_path"`
	OnEndAction                  OnEndAction        `yaml:"on_end"`
	PublishMode                  router.PublishMode `yaml:"publish_mode"`
	WaitUntilVideoTracksCount    uint               `yaml:"wait_until_video_tracks_count"`
	WaitUntilAudioTracksCount    uint               `yaml:"wait_until_audio_tracks_count"`
	WaitUntilSubtitleTracksCount uint               `yaml:"wait_until_subtitle_tracks_count"`
	WaitUntilDataTracksCount     uint               `yaml:"wait_until_data_tracks_count"`
	IgnoreZeroDuration           *bool              `yaml:"ignore_zero_duration"`
	CustomOptions                DictionaryItems    `yaml:"custom_options"`

	// not all protocols respects these, but some do:
	BufferDuration   time.Duration `yaml:"buffer_duration"`
	MaxBufferSize    uint32        `yaml:"max_buffer_size"`
	ReorderQueueSize uint32        `yaml:"reorder_queue_size"`
	Timeout          time.Duration `yaml:"timeout"`

	// protocol-specific stuff
	RTSP ListenConfigRTSP `yaml:"rtsp"`
}

func (cfg ListenConfig) GetPublishMode() router.PublishMode {
	if cfg.PublishMode == router.UndefinedPublishMode {
		return router.PublishModeSharedTakeover
	}
	return cfg.PublishMode
}

func (cfg ListenConfig) GetBufferDuration() time.Duration {
	if cfg.BufferDuration == 0 {
		return time.Second
	}
	return cfg.BufferDuration
}

func (cfg ListenConfig) GetIgnoreZeroDuration() bool {
	if cfg.IgnoreZeroDuration == nil {
		return false
	}
	return *cfg.IgnoreZeroDuration
}

func (cfg ListenConfig) DictionaryItems(
	protocol StreamingProtocol,
	mode StreamingPortMode,
) DictionaryItems {
	customOpts := DictionaryItems{}
	if protocol != ProtocolRTSP || mode == PortModePublishers {
		customOpts = append(customOpts, DictionaryItem{Key: "listen", Value: "1"})
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
		customOpts = append(customOpts, DictionaryItem{Key: "rtsp_transport", Value: "tcp"})
		if mode == PortModePublishers {
			customOpts = append(customOpts, DictionaryItem{Key: "rtsp_flags", Value: "listen"})
		}
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

type ListenOptionPublishMode router.PublishMode

func (opt ListenOptionPublishMode) apply(cfg *ListenConfig) {
	cfg.PublishMode = router.PublishMode(opt)
}

type ListenOptionWaitUntilVideoTracksCount uint

func (opt ListenOptionWaitUntilVideoTracksCount) apply(cfg *ListenConfig) {
	cfg.WaitUntilVideoTracksCount = uint(opt)
}

type ListenOptionWaitUntilAudioTracksCount uint

func (opt ListenOptionWaitUntilAudioTracksCount) apply(cfg *ListenConfig) {
	cfg.WaitUntilAudioTracksCount = uint(opt)
}

type ListenOptionWaitUntilSubtitleTracksCount uint

func (opt ListenOptionWaitUntilSubtitleTracksCount) apply(cfg *ListenConfig) {
	cfg.WaitUntilSubtitleTracksCount = uint(opt)
}

type ListenOptionWaitUntilDataTracksCount uint

func (opt ListenOptionWaitUntilDataTracksCount) apply(cfg *ListenConfig) {
	cfg.WaitUntilDataTracksCount = uint(opt)
}

type ListenOptionCustomOptions DictionaryItems

func (opt ListenOptionCustomOptions) apply(cfg *ListenConfig) {
	cfg.CustomOptions = DictionaryItems(opt)
}

type ListenOptionIgnoreZeroDuration bool

func (opt ListenOptionIgnoreZeroDuration) apply(cfg *ListenConfig) {
	cfg.IgnoreZeroDuration = ptr(bool(opt))
}
