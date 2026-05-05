// port_streaming.go defines the configuration for streaming ports and endpoints.

package config

import (
	"fmt"
	"slices"
	"strings"

	"github.com/goccy/go-yaml"
	"github.com/xaionaro-go/avd/pkg/avd/types"
	transcodertypes "github.com/xaionaro-go/avpipeline/preset/transcoderwithpassthrough/types"
)

type DestinationLocal struct {
	Route       types.RoutePath
	PublishMode PublishMode `yaml:"publish_mode"`
}

type Destination struct {
	URL   *string           `yaml:"url,omitempty"`
	Local *DestinationLocal `yaml:"local,omitempty"`
}

type PrivacyBlurConfig struct {
	Enabled           bool    `yaml:"enabled"`
	Faces             bool    `yaml:"faces"`
	Plates            bool    `yaml:"plates"`
	BlurRadius        float64 `yaml:"blur_radius"`
	PixelateBlockSize int     `yaml:"pixelate_block_size"`
}

type DeblemishConfig struct {
	Enabled  bool    `yaml:"enabled"`
	SigmaS   float64 `yaml:"sigma_s"`
	SigmaR   float64 `yaml:"sigma_r"`
	Diameter int     `yaml:"diameter"`
	FaceOnly bool    `yaml:"face_only"`
}

type WhisperConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Model    string `yaml:"model"`
	Language string `yaml:"language"`
}

// DefaultIdleTimeoutSec is the fallback idle timeout applied to an
// on-demand forwarding when IdleTimeoutSec is zero. It gives the
// transcoder a 30-second grace period to handle brief gaps between
// consumers before tearing the pipeline down.
const DefaultIdleTimeoutSec = 30

type ForwardConfig struct {
	Destination    Destination                       `yaml:"destination"`
	Transcoding    *transcodertypes.TranscoderConfig `yaml:"transcoding"`
	WaitUntil      WaitUntilConfig                   `yaml:"wait_until"`
	PrivacyBlur    *PrivacyBlurConfig                `yaml:"privacy_blur,omitempty"`
	Deblemish      *DeblemishConfig                  `yaml:"deblemish,omitempty"`
	Whisper        *WhisperConfig                    `yaml:"whisper,omitempty"`
	OnDemand       bool                              `yaml:"on_demand,omitempty"`
	IdleTimeoutSec uint                              `yaml:"idle_timeout_sec,omitempty"`
}

// EffectiveIdleTimeoutSec returns IdleTimeoutSec when set, or
// DefaultIdleTimeoutSec when the caller left it zero. Only meaningful
// when OnDemand is true; eager forwardings ignore this value entirely.
func (c ForwardConfig) EffectiveIdleTimeoutSec() uint {
	if c.IdleTimeoutSec == 0 {
		return DefaultIdleTimeoutSec
	}
	return c.IdleTimeoutSec
}

type Command struct {
	Command []string      `yaml:"command"`
	Restart RestartPolicy `yaml:"restart"`
}

func (cmd *Command) UnmarshalYAML(b []byte) error {
	type rawCommand Command
	raw := rawCommand{
		Restart: RestartPolicyNever,
	}
	if err := yaml.Unmarshal(b, &raw); err != nil {
		return err
	}
	*cmd = Command(raw)
	return nil
}

type EndpointConfig struct {
	Forwardings        []ForwardConfig `yaml:"forwardings"`
	OnPublisherAdded   *Command        `yaml:"on_publisher_added,omitempty"`
	OnPublisherRemoved *Command        `yaml:"on_publisher_removed,omitempty"`
	OnConsumerAdded    *Command        `yaml:"on_consumer_added,omitempty"`
	OnConsumerRemoved  *Command        `yaml:"on_consumer_removed,omitempty"`
}

type StreamingProtocolHandlerConfig struct {
	RTMP   *RTMPConfig   `yaml:"rtmp,omitempty"`
	RTSP   *RTSPConfig   `yaml:"rtsp,omitempty"`
	SRT    *SRTConfig    `yaml:"srt,omitempty"`
	MPEGTS *MPEGTSConfig `yaml:"mpegts,omitempty"`
}

type StreamingProtocol = types.StreamingProtocol

func (cfg StreamingProtocolHandlerConfig) Protocol() (StreamingProtocol, error) {
	m := map[StreamingProtocol]bool{
		types.ProtocolRTMP:   cfg.RTMP != nil,
		types.ProtocolRTSP:   cfg.RTSP != nil,
		types.ProtocolSRT:    cfg.SRT != nil,
		types.ProtocolMPEGTS: cfg.MPEGTS != nil,
	}

	var enabledProtocols []StreamingProtocol
	for protocol, isEnabled := range m {
		if isEnabled {
			enabledProtocols = append(enabledProtocols, protocol)
		}
	}
	slices.Sort(enabledProtocols)

	switch len(enabledProtocols) {
	case 0:
		return 0, fmt.Errorf("no protocols enabled")
	case 1:
		return enabledProtocols[0], nil
	default:
		var s []string
		for _, p := range enabledProtocols {
			s = append(s, p.String())
		}
		return 0, fmt.Errorf("more than one protocol enabled: %s", strings.Join(s, ","))
	}
}

type (
	StreamingPortMode   = types.StreamingPortMode
	PublishMode         = types.PublishMode
	DictionaryItem      = types.DictionaryItem
	DictionaryItems     = types.DictionaryItems
	OnEndAction         = types.OnEndAction
	StreamingPortConfig struct {
		Address            PortAddress                    `yaml:"address"`
		Mode               StreamingPortMode              `yaml:"mode"`
		PublishMode        PublishMode                    `yaml:"publish_mode"`
		ProtocolHandler    StreamingProtocolHandlerConfig `yaml:"protocol_handler"`
		CustomOptions      DictionaryItems                `yaml:"custom_options,omitempty"`
		DefaultRoutePath   string                         `yaml:"default_route_path"`
		OnEnd              OnEndAction                    `yaml:"on_end"`
		WaitUntil          WaitUntilConfig                `yaml:"wait_until,omitempty"`
		IgnoreZeroDuration *bool                          `yaml:"ignore_zero_duration,omitempty"`
	}
)

func (cfg StreamingPortConfig) ListenOptions() []types.ListenOption {
	opts := types.ListenOptions{
		types.ListenOptionOnEndAction(cfg.OnEnd),
		types.ListenOptionPublishMode(cfg.PublishMode),
		types.ListenOptionCustomOptions(cfg.CustomOptions),
	}
	if cfg.DefaultRoutePath != "" {
		opts = append(opts, types.ListenOptionDefaultRoutePath(cfg.DefaultRoutePath))
	}
	if cfg.ProtocolHandler.RTSP != nil &&
		cfg.ProtocolHandler.RTSP.TransportProtocol != types.UndefinedTransportProtocol {
		opts = append(opts, types.ListenOptionTransportProtocol(cfg.ProtocolHandler.RTSP.TransportProtocol))
	}
	if cfg.WaitUntil.VideoTrackCount > 0 {
		opts = append(opts, types.ListenOptionWaitUntilVideoTracksCount(cfg.WaitUntil.VideoTrackCount))
	}
	if cfg.WaitUntil.AudioTrackCount > 0 {
		opts = append(opts, types.ListenOptionWaitUntilAudioTracksCount(cfg.WaitUntil.AudioTrackCount))
	}
	if cfg.WaitUntil.SubtitleTrackCount > 0 {
		opts = append(opts, types.ListenOptionWaitUntilSubtitleTracksCount(cfg.WaitUntil.SubtitleTrackCount))
	}
	if cfg.WaitUntil.DataTrackCount > 0 {
		opts = append(opts, types.ListenOptionWaitUntilDataTracksCount(cfg.WaitUntil.DataTrackCount))
	}
	if cfg.IgnoreZeroDuration != nil {
		opts = append(opts, types.ListenOptionIgnoreZeroDuration(*cfg.IgnoreZeroDuration))
	}
	return opts
}
