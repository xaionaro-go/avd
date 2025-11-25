package config

import (
	"fmt"
	"strings"

	"slices"

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

type ForwardConfig struct {
	Destination Destination                    `yaml:"destination"`
	Recoding    *transcodertypes.RecoderConfig `yaml:"recoding"`
}

type Command struct {
	Command []string      `yaml:"command"`
	Restart RestartPolicy `yaml:"restart"`
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
	MPEGTS *MPEGTSConfig `yaml:"mpegts,omitempty"`
}

type StreamingProtocol = types.StreamingProtocol

func (cfg StreamingProtocolHandlerConfig) Protocol() (StreamingProtocol, error) {
	m := map[StreamingProtocol]bool{
		types.ProtocolRTMP:   cfg.RTMP != nil,
		types.ProtocolRTSP:   cfg.RTSP != nil,
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

type StreamingPortMode = types.StreamingPortMode
type PublishMode = types.PublishMode
type DictionaryItem = types.DictionaryItem
type DictionaryItems = types.DictionaryItems
type OnEndAction = types.OnEndAction
type StreamingPortConfig struct {
	Address             PortAddress                    `yaml:"address"`
	Mode                StreamingPortMode              `yaml:"mode"`
	PublishMode         PublishMode                    `yaml:"publish_mode"`
	ProtocolHandler     StreamingProtocolHandlerConfig `yaml:"protocol_handler"`
	CustomOptions       DictionaryItems                `yaml:"custom_options,omitempty"`
	DefaultRoutePath    string                         `yaml:"default_route_path"`
	OnEnd               OnEndAction                    `yaml:"on_end"`
	WaitUntil           WaitUntilConfig                `yaml:"wait_until,omitempty"`
	CorrectZeroDuration *bool                          `yaml:"correct_zero_duration,omitempty"`
}

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
	if cfg.CorrectZeroDuration != nil {
		opts = append(opts, types.ListenOptionCorrectZeroDuration(*cfg.CorrectZeroDuration))
	}
	return opts
}
