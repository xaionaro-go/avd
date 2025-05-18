package config

import (
	"fmt"
	"strings"

	"slices"

	"github.com/xaionaro-go/avd/pkg/avd/types"
	transcodertypes "github.com/xaionaro-go/avpipeline/preset/transcoderwithpassthrough/types"
)

type Destination struct {
	URL   string          `yaml:"url"`
	Route types.RoutePath `yaml:"route"`
}

type ForwardConfig struct {
	Destination Destination                    `yaml:"destination"`
	Recoding    *transcodertypes.RecoderConfig `yaml:"recoding"`
}

type EndpointConfig struct {
	Forwardings []ForwardConfig `yaml:"forwardings"`
}

type ProtocolHandlerConfig struct {
	RTMP   *RTMPConfig   `yaml:"rtmp,omitempty"`
	RTSP   *RTSPConfig   `yaml:"rtsp,omitempty"`
	MPEGTS *MPEGTSConfig `yaml:"mpegts,omitempty"`
}

type Protocol = types.Protocol

func (cfg ProtocolHandlerConfig) Protocol() (Protocol, error) {
	m := map[Protocol]bool{
		types.ProtocolRTMP:   cfg.RTMP != nil,
		types.ProtocolRTSP:   cfg.RTSP != nil,
		types.ProtocolMPEGTS: cfg.MPEGTS != nil,
	}

	var enabledProtocols []Protocol
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

type PortMode = types.PortMode
type DictionaryItem = types.DictionaryItem
type DictionaryItems = types.DictionaryItems
type OnEndAction = types.OnEndAction
type PortConfig struct {
	Address          PortAddress           `yaml:"address"`
	Mode             PortMode              `yaml:"mode"`
	ProtocolHandler  ProtocolHandlerConfig `yaml:"protocol_handler"`
	CustomOptions    DictionaryItems       `yaml:"custom_options"`
	DefaultRoutePath string                `yaml:"default_route_path"`
	OnEnd            OnEndAction           `yaml:"on_end"`
	WaitUntil        WaitUntilConfig       `yaml:"wait_until"`
}

func (cfg PortConfig) ListenOptions() []types.ListenOption {
	opts := types.ListenOptions{
		types.ListenOptionOnEndAction(cfg.OnEnd),
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
	return opts
}

type Config struct {
	Ports     []PortConfig                       `yaml:"ports"`
	Endpoints map[types.RoutePath]EndpointConfig `yaml:"endpoints"`
}
