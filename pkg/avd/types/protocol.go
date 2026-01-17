// Package types contains common types used in the avd package.
package types

import (
	"encoding/json"
	"fmt"
	"strings"
)

type StreamingProtocol int

const (
	UndefinedProtocol = StreamingProtocol(iota)
	ProtocolRTMP
	ProtocolRTSP
	ProtocolSRT
	ProtocolMPEGTS
	EndOfProtocol
)

func (c StreamingProtocol) IsValid() bool {
	switch c {
	case ProtocolRTMP:
		return true
	case ProtocolRTSP:
		return true
	case ProtocolSRT:
		return true
	case ProtocolMPEGTS:
		return true
	}
	return false
}

func (c StreamingProtocol) String() string {
	switch c {
	case ProtocolRTMP:
		return "rtmp"
	case ProtocolRTSP:
		return "rtsp"
	case ProtocolSRT:
		return "srt"
	case ProtocolMPEGTS:
		return "mpegts"
	default:
		return ""
	}
}

func (c StreamingProtocol) FormatName() string {
	switch c {
	case ProtocolRTMP:
		return "flv"
	case ProtocolRTSP:
		return "rtsp"
	case ProtocolSRT:
		return "mpegts"
	case ProtocolMPEGTS:
		return "mpegts"
	default:
		return ""
	}
}

func (c *StreamingProtocol) UnmarshalYAML(b []byte) error {
	var modeString string
	if err := json.Unmarshal(b, &modeString); err != nil {
		return err
	}

	modeString = strings.Trim(strings.ToLower(modeString), " ")
	for candidate := StreamingProtocol(0); candidate < EndOfProtocol; candidate++ {
		if candidate.String() == modeString {
			*c = candidate
			return nil
		}
	}

	return fmt.Errorf("unknown protocol: '%s'", modeString)
}

func (c StreamingProtocol) MarshalYAML() ([]byte, error) {
	return json.Marshal(c.String())
}

func SupportedProtocols() []StreamingProtocol {
	return []StreamingProtocol{ProtocolRTMP, ProtocolRTSP, ProtocolSRT}
}
