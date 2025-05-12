package types

import (
	"encoding/json"
	"fmt"
	"strings"
)

type Protocol int

const (
	UndefinedProtocol = Protocol(iota)
	ProtocolRTMP
	ProtocolRTSP
	ProtocolSRT
	ProtocolMPEGTS
	EndOfProtocol
)

func (c Protocol) IsValid() bool {
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

func (c Protocol) String() string {
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

func (c Protocol) FormatName() string {
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

func (c *Protocol) UnmarshalYAML(b []byte) error {
	var modeString string
	if err := json.Unmarshal(b, &modeString); err != nil {
		return err
	}

	modeString = strings.Trim(strings.ToLower(modeString), " ")
	for candidate := Protocol(0); candidate < EndOfProtocol; candidate++ {
		if candidate.String() == modeString {
			*c = candidate
			return nil
		}
	}

	return fmt.Errorf("unknown protocol: '%s'", modeString)
}

func (c Protocol) MarshalYAML() ([]byte, error) {
	return json.Marshal(c.String())
}

func SupportedProtocols() []Protocol {
	return []Protocol{ProtocolRTMP, ProtocolRTSP}
}
