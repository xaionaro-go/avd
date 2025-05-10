package types

type ListenConfigRTSP struct {
	TransportProtocol TransportProtocol
	PacketSize        uint16
}

type ListenOptionTransportProtocol TransportProtocol

func (opt ListenOptionTransportProtocol) apply(cfg *ListenConfig) {
	cfg.RTSP.TransportProtocol = TransportProtocol(opt)
}

type ListenOptionPacketSize uint16

func (opt ListenOptionPacketSize) apply(cfg *ListenConfig) {
	cfg.RTSP.PacketSize = uint16(opt)
}
