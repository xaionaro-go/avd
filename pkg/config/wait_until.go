package config

type WaitUntilConfig struct {
	VideoTrackCount uint `yaml:"video_track_count,omitempty"`
	AudioTrackCount uint `yaml:"audio_track_count,omitempty"`
}
