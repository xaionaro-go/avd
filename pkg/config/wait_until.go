// wait_until.go defines the configuration for waiting for a certain number of tracks before starting.

package config

type WaitUntilConfig struct {
	VideoTrackCount    uint `yaml:"video_track_count,omitempty"`
	AudioTrackCount    uint `yaml:"audio_track_count,omitempty"`
	SubtitleTrackCount uint `yaml:"subtitle_track_count,omitempty"`
	DataTrackCount     uint `yaml:"data_track_count,omitempty"`
}
