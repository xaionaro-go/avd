package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	codectypes "github.com/xaionaro-go/avpipeline/codec/types"
)

func TestConfigReadPreservesTranscodingCodecNames(t *testing.T) {
	var cfg Config

	_, err := cfg.Read([]byte(`
endpoints:
  /camera:
    forwardings:
      - transcoding:
          input:
            audio_track_configs:
              - codec_name: aac
                codec_names: [libfdk_aac, aac]
            video_track_configs:
              - codec_name: av1
                codec_names: [av1_cuvid, libdav1d]
          output:
            audio_track_configs:
              - codec_name: aac
                codec_names: [libopus, aac]
            video_track_configs:
              - codec_name: h264
                codec_names: [h264_nvenc, libx264]
`))
	require.NoError(t, err)

	endpoint, ok := cfg.Endpoints["/camera"]
	require.True(t, ok)
	require.Len(t, endpoint.Forwardings, 1)

	transcoding := endpoint.Forwardings[0].Transcoding
	require.NotNil(t, transcoding)
	require.NotNil(t, transcoding.Input)
	require.Len(t, transcoding.Input.AudioTrackConfigs, 1)
	require.Len(t, transcoding.Input.VideoTrackConfigs, 1)
	require.Len(t, transcoding.Output.AudioTrackConfigs, 1)
	require.Len(t, transcoding.Output.VideoTrackConfigs, 1)

	inputAudio := transcoding.Input.AudioTrackConfigs[0]
	assert.Equal(t, codectypes.Name("aac"), inputAudio.CodecName)
	assert.Equal(t, []codectypes.Name{"libfdk_aac", "aac"}, inputAudio.CodecNames)

	inputVideo := transcoding.Input.VideoTrackConfigs[0]
	assert.Equal(t, codectypes.Name("av1"), inputVideo.CodecName)
	assert.Equal(t, []codectypes.Name{"av1_cuvid", "libdav1d"}, inputVideo.CodecNames)

	outputAudio := transcoding.Output.AudioTrackConfigs[0]
	assert.Equal(t, codectypes.Name("aac"), outputAudio.CodecName)
	assert.Equal(t, []codectypes.Name{"libopus", "aac"}, outputAudio.CodecNames)

	outputVideo := transcoding.Output.VideoTrackConfigs[0]
	assert.Equal(t, codectypes.Name("h264"), outputVideo.CodecName)
	assert.Equal(t, []codectypes.Name{"h264_nvenc", "libx264"}, outputVideo.CodecNames)
}
