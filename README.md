# `avd` -- (Lib)AV daemon

`avd` -- is a streaming server that uses [`libav`](https://github.com/FFmpeg/FFmpeg) under the hood (as an alternative to [`nginx-module-rtmp`](https://docs.nginx.com/nginx/admin-guide/dynamic-modules/rtmp/), [`mediamtx`](https://github.com/bluenviron/mediamtx), [`MonaServer`](https://github.com/MonaSolutions/MonaServer) or whatnot).

On one hand, `libav` is a powerful, fine-polished and fine-tuned video/audio processing library, that supports RTMP, RTSP, SRT and other protocols out of the box.
On the other hand, `libav` lacks capabilities to serve these protocols beyond just accepting a pre-defined stream or/and using the protocols as a client.

`avd` fixes that problem, by wrapping around the `libav`'s capability to accept a single pre-defined stream in a way to provide a generic server that may accept multiple streams and that could be processed pretty similar to how you would do it with a normal streaming server.

# Motivation

The best I found as a streaming server was [`mediamtx`](https://github.com/bluenviron/mediamtx), but unfortunately it handled pretty poorly all of my edge cases. While I need a tool I can trust: a tool that just works.

Investigating myself all the intricacies of H264, RTMP, RTSP, SRT, HEVC, AAC, etc, to find a way to workaround `mediamtx` was taking too much time. Moreover, `mediamtx` does not allow really integrating into other projects (because it keeps everything in `internal`) which is in a strong conflict with [one of my hobby project](https://github.com/xaionaro-go/streamctl/).

So I decided to just reuse all the fine-polishing of `libav` a make a server out of that. Gladfully, I've already previously made a library that makes that easy: [`avpipeline`](https://github.com/xaionaro-go/avpipeline).

### The alternatives tried before starting this project

* [`nginx-module-rtmp`](https://docs.nginx.com/nginx/admin-guide/dynamic-modules/rtmp/): very poor debugging, does not support the protocols I need; and not integratable into another Go project.
* [`mediamtx`](https://github.com/bluenviron/mediamtx): does not work on my edge cases; and not integratable into another Go project.
* [`livego`](https://github.com/gwuhaolin/livego): it was much worse than mediamtx for my use cases (do not remember the exact reasons); and not integratable into another Go project.
* [`go2rtc`](https://github.com/AlexxIT/go2rtc): it appeared to be just a forwarding/routing server, rather than a normal server (e.g.: [ITS#1238](https://github.com/AlexxIT/go2rtc/issues/1238#issuecomment-2237036661)); not integratable into another Go project; and even those were not problems by now I'm convinced it would not have handled my edge cases better than mediamtx.
* So on.

I also tried to solve my problems with just small libraries/packages, e.g. [github.com/yutopp/go-rtmp](https://github.com/yutopp/go-rtmp) (see also [github.com/xaionaro-go/go-rtmp](https://github.com/xaionaro-go/go-rtmp)), but all of them were even further from supporting my edge cases. For example, IIRC, `go-rtmp` did not even support multihour streams (the timestamp field in RTMP was overflowing). 

The general pattern is that a project:
* Does not support protocols I need.
* Works badly in edge cases.
* Is too difficult to build for Android/iOS/Linux/whatever.
* Is not integratable into an existing Go project.
* Has major bugs/limitations even in normal cases.

So the hope is that if I'll just use `libav` I'll avoid these problems better than the other projects, but with focus on solving my personal edge cases.

# Quick start

```sh
$ avd --generate-config | tee ~/.avd.conf
```
```
ports:
- address: tcp:127.0.0.1:1936
  rtmp:
    mode: "publishers"
- address: tcp:0.0.0.0:1935
  rtmp:
    mode: "consumers"
endpoints:
  mystream:
    forwardings:
    - destination:
        url: ""
        route: ""
      recoding: {}
```

```sh
$ avd
```
