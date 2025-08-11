PROTO_SRC := .
PROTO_FILES := $(wildcard $(PROTO_SRC)/*.proto)
GO_OUT := avdmanagementgrpc

.PHONY: all proto clean

all: proto

proto: $(PROTO_FILES)
	mkdir -p "$(GO_OUT)"
	protoc -I. -Iimports/avpipeline/protobuf --go_out=$(GO_OUT) --go-grpc_out=$(GO_OUT) --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative $^

clean:
	rm -f $(GO_OUT)/*.pb.go