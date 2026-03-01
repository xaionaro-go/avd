.PHONY: all build test test-unit test-race test-cover test-e2e vet proto clean

all: vet test build

# Build
build:
	go build -o build/avd ./cmd/avd
	go build -o build/avcli ./cmd/avcli

# Run all tests (unit + race)
test: test-unit test-race

# Unit tests
test-unit:
	go test ./...

# Race detector
test-race:
	go test -race ./...

# Coverage report
test-cover:
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out
	@echo ""
	@echo "HTML report: go tool cover -html=coverage.out"

# E2E tests requiring OpenCV (build tag: with_cv)
test-e2e:
	go test -tags with_cv -count=1 -timeout 120s ./tests/e2e/...

# Static analysis
vet:
	go vet ./...

# Protobuf generation
proto:
	$(MAKE) -C pkg/management/grpc/proto

# Remove build artifacts
clean:
	rm -f build/avd build/avcli coverage.out
