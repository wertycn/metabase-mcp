APP     := metabase-mcp
VERSION ?= dev

# Build output directory
DIST    := dist

# Platforms for release builds
PLATFORMS := \
	linux/amd64 \
	linux/arm64 \
	darwin/amd64 \
	darwin/arm64 \
	windows/amd64

.PHONY: all build test install-deps release clean

## install-deps: download Go module dependencies
install-deps:
	go mod download
	@echo "✓ dependencies downloaded"

## build: compile for the current platform
build:
	CGO_ENABLED=0 go build -ldflags="-s -w -X main.version=$(VERSION)" -o $(APP) .
	@echo "✓ built ./$(APP)"

## test: run all tests
test:
	go test ./...
	@echo "✓ tests passed"

## release VERSION=x.y.z: cross-compile for all platforms → dist/
release: clean-dist
	@mkdir -p $(DIST)
	@$(foreach PLATFORM,$(PLATFORMS), \
		$(eval GOOS   := $(word 1,$(subst /, ,$(PLATFORM)))) \
		$(eval GOARCH := $(word 2,$(subst /, ,$(PLATFORM)))) \
		$(eval EXT    := $(if $(filter windows,$(GOOS)),.exe,)) \
		$(eval OUT    := $(DIST)/$(APP)-$(VERSION)-$(GOOS)-$(GOARCH)$(EXT)) \
		echo "→ building $(OUT)"; \
		CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) \
			go build -ldflags="-s -w -X main.version=$(VERSION)" -o $(OUT) . ; \
	)
	@echo "── release artifacts ──"
	@ls -lh $(DIST)/
	@echo "✓ release $(VERSION) done"

## clean: remove build artifacts
clean: clean-dist
	rm -f $(APP)
	@echo "✓ cleaned"

clean-dist:
	rm -rf $(DIST)
