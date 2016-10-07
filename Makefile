.PHONY: all clean test

PACKAGES = $(shell go list ./... | grep -v /vendor/)

all: deps build

clean:
	go clean -i ./...

deps:
	go get -t ./...

build:
	CGO_ENABLED=0 go build -ldflags '-extldflags "-static"'

test:
	echo "mode: set" > coverage.out;
	@for PKG in $(PACKAGES); do go test -coverprofile $$GOPATH/src/$$PKG/coverage-package.out $$PKG || exit 1; cat $$GOPATH/src/$$PKG/coverage-package.out | grep -v mode: | sort -r >> coverage.out; done