.PHONY: all clean test

PACKAGES = $(shell go list ./... | grep -v /vendor/)

all: deps build

clean:
	go clean -i ./...
	find . -name \*.out -type f -delete

deps:
	go get -t ./...

build:
	CGO_ENABLED=0 go build -ldflags '-extldflags "-static"'

test:
	echo "mode: atomic" > coverage.out;
	@for PKG in $(PACKAGES); do go test -race -coverprofile $$GOPATH/src/$$PKG/coverage-package.out -covermode=atomic $$PKG || exit 1; cat $$GOPATH/src/$$PKG/coverage-package.out | grep -v mode: | sort -r >> coverage.out; done