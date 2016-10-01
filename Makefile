.PHONY: all clean

all: deps build

clean:
	go clean -i ./...

deps:
	go get -t ./...

build:
	CGO_ENABLED=0 go build -ldflags '-extldflags "-static"'