.PHONY: all clean deps build pki test-certs test

# Set this to your number of configured worker
# nodes, see your main configuration file.
NUMBER_OF_WORKER_NODES=3

PACKAGES = $(shell go list ./... | grep -v /vendor/)

all: deps build

clean:
	go clean -i ./...
	find . -name \*.out -type f -delete
	rm -f generate_pki

deps:
	go get -t ./...

build:
	CGO_ENABLED=0 go build -ldflags '-extldflags "-static"'

pki:
	if [ ! -d "private" ]; then mkdir private; fi
	chmod 0700 private
	go build crypto/generate_pki.go
	./generate_pki -path-prefix ./
	rm generate_pki

test-certs:
	if [ ! -d "private" ]; then mkdir private; fi
	chmod 0700 private
	go build crypto/generate_pki.go
	./generate_pki -path-prefix ./ -rsa-bits 1024
	rm generate_pki

test:
	echo "mode: atomic" > coverage.out;
	@for PKG in $(PACKAGES); do go test -race -coverprofile $$GOPATH/src/$$PKG/coverage-package.out -covermode=atomic $$PKG || exit 1; test ! -f $$GOPATH/src/$$PKG/coverage-package.out || (cat $$GOPATH/src/$$PKG/coverage-package.out | grep -v mode: | sort -r >> coverage.out); done