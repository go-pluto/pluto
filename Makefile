.PHONY: all clean certs test

PACKAGES = $(shell go list ./... | grep -v /vendor/)

all: deps build

clean:
	go clean -i ./...
	find . -name \*.out -type f -delete
	rm -f generate_cert generate_cert.go

deps:
	go get -t ./...

build:
	CGO_ENABLED=0 go build -ldflags '-extldflags "-static"'

certs:
	mkdir private
	chmod 0700 private
	wget https://raw.githubusercontent.com/golang/go/master/src/crypto/tls/generate_cert.go
	go build generate_cert.go
	./generate_cert -ca -duration 2160h -host localhost,127.0.0.1 -rsa-bits 8192
	mv cert.pem private/cert.pem && mv key.pem private/key.pem
	go clean
	rm -f generate_cert.go

test-certs:
	mkdir private
	chmod 0700 private
	wget https://raw.githubusercontent.com/golang/go/master/src/crypto/tls/generate_cert.go
	go build generate_cert.go
	./generate_cert -ca -duration 1h -host localhost,127.0.0.1 -rsa-bits 1024
	mv cert.pem private/cert.pem && mv key.pem private/key.pem
	go clean
	rm -f generate_cert.go

test:
	echo "mode: atomic" > coverage.out;
	@for PKG in $(PACKAGES); do go test -race -coverprofile $$GOPATH/src/$$PKG/coverage-package.out -covermode=atomic $$PKG || exit 1; [[ ! -f $$GOPATH/src/$$PKG/coverage-package.out ]] || (cat $$GOPATH/src/$$PKG/coverage-package.out | grep -v mode: | sort -r >> coverage.out); done