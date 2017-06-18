.PHONY: all clean build docker pki test-pki test-public test

PACKAGES = $(shell go list ./... | grep -v /vendor/)

all: clean build

clean:
	go clean -i ./...
	find . -name \*.out -type f -delete
	find . -name test-\*.log -type f -delete
	rm -f generate_pki generate_cert generate_cert.go

proto:
	protoc -I imap/ imap/node.proto --go_out=plugins=grpc:imap
	protoc -I comm/ comm/receiver.proto --go_out=plugins=grpc:comm

build:
	CGO_ENABLED=0 go build -ldflags '-extldflags "-static"'

docker: build
	docker build -t numbleroot/pluto .

pki:
	if [ ! -d "private" ]; then mkdir private; fi
	chmod 0700 private
	go build crypto/generate_pki.go
	./generate_pki -path-prefix ./
	rm generate_pki

test-pki:
	if [ ! -d "private" ]; then mkdir private; fi
	chmod 0700 private
	go build crypto/generate_pki.go
	./generate_pki -path-prefix ./ -pluto-config test-config.toml -rsa-bits 1024
	rm generate_pki

test-public:
	if [ ! -d "private" ]; then mkdir private; fi
	chmod 0700 private
	wget https://raw.githubusercontent.com/golang/go/master/src/crypto/tls/generate_cert.go
	go build generate_cert.go
	./generate_cert -ca -duration 2160h -host localhost,127.0.0.1,::1 -rsa-bits 1024
	mv cert.pem private/public-distributor-cert.pem && mv key.pem private/public-distributor-key.pem
	go clean
	rm -f generate_cert.go

test:
	echo "mode: atomic" > coverage.out;
	@echo ""
	if [ -d "private/Maildirs" ]; then rm -rf private/Maildirs; fi
	if [ -d "private/crdt-layers" ]; then rm -rf private/crdt-layers; fi
	@echo ""
	@for PKG in $(PACKAGES); do \
		go test -v -race -coverprofile $${GOPATH}/src/$${PKG}/coverage-package.out -covermode=atomic $${PKG} || exit 1; \
		test ! -f $${GOPATH}/src/$${PKG}/coverage-package.out || (cat $${GOPATH}/src/$${PKG}/coverage-package.out | grep -v mode: | sort -r >> coverage.out); \
	done
