.PHONY: prod clean deps setup-test-env destroy-test-env exec-tests build pki test-pki test-public test

PACKAGES = $(shell go list ./... | grep -v /vendor/)
NUM_USERS := 9

prod: clean deps pki build

clean:
	go clean -i ./...
	find . -name \*.out -type f -delete
	find . -name test-\*.log -type f -delete
	rm -f generate_pki generate_cert generate_cert.go

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

test: destroy-test-env setup-test-env exec-tests

destroy-test-env:
	@echo ""
	if [ -d "private/Maildirs" ]; then rm -rf private/Maildirs; fi
	if [ -d "private/crdt-layers" ]; then rm -rf private/crdt-layers; fi

setup-test-env:
	@echo ""
	echo "mode: atomic" > coverage.out;
	if [ ! -d "private" ]; then mkdir private; fi
	chmod 0700 private
	if [ ! -d "private/Maildirs/worker-1" ]; then mkdir -p private/Maildirs/worker-1; fi
	@for i in `seq 0 ${NUM_USERS}`; do if [ ! -d "private/Maildirs/worker-1/user$${i}/new" ]; then mkdir -p private/Maildirs/worker-1/user$${i}/new; fi; done
	@for i in `seq 0 ${NUM_USERS}`; do if [ ! -d "private/Maildirs/worker-1/user$${i}/tmp" ]; then mkdir -p private/Maildirs/worker-1/user$${i}/tmp; fi; done
	@for i in `seq 0 ${NUM_USERS}`; do if [ ! -d "private/Maildirs/worker-1/user$${i}/cur" ]; then mkdir -p private/Maildirs/worker-1/user$${i}/cur; fi; done
	if [ ! -d "private/Maildirs/storage" ]; then mkdir -p private/Maildirs/storage; fi
	@for i in `seq 0 ${NUM_USERS}`; do if [ ! -d "private/Maildirs/storage/user$${i}/new" ]; then mkdir -p private/Maildirs/storage/user$${i}/new; fi; done
	@for i in `seq 0 ${NUM_USERS}`; do if [ ! -d "private/Maildirs/storage/user$${i}/tmp" ]; then mkdir -p private/Maildirs/storage/user$${i}/tmp; fi; done
	@for i in `seq 0 ${NUM_USERS}`; do if [ ! -d "private/Maildirs/storage/user$${i}/cur" ]; then mkdir -p private/Maildirs/storage/user$${i}/cur; fi; done
	chmod -R 0700 private/Maildirs/*
	if [ ! -d "private/crdt-layers/worker-1" ]; then mkdir -p private/crdt-layers/worker-1; fi
	@for i in `seq 0 ${NUM_USERS}`; do \
		if [ ! -d "private/crdt-layers/worker-1/user$${i}" ]; then mkdir -p private/crdt-layers/worker-1/user$${i}; fi; \
		if [ ! -f "private/crdt-layers/worker-1/user$${i}/mailbox-structure.log" ]; then touch private/crdt-layers/worker-1/user$${i}/mailbox-structure.log && echo -n "SU5CT1g=;aa$${i}cc488-7469-4984-8f88-27adc426ab6a" > private/crdt-layers/worker-1/user$${i}/mailbox-structure.log; fi; \
		if [ ! -f "private/crdt-layers/worker-1/user$${i}/INBOX.log" ]; then touch private/crdt-layers/worker-1/user$${i}/INBOX.log; fi; \
	done
	if [ ! -d "private/crdt-layers/storage" ]; then mkdir -p private/crdt-layers/storage; fi
	@for i in `seq 0 ${NUM_USERS}`; do \
		if [ ! -d "private/crdt-layers/storage/user$${i}" ]; then mkdir -p private/crdt-layers/storage/user$${i}; fi; \
		if [ ! -f "private/crdt-layers/storage/user$${i}/mailbox-structure.log" ]; then touch private/crdt-layers/storage/user$${i}/mailbox-structure.log && echo -n "SU5CT1g=;bb$${i}cc488-7469-4984-8f88-27adc426ab6a" > private/crdt-layers/storage/user$${i}/mailbox-structure.log; fi; \
		if [ ! -f "private/crdt-layers/storage/user$${i}/INBOX.log" ]; then touch private/crdt-layers/storage/user$${i}/INBOX.log; fi; \
	done

exec-tests:
	@echo ""
	@for PKG in $(PACKAGES); do \
		go test -v -race -coverprofile $${GOPATH}/src/$${PKG}/coverage-package.out -covermode=atomic $${PKG} || exit 1; \
		test ! -f $${GOPATH}/src/$${PKG}/coverage-package.out || (cat $${GOPATH}/src/$${PKG}/coverage-package.out | grep -v mode: | sort -r >> coverage.out); \
	done
	@echo ""