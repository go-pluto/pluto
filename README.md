# Pluto

[![GoDoc](https://godoc.org/github.com/numbleroot/pluto?status.svg)](https://godoc.org/github.com/numbleroot/pluto) [![Build Status](https://travis-ci.org/numbleroot/pluto.svg?branch=master)](https://travis-ci.org/numbleroot/pluto) [![Go Report Card](https://goreportcard.com/badge/github.com/numbleroot/pluto)](https://goreportcard.com/report/github.com/numbleroot/pluto) [![codecov](https://codecov.io/gh/numbleroot/pluto/branch/master/graph/badge.svg)](https://codecov.io/gh/numbleroot/pluto)

Pluto is a distributed IMAP server that implements a subset of the [IMAPv4 standard](https://tools.ietf.org/html/rfc3501). It makes use of [Conflict-free Replicated Data Types](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type) to allow state to be kept on each worker node but still achieve system-wide convergence of user data. Pluto is written in Go.


## Status

**Work in progress:** This is the code base for my Bachelor Thesis. It is heavily work in progress and not ready yet.


## Installation

If you have a working [Go](https://golang.org/) setup, installation is as easy as:

```bash
 $ go get github.com/numbleroot/pluto
```

You need to provide a valid TLS certificate. Either you use your existing certificate or you could use the provided `certs` target with `make` to generate them, e.g.:

```bash
$ make certs
```

This will generate sufficiently large certificates that are valid for 90 day. The target basically performs the following steps and makes use of [this script](https://github.com/golang/go/blob/master/src/crypto/tls/generate_cert.go) to generate the certificates:

```bash
$ mkdir private
$ chmod 0700 private
$ wget https://raw.githubusercontent.com/golang/go/master/src/crypto/tls/generate_cert.go
$ go build generate_cert.go
$ ./generate_cert -ca -duration 2160h -host localhost,127.0.0.1 -rsa-bits 8192
$ mv cert.pem private/cert.pem && mv key.pem private/key.pem
$ go clean
$ rm -f generate_cert.go
```


## Usage


## Acknowledgments


## License

This project is [GPLv3](https://github.com/numbleroot/pluto/blob/master/LICENSE) licensed.