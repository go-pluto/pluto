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

You need to provide a valid TLS certificate. Either you use your existing certificate or you could use the provided `certs` target with `make` to generate them. Make sure to set the `NUMBER_OF_WORKER_NODES` variable to the correct amount of configured worker nodes in your system. After done that, run:

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
$ mv cert.pem private/distributor-cert.pem && mv key.pem private/distributor-key.pem
$ for ID in `seq 1 ${NUMBER_OF_WORKER_NODES}`; do ./generate_cert -ca -duration 2160h -host localhost,127.0.0.1 -rsa-bits 8192; mv cert.pem private/worker-${ID}-cert.pem && mv key.pem private/worker-${ID}-key.pem; done
$ ./generate_cert -ca -duration 2160h -host localhost,127.0.0.1 -rsa-bits 8192
$ mv cert.pem private/storage-cert.pem && mv key.pem private/storage-key.pem
$ go clean
$ rm -f generate_cert.go
```

If you plan on using a PostgreSQL database for storing the user authorization information, you need to have a PostgreSQL running somewhere. If you need a new user that owns the database, you might use these commands on the PostgreSQL host:

```bash
user@system $ sudo -i -u postgres
postgres@system $ createuser --encrypted --pwprompt --createdb --no-createrole --no-superuser pluto
Enter password for new role:
Enter it again:
postgres@system $ exit
```

After that, you can create a new database `pluto` to hold the user information like so:

```bash
user@system $ createdb -U pluto pluto
```


## Usage


## Acknowledgments


## License

This project is [GPLv3](https://github.com/numbleroot/pluto/blob/master/LICENSE) licensed.