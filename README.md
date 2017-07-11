# Pluto

[![GoDoc](https://godoc.org/github.com/go-pluto/pluto?status.svg)](https://godoc.org/github.com/go-pluto/pluto) [![License: GPLv3](https://img.shields.io/badge/license-GPLv3-blue.svg)](https://github.com/go-pluto/pluto/blob/master/LICENSE) [![Build Status](https://drone.go-pluto.de/api/badges/go-pluto/pluto/status.svg)](https://drone.go-pluto.de/go-pluto/pluto) [![Build Status](https://travis-ci.org/go-pluto/pluto.svg?branch=master)](https://travis-ci.org/go-pluto/pluto) [![Go Report Card](https://goreportcard.com/badge/github.com/go-pluto/pluto)](https://goreportcard.com/report/github.com/go-pluto/pluto) [![Issue Count](https://codeclimate.com/github/go-pluto/pluto/badges/issue_count.svg)](https://codeclimate.com/github/go-pluto/pluto) [![codecov](https://codecov.io/gh/go-pluto/pluto/branch/master/graph/badge.svg)](https://codecov.io/gh/go-pluto/pluto)

Pluto is a distributed IMAP server that implements a subset of the [IMAPv4 standard](https://tools.ietf.org/html/rfc3501). It makes use of [Conflict-free Replicated Data Types](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type) (operation-based style) defined by Shapiro et al. to replicate application state into traditionally stateless tiers (worker nodes) and still achieve system-wide convergence of user data. By this, pluto attempts to offer a solution towards the challenges arising from Brewer's [CAP theorem](https://en.wikipedia.org/wiki/CAP_theorem): in case of failures in the system, choose to stay available by accepting reduced consistency in form of the CRDTs' [strong eventual consistency](https://en.wikipedia.org/wiki/Eventual_consistency#Strong_eventual_consistency). We described our system architecture, the modelled IMAP operations based on OR-Set CmRDTs, and a response time evaluation of this prototype compared to Dovecot in our paper called ["pluto: The CRDT-Driven IMAP Server"](https://dl.acm.org/citation.cfm?id=3064891), presented at [PaPoC 2017](http://software.imdea.org/Conferences/PAPOC17/).

Pluto is written in Go and provided under copyleft license [GPLv3](https://github.com/go-pluto/pluto/blob/master/LICENSE).


## Status

**Use with caution and not in production:** this is a prototypical implementation of the system architecture and presented concepts discussed in my Bachelor Thesis and continued in our project work during summer term 2017. Pluto's code is generally in flux due to ongoing development and inclusion of new ideas and concepts.


## Installation

If you have a working [Go](https://golang.org/) setup, installation is as easy as:

```bash
 $ go get -u github.com/go-pluto/pluto
```


## Quick Start

You need to configure your pluto setup in a `config.toml` file. Please refer to the provided [config.toml.example](https://github.com/go-pluto/pluto/blob/master/config.toml.example) for an exemplary configuration that only needs adaptation to your requirements.

**Important:** Please make sure, not to include `|` (pipe character) in names for your distributor, worker and storage nodes as this character is used in marshalled messages sent on internal network.

If you know what happens in background, these are the steps to take in order to ready pluto for use:

```bash
 $ cp config.toml.example config.toml            # Use example file as basis for your config file
 $ vim config.toml                               # Adjust config to your setup and needs
 $ make pki                                      # Creates internally used system of certificates
 $ make deps                                     # Fetch all dependencies
 $ make build                                    # Compile pluto
 $ scp pluto-and-private-bundle <other nodes>    # Distribute pluto binary to other network nodes
 $ ./pluto -storage                              # On node 'storage'
 $ ./pluto -worker worker-1                      # On node 'worker-1'
 $ ./pluto -worker worker-2                      # On node 'worker-2'
 $ ./pluto -worker worker-$ID                    # Repeat this on all remaining worker nodes
 $ ./pluto -distributor                          # On node 'distributor'
```

Now you should be able to access your IMAP server from the Internet under configured IP.


## User authentication

Currently, two schemes are available to authenticate users though provided `auth` package easily is extensible to fit specific authentication scenarios.


### PostgreSQL

If you plan on using a PostgreSQL database to provide an user information table, you need to have a PostgreSQL running somewhere. If you need a new user that owns the database, you might use these commands on the PostgreSQL host:

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


### Plain text file

If you would like to easily test a configuration or you are developing locally, a plain text file holding user information might suffice. Simply put lines of user names and user passwords delimited by a reserved symbol (e.g. `;`) into a text file and configure your `config.toml` accordingly.

An authentication file called `local-dev-users.txt` might look like:

```
username1;secret1
username2;secret2
username3;secret3
...
```

In your `config.toml` you set the distributor node to authenticate users based on this file, for example:

```
[Distributor]
...
AuthAdapter = "AuthFile"

    ...

    [Distributor.AuthFile]
    File = "/path/to/your/setup/local-dev-users.txt"
    Separator = ";"
...
```

**Warning:** Please dot not use this scheme in any places **real** user data is involved. **It is not considered secure.**


## Certificates

There are multiple certificates needed in order to operate a pluto setup. Fortunately, you only have to provide one certificate that is valid for normal use in e.g. webservers. The other required certificates are used for internal communication among pluto nodes and will be generated by a simple Makefile command.

So first, you need to provide a valid public TLS certificate that the distributor node can present to Internet-faced connections. It is recommended that you obtain a certificate signed by a Certificate Authority (CA) that is accepted by most clients per default, e.g. [Let's Encrypt](https://letsencrypt.org/). For testing though, you can use the provided `test-public` Makefile target and call it to generate a self-signed certificate:

```bash
$ make test-public
```

This will generate one certificate that is valid for 90 day. The target basically performs the following steps and makes use of [this script](https://github.com/golang/go/blob/master/src/crypto/tls/generate_cert.go) to generate the certificates:

```bash
$ if [ ! -d "private" ]; then mkdir private; fi
$ chmod 0700 private
$ wget https://raw.githubusercontent.com/golang/go/master/src/crypto/tls/generate_cert.go
$ go build generate_cert.go
$ ./generate_cert -ca -duration 2160h -host localhost,127.0.0.1,::1 -rsa-bits 1024
$ mv cert.pem private/public-distributor-cert.pem && mv key.pem private/public-distributor-key.pem
$ go clean
$ rm -f generate_cert.go
```

Please keep in mind that the certificate generated via this command really only is to be used for testing, never in production mode. It is self-signed for localhost addresses and only of 1024 bits key length which should not be used anywhere serious anymore.

The remaining required certificates, mentioned internal ones, can simply be generated by running:

```bash
$ make pki
```


## Evaluation

Evaluation scripts to benchmark pluto's response time performance against the de facto standard IMAP server [Dovecot](https://www.dovecot.org) are provided in repository [evaluation](https://github.com/go-pluto/evaluation).


## Acknowledgments

* [Tim Jungnickel](https://github.com/TimJuni): initial project idea, feedback and ideas, thesis supervision, paper. Since summer term 2017 also actively involved in verification and evaluation part.
* [Matthias Loibl](https://github.com/MetalMatze): routinely reviewed and improved my Go code during thesis. Since summer term 2017 active contributor in the areas engineering and evaluation.


## License

This project is [GPLv3](https://github.com/go-pluto/pluto/blob/master/LICENSE) licensed.
