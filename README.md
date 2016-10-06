# Pluto

[![GoDoc](https://godoc.org/github.com/numbleroot/pluto?status.svg)](https://godoc.org/github.com/numbleroot/pluto) [![Build Status](https://travis-ci.org/numbleroot/pluto.svg?branch=master)](https://travis-ci.org/numbleroot/pluto)

Pluto is a distributed IMAP server that implements a subset of the [IMAPv4 standard](https://tools.ietf.org/html/rfc3501). It makes use of [Conflict-free Replicated Data Types](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type) to allow state to be kept on each worker node but still achieve system-wide convergence of user data. Pluto is written in Go.


## Status

**Work in progress:** This is the code base for my Bachelor Thesis. It is heavily work in progress and not ready yet.


## Installation

If you have a working [Go](https://golang.org/) setup, installation is as easy as:

```bash
 $ go get github.com/numbleroot/pluto
```


## Usage


## Acknowledgments


## License

This project is [GPLv3](https://github.com/numbleroot/pluto/blob/master/LICENSE) licensed.