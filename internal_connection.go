package main

import (
	"fmt"
	"time"

	"crypto/tls"

	"github.com/numbleroot/pluto/crypto"
)

type internalConnection struct {
	config *tls.Config
	retry  int
}

func NewInternalConnection(certLoc, keyLoc, rootCertPath string, retry int) (*internalConnection, error) {
	// Load internal TLS config.
	tlsConfig, err := crypto.NewInternalTLSConfig(certLoc, keyLoc, rootCertPath)
	if err != nil {
		return nil, err
	}

	return &internalConnection{config: tlsConfig}, nil
}

func (c *internalConnection) ReliableConnect(addr string) (*tls.Conn, error) {

	var err error
	var conn *tls.Conn

	// Define what an error looks like we can deal with.
	okError := fmt.Sprintf("dial tcp %s: getsockopt: connection refused", addr)

	// Initially, set error string to the one we can deal with.
	err = fmt.Errorf(okError)

	for err != nil {

		// Attempt to connect to worker node.
		conn, err = tls.Dial("tcp", addr, c.config)
		if err != nil {

			if err.Error() == okError {
				time.Sleep(time.Duration(c.retry) * time.Millisecond)
			} else {
				return nil, fmt.Errorf("could not connect to port of node '%s' because of: %s", addr, err.Error())
			}
		}
	}

	return conn, nil
}
