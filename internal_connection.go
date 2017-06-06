package main

import (
	"fmt"
	"time"

	"crypto/tls"
)

type intlConn struct {
	config *tls.Config
	retry  int
}

// ReliableConnect provides a mechanism for nodes in
// pluto's internal network to reliably contact one another.
func (c *intlConn) ReliableConnect(addr string) (*tls.Conn, error) {

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
