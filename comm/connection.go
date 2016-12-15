package comm

import (
	"fmt"
	"log"
	"time"

	"crypto/tls"
)

// Functions

// ReliableConnect attempts to connect to defined remote node
// as longs as the error from previous attempts is possible
// to be dealt with.
func ReliableConnect(name string, remoteName string, remoteIP string, remotePort string, tlsConfig *tls.Config, wait int, retry int) (*tls.Conn, error) {

	var err error
	var c *tls.Conn

	// Define address we are trying to connect to.
	nodeAddr := fmt.Sprintf("%s:%s", remoteIP, remotePort)

	// Define what an error looks like we can deal with.
	okError := fmt.Sprintf("dial tcp %s: getsockopt: connection refused", nodeAddr)

	// Initially, set error string to the one we can deal with.
	err = fmt.Errorf(okError)

	// In the beginning, give the other nodes some time to become available.
	time.Sleep(time.Duration(wait) * time.Millisecond)

	for err != nil {

		// Attempt to connect to worker node.
		c, err = tls.Dial("tcp", nodeAddr, tlsConfig)
		if err != nil {

			if err.Error() == okError {
				time.Sleep(time.Duration(retry) * time.Millisecond)
			} else {
				return nil, fmt.Errorf("%s: Could not connect to sync port of node '%s' because of: %s\n", name, remoteName, err.Error())
			}
		}
	}

	log.Printf("%s: Successfully connected to worker node '%s'.\n", name, remoteName)

	return c, nil
}
