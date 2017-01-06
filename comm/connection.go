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
func ReliableConnect(remoteName string, remoteAddr string, tlsConfig *tls.Config, retry int) (*tls.Conn, error) {

	var err error
	var c *tls.Conn

	// Define what an error looks like we can deal with.
	okError := fmt.Sprintf("dial tcp %s: getsockopt: connection refused", remoteAddr)

	// Initially, set error string to the one we can deal with.
	err = fmt.Errorf(okError)

	for err != nil {

		// Attempt to connect to worker node.
		c, err = tls.Dial("tcp", remoteAddr, tlsConfig)
		if err != nil {

			if err.Error() == okError {
				time.Sleep(time.Duration(retry) * time.Millisecond)
			} else {
				return nil, fmt.Errorf("Could not connect to port of node '%s' because of: %s\n", remoteName, err.Error())
			}
		}
	}

	log.Printf("Successfully connected to worker node '%s'.\n", remoteName)

	return c, nil
}

// ReliableSend sends text to other node specified and
// tries to reconnect in case of simple disconnects.
func ReliableSend(conn *tls.Conn, text string, remoteName string, remoteAddr string, tlsConfig *tls.Config, timeout int, retry int) error {

	var err error
	var replacedConn *tls.Conn

	// Set configured timeout on waiting for response.
	conn.SetWriteDeadline(time.Now().Add(time.Duration(timeout) * time.Millisecond))

	// Test long-lived connection.
	_, err = conn.Write([]byte("> ping <\r\n"))
	if err != nil {
		return fmt.Errorf("sending ping to node '%s' failed with: %s\n", remoteName, err.Error())
	}

	// Wait for configured time to pass.
	time.Sleep(time.Duration(timeout) * time.Millisecond)

	// Disable write deadline again for future calls.
	conn.SetDeadline(time.Time{})

	// Write message to TLS connections.
	_, err = fmt.Fprintf(conn, "%s\r\n", text)
	for err != nil {

		log.Printf("[comm.ReliableSend] Sending to node '%s' failed, trying to recover...\n", remoteName)

		// Define an error we can deal with.
		okError := fmt.Sprintf("write tcp %s->%s: write: broken pipe", conn.LocalAddr(), conn.RemoteAddr())

		if err.Error() == okError {

			// Connection was lost. Reconnect.
			replacedConn, err = ReliableConnect(remoteName, remoteAddr, tlsConfig, retry)
			if err != nil {
				return fmt.Errorf("could not reestablish connection with '%s': %s\n", remoteName, err.Error())
			}

			// Retry transfer.
			_, err = fmt.Fprintf(replacedConn, "%s\r\n", text)
		} else {
			return fmt.Errorf("could not reestablish connection with '%s': %s\n", remoteName, err.Error())
		}
	}

	return nil
}
