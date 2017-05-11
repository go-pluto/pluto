package comm

import (
	"fmt"
	stdlog "log"
	"time"

	"crypto/tls"
)

// Functions

// ReliableConnect attempts to connect to defined remote node
// as longs as the error from previous attempts is possible
// to be dealt with.
func ReliableConnect(remoteAddr string, tlsConfig *tls.Config, retry int) (*tls.Conn, error) {

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
				return nil, fmt.Errorf("could not connect to port of node '%s' because of: %s", remoteAddr, err.Error())
			}
		}
	}

	return c, nil
}

// ReliableSend attempts to transmit a message between pluto
// nodes. If the first attempt fails, the node will try to
// reconnect and resend the message until successfully transmitted.
func ReliableSend(conn *tls.Conn, text string, remoteAddr string, tlsConfig *tls.Config, timeout int, retry int) error {

	// TODO: Make this routine first check for closed
	//       pipe and after that attempting 'ping' test
	//       with exponential (?) backoff for write deadline
	//       up to timeout.

	// Set configured timeout on waiting for response.
	conn.SetWriteDeadline(time.Now().Add(time.Duration(timeout) * time.Millisecond))

	// Test long-lived connection.
	_, err := conn.Write([]byte("> ping <\r\n"))
	if err != nil {
		return fmt.Errorf("sending ping to node '%s' failed with: %s\n", remoteAddr, err.Error())
	}

	// Wait for configured time to pass.
	time.Sleep(time.Duration(timeout) * time.Millisecond)

	// Disable write deadline again for future calls.
	conn.SetDeadline(time.Time{})

	// Write message to TLS connections.
	_, err = fmt.Fprintf(conn, "%s\r\n", text)
	for err != nil {

		stdlog.Printf("[comm.ReliableSend] Sending to node '%s' failed, trying to recover...\n", remoteAddr)

		// Define an error we can deal with.
		okError := fmt.Sprintf("write tcp %s->%s: write: broken pipe", conn.LocalAddr().String(), remoteAddr)

		if err.Error() == okError {

			// Connection was lost. Reconnect.
			conn, err = ReliableConnect(remoteAddr, tlsConfig, retry)
			if err != nil {
				return fmt.Errorf("could not reestablish connection with '%s': %s", remoteAddr, err.Error())
			}

			stdlog.Printf("[comm.ReliableSend] Reconnected to '%s'.\n", remoteAddr)

			// Resend message.
			_, err = fmt.Fprintf(conn, "%s\r\n", text)
		} else {
			return fmt.Errorf("could not reestablish connection with '%s': %s", remoteAddr, err.Error())
		}
	}

	return nil
}
