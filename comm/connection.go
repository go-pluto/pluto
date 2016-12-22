package comm

import (
	"bufio"
	"fmt"
	"log"
	"strings"
	"time"

	"crypto/tls"
)

// Functions

// ReliableConnect attempts to connect to defined remote node
// as longs as the error from previous attempts is possible
// to be dealt with.
func ReliableConnect(name string, remoteName string, remoteIP string, remotePort string, tlsConfig *tls.Config, retry int) (*tls.Conn, error) {

	var err error
	var c *tls.Conn

	// Define address we are trying to connect to.
	nodeAddr := fmt.Sprintf("%s:%s", remoteIP, remotePort)

	// Define what an error looks like we can deal with.
	okError := fmt.Sprintf("dial tcp %s: getsockopt: connection refused", nodeAddr)

	// Initially, set error string to the one we can deal with.
	err = fmt.Errorf(okError)

	for err != nil {

		// Attempt to connect to worker node.
		c, err = tls.Dial("tcp", nodeAddr, tlsConfig)
		if err != nil {

			if err.Error() == okError {
				time.Sleep(time.Duration(retry) * time.Millisecond)
			} else {
				return nil, fmt.Errorf("%s: Could not connect to port of node '%s' because of: %s\n", name, remoteName, err.Error())
			}
		}
	}

	log.Printf("%s: Successfully connected to worker node '%s'.\n", name, remoteName)

	return c, nil
}

// ReliableSend sends text to other node specified and
// tries to reconnect in case of simple disconnects.
func ReliableSend(conn *tls.Conn, text string, name string, remoteName string, remoteIP string, remotePort string, tlsConfig *tls.Config, timeout int, retry int) (*tls.Conn, bool, error) {

	var err error
	var replacedConn *tls.Conn

	// Track if we replaced the connection.
	replaced := false

	// Set configured timeout on waiting for response.
	conn.SetWriteDeadline(time.Now().Add(time.Duration(timeout) * time.Millisecond))

	// Test long-lived connection.
	_, err = conn.Write([]byte("> ping <\n"))
	if err != nil {
		return nil, false, fmt.Errorf("sending ping to node '%s' failed with: %s\n", remoteName, err.Error())
	}

	// Wait for configured time to pass.
	time.Sleep(time.Duration(timeout) * time.Millisecond)

	// Disable write deadline again for future calls.
	conn.SetDeadline(time.Time{})

	// Write message to TLS connections.
	_, err = fmt.Fprintf(conn, "%s\n", text)
	for err != nil {

		log.Printf("[comm.ReliableSend] Sending to node '%s' failed, trying to recover...\n", remoteName)

		// Define an error we can deal with.
		okError := fmt.Sprintf("write tcp %s->%s: write: broken pipe", conn.LocalAddr(), conn.RemoteAddr())

		if err.Error() == okError {

			// Connection was lost. Reconnect.
			replacedConn, err = ReliableConnect(name, remoteName, remoteIP, remotePort, tlsConfig, retry)
			if err != nil {
				return nil, false, fmt.Errorf("could not reestablish connection with '%s': %s\n", remoteName, err.Error())
			}

			// Indicate we replaced connection.
			replaced = true

			// Wait configured time before attempting next transfer.
			time.Sleep(time.Duration(retry) * time.Millisecond)

			// Retry transfer.
			_, err = fmt.Fprintf(replacedConn, "%s\n", text)
		} else {
			return nil, false, fmt.Errorf("could not reestablish connection with '%s': %s\n", remoteName, err.Error())
		}
	}

	if replaced {
		return replacedConn, replaced, nil
	}

	return conn, replaced, nil
}

// InternalSend is used by nodes of the pluto system to
// successfully transmit a message to another node or
// fail definitely. This prevents further handler advancement
// in case a link failed.
func InternalSend(conn *tls.Conn, text string, name string, remoteName string) error {

	// Test long-lived connection.
	_, err := conn.Write([]byte("> ping <\n"))
	if err != nil {
		return fmt.Errorf("%s: sending ping to node '%s' failed: %s\n", name, remoteName, err.Error())
	}

	// Write message to TLS connections.
	_, err = fmt.Fprintf(conn, "%s\n", text)
	for err != nil {
		return fmt.Errorf("%s: sending message to node '%s' failed: %s\n", name, remoteName, err.Error())
	}

	return nil
}

// InternalReceive is used by nodes in the pluto system
// receive an incoming message and filter out all prior
// received ping message.
func InternalReceive(reader *bufio.Reader) (string, error) {

	var err error

	// Initial value for received message in order
	// to skip past the mandatory ping message.
	text := "> ping <\n"

	for text == "> ping <\n" {

		text, err = reader.ReadString('\n')
		if err != nil {
			break
		}
	}

	// If an error happened, return it.
	if err != nil {
		return "", err
	}

	return strings.TrimRight(text, "\n"), nil
}
