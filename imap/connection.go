package imap

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"crypto/tls"
)

// Constants

// Integer counter for IMAP states.
const (
	ANY IMAPState = iota
	NOT_AUTHENTICATED
	AUTHENTICATED
	MAILBOX
	LOGOUT
)

// Structs

// IMAPState represents the integer value associated
// with one of the implemented IMAP states a connection
// can be in.
type IMAPState int

// Structs

// Connection carries all information specific
// to one observed connection on its way through
// the IMAP server.
type Connection struct {
	Conn      net.Conn
	Worker    string
	Reader    *bufio.Reader
	UserToken string
	UserName  string
}

// Functions

// NewConnection creates a new element of above
// connection struct and fills it with content from
// a supplied, real IMAP connection.
func NewConnection(c net.Conn) *Connection {

	return &Connection{
		Conn:   c,
		Reader: bufio.NewReader(c),
	}
}

// ReliableConnect attempts to connect to defined
// remote node as longs as the error from previous
// attempts is possible to be dealt with.
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

// Receive wraps the main io.Reader function that
// awaits text until a newline symbol and deletes
// that symbol afterwards again. It returns the
// resulting string or an error.
func (c *Connection) Receive() (string, error) {

	text, err := c.Reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	return strings.TrimRight(text, "\n"), nil
}

// Send takes in an answer text from server as a
// string and writes it to the connection to the client.
// In case an error occurs, this method returns it to
// the calling function.
func (c *Connection) Send(text string) error {

	if _, err := fmt.Fprintf(c.Conn, "%s\n", text); err != nil {
		return err
	}

	return nil
}

// SignalSessionPrefixWorker is used by the distributor node to
// signal an involved worker node context about future requests.
func (c *Connection) SignalSessionPrefixWorker(worker *tls.Conn) error {

	if _, err := fmt.Fprintf(worker, "> id: %s <\n", c.UserToken); err != nil {
		return err
	}

	return nil
}

// SignalSessionPrefixStorage is used by a failover worker node
// to signal the storage node context about future requests.
func (c *Connection) SignalSessionPrefixStorage(storage *tls.Conn, clientID, workerName string) error {

	if _, err := fmt.Fprintf(storage, "> id: %s %s <\n", clientID, workerName); err != nil {
		return err
	}

	return nil
}

// SignalSessionError can be used by distributor or worker nodes
// to signal other nodes that an fatal error occurred during
// processing a request.
func (c *Connection) SignalSessionError(node *tls.Conn) error {

	var err error

	if node != nil {
		// Any node: send error signal to other node.
		_, err = fmt.Fprint(node, "> error <\n")
	} else {
		// Worker: send error signal to distributor.
		_, err = fmt.Fprint(c.Conn, "> error <\n")
	}

	if err != nil {
		return err
	}

	return nil
}

// SignalSessionDone is either used by the distributor to signal
// the worker that a client logged out or by any node to indicated
// that the current operation is done.
func (c *Connection) SignalSessionDone(node *tls.Conn) error {

	var err error

	if node != nil {
		// Any node: send done signal to other node.
		_, err = fmt.Fprint(node, "> done <\n")
	} else {
		// Worker: send done signal to distributor.
		_, err = fmt.Fprint(c.Conn, "> done <\n")
	}

	if err != nil {
		return err
	}

	return nil
}

// SignalAwaitingLiteral is used by workers to indicate
// a proxying distributor node that they are awaiting
// literal data from a client. The amount of awaited data
// is sent along this signal.
func (c *Connection) SignalAwaitingLiteral(awaiting int) error {

	var err error

	// Indicate how many bytes of literal data are awaited.
	_, err = fmt.Fprintf(c.Conn, "> literal: %d <\n", awaiting)
	if err != nil {
		return err
	}

	return nil
}

// Terminate tears down the state of a connection.
// This includes closing contained connection items.
// It returns nil or eventual errors.
func (c *Connection) Terminate() error {

	if err := c.Conn.Close(); err != nil {
		return err
	}

	return nil
}

// Error makes use of Terminate but provides an additional
// error message before terminating.
func (c *Connection) Error(msg string, err error) {

	// Log error.
	log.Printf("%s: %s. Terminating connection to %s\n", msg, err.Error(), c.Conn.RemoteAddr().String())

	// Terminate connection.
	err = c.Terminate()
	if err != nil {
		log.Fatal(err)
	}
}

// ErrorLogOnly is used by nodes to log and indicate fatal
// errors but without closing the permanent connection
// to other nodes, e.g. the distributor.
func (c *Connection) ErrorLogOnly(msg string, err error) {

	// Log error.
	log.Printf("%s: %s. Signalling error to distributor.\n", msg, err.Error())

	// Signal error to distributor node.
	err = c.SignalSessionError(nil)
	if err != nil {
		log.Fatal(err)
	}
}
