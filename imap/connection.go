package imap

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"

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
	IMAPState IMAPState
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

// SignalSessionPrefix is used by the distributor node to signal
// an involved worker node context about future requests.
func (c *Connection) SignalSessionPrefix(worker *tls.Conn) error {

	if _, err := fmt.Fprintf(worker, "> token: %s name: %s <\n", c.UserToken, c.UserName); err != nil {
		return err
	}

	return nil
}

// SignalSessionChanged indicates to worker node that a session
// experienced a major change and therefore allows workers to take
// corresponding actions such as closing IMAP state.
func (c *Connection) SignalSessionChanged(worker *tls.Conn) error {

	if _, err := fmt.Fprintf(worker, "> changed <\n"); err != nil {
		return err
	}

	return nil
}

// SignalSessionError can be used by distributor or worker nodes
// to signal the other side that an fatal error occurred during
// processing a request.
func (c *Connection) SignalSessionError(worker *tls.Conn) error {

	var err error

	if worker != nil {
		// Distributor: send error signal to worker.
		_, err = fmt.Fprint(worker, "> error <\n")
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
// the worker that a client logged out or by a worker to indicated
// that the current operation is done.
func (c *Connection) SignalSessionDone(worker *tls.Conn) error {

	var err error

	if worker != nil {
		// Distributor: send done signal to worker.
		_, err = fmt.Fprint(worker, "> done <\n")
	} else {
		// Worker: send done signal to distributor.
		_, err = fmt.Fprint(c.Conn, "> done <\n")
	}

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

// ErrorLogOnly is used by worker nodes to log and indicate
// fatal errors but without closing the permanent connection
// to the distributor node.
func (c *Connection) ErrorLogOnly(msg string, err error) {

	// Log error.
	log.Printf("%s: %s. Signalling error to DISTRIBUTOR.\n", msg, err.Error())

	// Signal error to distributor node.
	err = c.SignalSessionError(nil)
	if err != nil {
		log.Fatal(err)
	}
}
