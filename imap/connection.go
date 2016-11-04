package imap

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"

	"crypto/tls"
)

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

// InjectContext takes in received raw context string,
// verifies and parses it and if successful, injects
// context information about client into connection struct.
func (c *Connection) InjectContext(contextRaw string) error {

	// Split received context at white spaces and check
	// for correct amount of found fields.
	contexts := strings.Fields(contextRaw)
	if len(contexts) != 6 {
		return fmt.Errorf("A received an invalid context information")
	}

	// Check if structure is correct.
	if contexts[0] != ">" || contexts[1] != "token:" || contexts[3] != "name:" || contexts[5] != "<" {
		return fmt.Errorf("B received an invalid context information")
	}

	// Extract token and name of client and store it
	// in connection context.
	c.UserToken = contexts[2]
	c.UserName = contexts[4]

	log.Printf("contexts[2]: %s, contexts[4]: %s\n", contexts[2], contexts[4])

	return nil
}

// SignalDistributorStart is used by the distributor node to signal
// an involved worker node context about future requests.
func (c *Connection) SignalDistributorStart(worker *tls.Conn) error {

	log.Println("DISTRIBUTOR: 'START' to WORKER")

	if _, err := fmt.Fprintf(worker, "> token: %s name: %s <\n", c.UserToken, c.UserName); err != nil {
		return err
	}

	return nil
}

// SignalDistributorDone is used by the distributor node
// to signal an involved worker node that the client logged out.
func (c *Connection) SignalDistributorDone(worker *tls.Conn) error {

	log.Println("DISTRIBUTOR: 'END'   to WORKER")

	if _, err := fmt.Fprint(worker, "> done <\n"); err != nil {
		return err
	}

	return nil
}

// SignalWorkerDone is used by workers to signal end
// of current operation to distributor node.
func (c *Connection) SignalWorkerDone() error {

	if _, err := fmt.Fprint(c.Conn, "> done <\n"); err != nil {
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
