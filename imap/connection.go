package imap

import (
	"bufio"
	"fmt"
	stdlog "log"
	"strings"

	"crypto/tls"
	"path/filepath"

	"github.com/numbleroot/pluto/comm"
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
// a pluto node that only authenticates and proxies
// IMAP connections.
type Connection struct {
	IncConn       *tls.Conn
	IncReader     *bufio.Reader
	OutConn       *tls.Conn
	OutReader     *bufio.Reader
	IntlTLSConfig *tls.Config
	IntlConnRetry int
	OutAddr       string
	ClientID      string
	UserName      string
}

// IMAPConnection contains additional elements needed
// for performing the actual IMAP operations for an
// authenticated client.
type IMAPConnection struct {
	*Connection
	IMAPState       IMAPState
	UserCRDTPath    string
	UserMaildirPath string
	SelectedMailbox string
}

// Functions

// Send takes in an answer text from a node as a
// string and writes it to the connection to the client.
// In case an error occurs, this method returns it to
// the calling function.
func (c *Connection) Send(inc bool, text string) error {

	var err error

	// Check which attached connection should be used.
	conn := c.IncConn
	if inc != true {
		conn = c.OutConn
	}

	// Send message.
	_, err = fmt.Fprintf(conn, "%s\r\n", text)
	if err != nil {
		return err
	}

	return nil
}

// InternalSend is used by nodes of the pluto system to
// successfully transmit a message to another node or fail
// definitely if no reconnection is possible. This prevents
// further handler advancement in case a link failed.
func (c *Connection) InternalSend(inc bool, text string) error {

	// Check which attached connection should be used.
	conn := c.IncConn
	if inc != true {
		conn = c.OutConn
	}

	// Test if connection is still healthy.
	_, err := conn.Write([]byte("> ping <\r\n"))
	if err != nil {
		return fmt.Errorf("sending ping to node '%s' failed: %s", conn.RemoteAddr().String(), err.Error())
	}

	// Write message to TLS connections.
	_, err = fmt.Fprintf(conn, "%s\r\n", text)
	for err != nil {

		stdlog.Printf("[imap.InternalSend] Sending to node '%s' failed, trying to recover...\n", conn.RemoteAddr())

		// Define what IP and port of remote node look like.
		remoteAddr := conn.RemoteAddr().String()

		// Define an error we can deal with.
		okError := fmt.Sprintf("write tcp %s->%s: write: broken pipe", conn.LocalAddr().String(), remoteAddr)

		if err.Error() == okError {

			// Reestablish TLS connection to remote node.
			conn, err = comm.ReliableConnect(remoteAddr, c.IntlTLSConfig, c.IntlConnRetry)
			if err != nil {
				return fmt.Errorf("failed to reestablish connection with '%s': %s", remoteAddr, err.Error())
			}

			// Save context to connection.
			if inc {
				c.IncConn = conn
				c.IncReader = bufio.NewReader(conn)
			} else {
				c.OutConn = conn
				c.OutReader = bufio.NewReader(conn)

				// Inform remote node about which session was active.
				err = c.SignalSessionStart(false)
				if err != nil {
					return fmt.Errorf("signalling session to remote node failed with: %s", err.Error())
				}
			}

			stdlog.Printf("[imap.InternalSend] Reconnected to '%s'.\n", remoteAddr)

			// Resend message to remote node.
			_, err = fmt.Fprintf(conn, "%s\r\n", text)
		} else {
			return fmt.Errorf("failed to send message to remote node '%s': %s", remoteAddr, err.Error())
		}
	}

	return nil
}

// Receive wraps the main io.Reader function that awaits text
// until an IMAP newline symbol and deletes the symbols after-
// wards again. It returns the resulting string or an error.
func (c *Connection) Receive(inc bool) (string, error) {

	var err error

	// Check which attached connection should be used.
	conn := c.IncConn
	if inc != true {
		conn = c.OutConn
	}

	reader := c.IncReader
	if inc != true {
		reader = c.OutReader
	}

	// Initial value for received message in order
	// to skip past the mandatory ping message.
	text := "> ping <\r\n"

	for text == "> ping <\r\n" {

		text, err = reader.ReadString('\n')
		if err != nil {

			if err.Error() == "EOF" {
				stdlog.Printf("[imap.Receive] Node at '%s' disconnected.\n", conn.RemoteAddr().String())
			}

			break
		}
	}

	// If an error happened, return it.
	if err != nil {
		return "", err
	}

	return strings.TrimRight(text, "\r\n"), nil
}

// InternalReceive is used by nodes in the pluto system
// receive an incoming message and filter out all prior
// received ping message.
func (c *Connection) InternalReceive(inc bool) (string, error) {

	var err error

	// Check which attached connection should be used.
	reader := c.IncReader
	if inc != true {
		reader = c.OutReader
	}

	// Initial value for received message in order
	// to skip past the mandatory ping message.
	text := "> ping <\r\n"

	for text == "> ping <\r\n" {

		text, err = reader.ReadString('\n')
		if err != nil {
			break
		}
	}

	// If an error happened, return it.
	if err != nil {
		return "", err
	}

	return strings.TrimRight(text, "\r\n"), nil
}

// SignalSessionStart is used by the distributor node to signal
// an involved worker node context about future requests.
func (c *Connection) SignalSessionStart(inc bool) error {

	// Text to send.
	msg := fmt.Sprintf("> id: %s <", c.ClientID)

	// Send session information internally.
	err := c.InternalSend(inc, msg)
	if err != nil {
		return err
	}

	return nil
}

// SignalSessionStartFailover is used by a failover worker node
// to signal the storage node context about future requests.
func (c *Connection) SignalSessionStartFailover(inc bool, recvClientID string, origWorker string) error {

	// Text to send.
	msg := strings.Replace(recvClientID, " <", fmt.Sprintf(" %s <", origWorker), -1)

	// Send session information internally.
	err := c.InternalSend(inc, msg)
	if err != nil {
		return err
	}

	return nil
}

// SignalSessionDone is either used by the distributor to signal
// the worker that a client logged out or by any node to indicated
// that the current operation is done.
func (c *Connection) SignalSessionDone(inc bool) error {

	// Send done signal.
	err := c.InternalSend(inc, "> done <")
	if err != nil {
		return err
	}

	return nil
}

// SignalAwaitingLiteral is used by workers to indicate
// a proxying distributor node that they are awaiting
// literal data from a client. The amount of awaited data
// is sent along this signal.
func (c *Connection) SignalAwaitingLiteral(inc bool, awaiting int) error {

	// Text to send.
	msg := fmt.Sprintf("> literal: %d <", awaiting)

	// Indicate how many bytes of literal data are awaited.
	err := c.InternalSend(inc, msg)
	if err != nil {
		return err
	}

	return nil
}

// Terminate tears down the state of a connection.
// This includes closing contained connection items.
// It returns nil or eventual errors.
func (c *Connection) Terminate() error {

	if c.IncConn != nil {

		// Possibly close incoming connection.
		err := c.IncConn.Close()
		if err != nil {
			return err
		}
	}

	if c.OutConn != nil {

		// Possibly close outgoing connection.
		err := c.OutConn.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

// Error makes use of Terminate but provides an additional
// error message before terminating.
func (c *Connection) Error(msg string, err error) {

	// Log error.
	stdlog.Printf("%s: %s. Terminating connection to %s.\n", msg, err.Error(), c.IncConn.RemoteAddr().String())

	// Terminate connection.
	err = c.Terminate()
	if err != nil {
		stdlog.Fatal(err)
	}
}

// UpdateClientContext expects the initial client information
// string sent when the proxying node opened a connection to
// a worker node. It updates the existing connection with the
// contained information.
func (c *IMAPConnection) UpdateClientContext(clientIDRaw string, CRDTLayerRoot string, MaildirRoot string) (string, error) {

	// Split received clientID string at white spaces
	// and check for correct amount of found fields.
	fields := strings.Fields(clientIDRaw)
	if (len(fields) != 4) && (len(fields) != 5) {
		return "", fmt.Errorf("received an invalid clientID information")
	}

	if len(fields) == 4 {

		// Check if structure for worker node is correct.
		if fields[0] != ">" || fields[1] != "id:" || fields[3] != "<" {
			return "", fmt.Errorf("received an invalid clientID information")
		}

	} else if len(fields) == 5 {

		// Check if structure for storage node is correct.
		if fields[0] != ">" || fields[1] != "id:" || fields[4] != "<" {
			return "", fmt.Errorf("received an invalid clientID information")
		}

	}

	// Parse parts including user name from clientID.
	clientInfo := strings.SplitN(fields[2], ":", 3)

	// Update existing IMAP connection.
	c.ClientID = fields[2]
	c.UserName = clientInfo[2]
	c.UserCRDTPath = filepath.Join(CRDTLayerRoot, clientInfo[2])
	c.UserMaildirPath = filepath.Join(MaildirRoot, clientInfo[2])

	return fields[3], nil
}
