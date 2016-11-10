package imap

import (
	"bufio"
	"fmt"
	"log"
	"strings"
)

// Functions

// Capability handles the IMAP CAPABILITY command.
// It outputs the supported actions in the current state.
func (distr *Distributor) Capability(c *Connection, req *Request) bool {

	log.Println()
	log.Printf("Serving CAPABILITY '%s'...\n", req.Tag)

	if len(req.Payload) > 0 {

		// If payload was not empty to CAPABILITY command,
		// this is a client error. Return BAD statement.
		err := c.Send(fmt.Sprintf("%s BAD Command CAPABILITY was sent with extra parameters", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Send mandatory capability options.
	// This means, AUTH=PLAIN is allowed and nothing else.
	// STARTTLS will be answered but is not listed as
	// each connection already is a TLS connection.
	err := c.Send(fmt.Sprintf("* CAPABILITY IMAP4rev1 AUTH=PLAIN\n%s OK CAPABILITY completed", req.Tag))
	if err != nil {
		c.Error("Encountered send error", err)
		return false
	}

	return true
}

// Login performs the authentication mechanism specified
// as part of the distributor config.
func (distr *Distributor) Login(c *Connection, req *Request) bool {

	log.Println()
	log.Printf("Serving LOGIN '%s'...\n", req.Tag)

	// Split payload on every space character.
	userCredentials := strings.Split(req.Payload, " ")

	if len(userCredentials) != 2 {

		// If payload did not contain exactly two elements,
		// this is a client error. Return BAD statement.
		err := c.Send(fmt.Sprintf("%s BAD Command LOGIN was not sent with exactly two parameters", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	id, clientID, err := distr.AuthAdapter.AuthenticatePlain(userCredentials[0], userCredentials[1], c.Conn.RemoteAddr().String())
	if err != nil {

		// If supplied credentials failed to authenticate client,
		// they are invalid. Return NO statement.
		err := c.Send(fmt.Sprintf("%s NO Name and / or password wrong", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// Signal success to client.
	err = c.Send(fmt.Sprintf("%s OK Logged in", req.Tag))
	if err != nil {
		c.Error("Encountered send error", err)
		return false
	}

	// Find worker node responsible for this connection.
	respWorker, err := distr.AuthAdapter.GetWorkerForUser(distr.Config.Workers, id)
	if err != nil {
		c.Error("Authentication error", err)
		return false
	}

	// If the client authenticated a second time during
	// a connection and user names differ, send changed
	// notice to worker and exchange user names.
	if (c.Worker != "") && (userCredentials[0] != c.UserName) {

		if err := c.SignalSessionPrefix(distr.Connections[c.Worker]); err != nil {
			c.Error("Encountered send error when distributor signalled context to worker", err)
			return false
		}

		err = c.SignalSessionChanged(distr.Connections[c.Worker])
		if err != nil {
			c.Error("Encountered send error when distributor signalled changed session to worker", err)
			return false
		}
	}

	// Save context to connection.
	c.Worker = respWorker
	c.UserToken = clientID
	c.UserName = userCredentials[0]

	return true
}

// Logout correctly ends a connection with a client.
// Also necessarily created management structures will
// get shut down gracefully.
func (distr *Distributor) Logout(c *Connection, req *Request) bool {

	log.Println()
	log.Printf("Serving LOGOUT '%s'...\n", req.Tag)

	if len(req.Payload) > 0 {

		// If payload was not empty to LOGOUT command,
		// this is a client error. Return BAD statement.
		err := c.Send(fmt.Sprintf("%s BAD Command LOGOUT was sent with extra parameters", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// If already a worker was assigned, signal logout.
	if c.Worker != "" {

		// Inform worker node about which session will log out.
		if err := c.SignalSessionPrefix(distr.Connections[c.Worker]); err != nil {
			c.Error("Encountered send error when distributor signalled context to worker", err)
			return false
		}

		// Signal to worker node that session is done.
		if err := c.SignalSessionDone(distr.Connections[c.Worker]); err != nil {
			c.Error("Encountered send error when distributor signalled end to worker", err)
			return false
		}
	}

	// Signal success to client.
	err := c.Send(fmt.Sprintf("* BYE Terminating connection\n%s OK LOGOUT completed", req.Tag))
	if err != nil {
		c.Error("Encountered send error", err)
		return false
	}

	// Delete context information from connection struct.
	c.Worker = ""
	c.UserToken = ""
	c.UserName = ""

	// Terminate connection.
	c.Terminate()

	return true
}

// StartTLS states on IMAP STARTTLS command
// that current connection is already encrypted.
func (distr *Distributor) StartTLS(c *Connection, req *Request) bool {

	log.Println()
	log.Printf("Serving STARTTLS '%s'...\n", req.Tag)

	if len(req.Payload) > 0 {

		// If payload was not empty to STARTTLS command,
		// this is a client error. Return BAD statement.
		err := c.Send(fmt.Sprintf("%s BAD Command STARTTLS was sent with extra parameters", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return true
	}

	// As the connection is already TLS encrypted,
	// tell client that a TLS session is active.
	err := c.Send(fmt.Sprintf("%s BAD TLS is already active", req.Tag))
	if err != nil {
		c.Error("Encountered send error", err)
		return false
	}

	return true
}

// Proxy forwards one request between the distributor
// node and the responsible worker distr.
func (distr *Distributor) Proxy(c *Connection, rawReq string) bool {

	log.Println()
	log.Printf("PROXYing request '%s'...\n", rawReq)

	// We need proper auxiliary variables for later access.
	connWorker := distr.Connections[c.Worker]
	readerWorker := bufio.NewReader(connWorker)

	// Inform worker node about context of request of this client.
	if err := c.SignalSessionPrefix(distr.Connections[c.Worker]); err != nil {
		c.Error("Encountered send error when distributor signalled context to worker", err)
		return false
	}

	// Send received client command to worker distr.
	if _, err := fmt.Fprintf(connWorker, "%s\n", rawReq); err != nil {
		c.Error("Encountered send error to worker", err)
		return false
	}

	// Reserve space for answer buffer.
	bufResp := make([]string, 0, 2)

	// Receive incoming worker response.
	curResp, err := readerWorker.ReadString('\n')
	if err != nil {
		c.Error("Encountered receive error from worker", err)
		return false
	}
	curResp = strings.TrimRight(curResp, "\n")

	// As long as the responsible worker has not
	// indicated the end of the current operation,
	// continue to buffer answers.
	for (curResp != "> done <") && (curResp != "> error <") {

		// Append it to answer buffer.
		bufResp = append(bufResp, curResp)

		// Receive incoming worker response.
		curResp, err = readerWorker.ReadString('\n')
		if err != nil {
			c.Error("Encountered receive error from worker", err)
			return false
		}
		curResp = strings.TrimRight(curResp, "\n")
	}

	for i := range bufResp {

		log.Printf("Sending back: %s\n", bufResp[i])

		// Send all buffered worker answers to client.
		err = c.Send(bufResp[i])
		if err != nil {
			c.Error("Encountered send error to client", err)
			return false
		}
	}

	// If the involved worker node indicated that an error
	// occurred, terminate connection to client.
	if curResp == "> error <" {
		err = c.Terminate()
		if err != nil {
			log.Fatal(err)
		}
	}

	return true
}
