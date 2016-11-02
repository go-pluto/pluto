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
func (node *Node) Capability(c *Connection, req *Request) bool {

	log.Println("CAPABILITY served")

	if len(req.Payload) > 0 {

		// If payload was not empty to CAPABILITY command,
		// this is a client error. Return BAD statement.
		err := c.Send(fmt.Sprintf("%s BAD Command CAPABILITY was sent with extra parameters", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return false
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
func (node *Node) Login(c *Connection, req *Request) bool {

	log.Println("LOGIN served")

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

		return false
	}

	id, err := node.AuthAdapter.AuthenticatePlain(userCredentials[0], userCredentials[1])
	if err != nil {

		// If supplied credentials failed to authenticate client,
		// they are invalid. Return NO statement.
		err := c.Send(fmt.Sprintf("%s NO Name and / or password wrong", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return false
	}

	// Signal success to client.
	err = c.Send(fmt.Sprintf("%s OK Logged in", req.Tag))
	if err != nil {
		c.Error("Encountered send error", err)
		return false
	}

	// Find worker node responsible for this connection
	// and put it into connection information struct.
	respWorker, err := node.AuthAdapter.GetWorkerForUser(node.Config.Workers, id)
	if err != nil {
		c.Error("Authentication error", err)
	}

	c.Worker = respWorker

	return true
}

// Logout correctly ends a connection with a client.
// Also necessarily created management structures will
// get shut down gracefully.
func (node *Node) Logout(c *Connection, req *Request) bool {

	log.Println("LOGOUT served")

	if len(req.Payload) > 0 {

		// If payload was not empty to LOGOUT command,
		// this is a client error. Return BAD statement.
		err := c.Send(fmt.Sprintf("%s BAD Command LOGOUT was sent with extra parameters", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return false
	}

	// Signal success to client.
	err := c.Send(fmt.Sprintf("* BYE Terminating connection\n%s OK LOGOUT completed", req.Tag))
	if err != nil {
		c.Error("Encountered send error", err)
		return false
	}

	// Terminate connection.
	c.Terminate()

	return true
}

// StartTLS states on IMAP STARTTLS command
// that current connection is already encrypted.
func (node *Node) StartTLS(c *Connection, req *Request) bool {

	log.Println("STARTTLS served")

	if len(req.Payload) > 0 {

		// If payload was not empty to STARTTLS command,
		// this is a client error. Return BAD statement.
		err := c.Send(fmt.Sprintf("%s BAD Command STARTTLS was sent with extra parameters", req.Tag))
		if err != nil {
			c.Error("Encountered send error", err)
			return false
		}

		return false
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
// node and the responsible worker node.
func (node *Node) Proxy(c *Connection, rawReq string) bool {

	log.Println("PROXYing request...")

	// We need proper auxiliary variables for later access.
	connWorker := node.Connections[c.Worker]
	readerWorker := bufio.NewReader(connWorker)

	// Send received client command to worker node.
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
	for curResp != "> done <" {

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

		// Send all buffered worker answers to client.
		err = c.Send(bufResp[i])
		if err != nil {
			c.Error("Encountered send error to client", err)
			return false
		}
	}

	return true
}

// AcceptDistributor acts as the main loop for requests
// targeted at IMAP functions implemented in distributor node.
// It parses incoming requests and executes command
// specific handlers matching the parsed data.
func (node *Node) AcceptDistributor(c *Connection) {

	// Connections in this state are only possible against
	// a node of type DISTRIBUTOR, none else.
	if node.Type != DISTRIBUTOR {
		log.Println("[imap.AcceptDistributor] WORKER or STORAGE node tried to run this function. Not allowed.")
		return
	}

	// As long as we do not receive a LOGOUT
	// command from client, we accept requests.
	recvUntil := ""

	for recvUntil != "LOGOUT" {

		// Receive next incoming client command.
		rawReq, err := c.Receive()
		if err != nil {
			c.Error("Encountered receive error", err)
			return
		}

		// Parse received next raw request into struct.
		req, err := ParseRequest(rawReq)
		if err != nil {

			// Signal error to client.
			err := c.Send(err.Error())
			if err != nil {
				c.Error("Encountered send error", err)
				return
			}

			// Go back to beginning of loop.
			continue
		}

		switch {

		case req.Command == "CAPABILITY":
			node.Capability(c, req)

		case req.Command == "LOGIN":
			node.Login(c, req)

		case req.Command == "LOGOUT":
			if ok := node.Logout(c, req); ok {

				// If a user logged in prior to logging out,
				// tell involved worker node that user logged out.
				if c.Worker != "" {

					if err := c.SignalDistributorDone(node.Connections[c.Worker]); err != nil {
						c.Error("Encountered send error while signaling distributor is done", err)
						return
					}
				}

				// A LOGOUT marks connection termination.
				c.Worker = ""
				recvUntil = "LOGOUT"
			}

		case req.Command == "STARTTLS":
			node.StartTLS(c, req)

		case c.Worker != "":
			node.Proxy(c, rawReq)

		default:
			// Client sent inappropriate command. Signal tagged error.
			err := c.Send(fmt.Sprintf("%s BAD Received invalid IMAP command", req.Tag))
			if err != nil {
				c.Error("Encountered send error", err)
				return
			}
		}
	}

	log.Println("User logged out")
}
