package comm

import (
	"fmt"
	"log"

	"crypto/tls"
)

// Structs

// Sender bundles information needed for sending
// out sync messages via CRDTs.
type Sender struct {
	name   string
	inc    chan string
	vclock map[string]int
	nodes  map[string]*tls.Conn
}

// Functions

// InitSender initiliazies above struct and sets
// default values for most involved elements to start
// with. It returns a channel local processes can put
// CRDT changes into, so that those changes will be
// communicated to connected nodes.
func InitSender(name string, nodes map[string]*tls.Conn) chan string {

	// Make channel to communicate over with local
	// processes intending to send a message.
	// TODO: Make buffered?
	c := make(chan string)

	// Create and initialize what we need for
	// a CRDT sender routine.
	sender := &Sender{
		name:   name,
		inc:    c,
		vclock: make(map[string]int),
		nodes:  nodes,
	}

	// Connect to CRDT peers and initially set vclock
	// entries to 0.
	for i, node := range nodes {
		log.Printf("INIT: i: %s, node: %v\n", i, node)

		sender.vclock[i] = 0
	}

	// Including the entry of this node.
	sender.vclock[name] = 0

	// Start receiving routine in background.
	go sender.HandleMessages()

	// Return this channel to pass to processes.
	return c
}

// HandleMessages waits for messages on prior created
// channel, increments this node's vector clock entry
// and sends out a marshalled version of the whole message.
func (sender *Sender) HandleMessages() {

	for {

		// Wait for an incoming message to send.
		payload := <-sender.inc

		// Update this node's vector clock.
		sender.vclock[sender.name] += 1

		// Create a new message based on these values.
		msg := Message{
			vclock:  sender.vclock,
			payload: payload,
		}

		// Write message to TLS connections.
		for i, conn := range sender.nodes {
			log.Printf("SEND: i: %s, conn: %v\n", i, conn)

			if _, err := fmt.Fprintf(conn, "%s\n", msg.String()); err != nil {
				log.Fatal(err)
			}
		}
	}
}
