package main

import (
	"net"
	"os"
	"testing"

	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/imap"
	"github.com/numbleroot/pluto/server"
)

// Variables

var Config *config.Config
var Server *server.Server

// Structs

var sendReceiveTests = []struct {
	in  string
	out string
}{
	{"a001 login mrc secret", "a001 login mrc secret"},
	{"a001 login mrc secret", "a001 login mrc secret"},
	{"", ""},
}

// Functions

func TestSendReceive(t *testing.T) {

	// Connect to IMAP server.
	conn, err := net.Dial("tcp", (Config.IP + ":" + Config.Port))
	if err != nil {
		t.Fatalf("[TestSendReceive] Error during connection attempt to IMAP server: %s\n", err.Error())
	}

	// Create new connection struct.
	c := imap.NewConnection(conn)

	// Consume mandatory IMAP greeting.
	_ = c.Receive()

	for _, tt := range sendReceiveTests {

		// Table test: send 'in' part of each line.
		c.Send(tt.in)

		// Receive answer.
		answer := c.Receive()

		if answer != tt.out {
			t.Fatalf("[TestSendReceive] Expected '%s' but received '%s'\n", tt.out, answer)
		}
	}
}

func TestMain(m *testing.M) {

	// Read configuration from file.
	Config = config.LoadConfig("config.toml")

	// Initialize a server instance.
	Server = server.InitServer(Config.IP, Config.Port)

	// Start test server in background.
	go Server.RunServer(Config.Server.Greeting)

	// Start main tests.
	os.Exit(m.Run())
}
