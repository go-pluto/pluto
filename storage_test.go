package main

import (
	"testing"
	"time"

	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/imap"
)

// Functions

// TestInitStorage executes a white-box unit test on the
// implemented InitStorage() function.
func TestInitStorage(t *testing.T) {

	// Read configuration from file.
	conf, err := config.LoadConfig("./test-config.toml")
	if err != nil {
		t.Fatalf("[imap.TestInitStorage] Expected loading of configuration file not to fail but: %v\n", err)
	}

	// Set different ports for this test to
	// avoid conflicting binds.
	conf.Storage.MailPort = "41000"
	conf.Storage.SyncPort = "51000"

	// Correct storage initialization.
	storage, err := imap.InitStorage(conf)
	if err != nil {
		t.Fatalf("[imap.TestInitStorage] Expected correct storage initialization but failed with: %v\n", err)
	}

	// Close the sockets.
	storage.MailSocket.Close()
	storage.SyncSocket.Close()

	time.Sleep(800 * time.Millisecond)
}
