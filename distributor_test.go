package main

import (
	"testing"
	"time"

	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/imap"
)

// Functions

// TestInitDistributor executes a white-box unit test
// on the implemented InitDistributor() function.
func TestInitDistributor(t *testing.T) {

	// Read configuration from file.
	conf, err := config.LoadConfig("./test-config.toml")
	if err != nil {
		t.Fatalf("[imap.TestInitDistributor] Expected loading of configuration file not to fail but: %v", err)
	}

	// Set different port for this test to
	// avoid conflicting binds.
	conf.Distributor.Port = "39933"

	// Create an authenticator.
	authenticator, err := initAuthenticator(conf)
	if err != nil {
		t.Fatalf("[imap.TestInitDistributor] Failed to initialize authenticator: %v", err)
	}

	// Correct distributor initialization.
	distr, err := imap.InitDistributor(conf, authenticator)
	if err != nil {
		t.Fatalf("[imap.TestInitDistributor] Expected correct distributor initialization but failed with: %v", err)
	}

	distr.Socket.Close()

	time.Sleep(800 * time.Millisecond)
}
