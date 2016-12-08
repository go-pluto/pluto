package imap_test

import (
	"log"
	"testing"

	"github.com/numbleroot/pluto/imap"
	"github.com/numbleroot/pluto/utils"
)

// Functions

// TestInitStorage executes a white-box unit test on the
// implemented InitStorage() function.
func TestInitStorage(t *testing.T) {

	var err error

	// Create needed test environment.
	config, _, err := utils.CreateTestEnv()
	if err != nil {
		log.Fatal(err)
	}

	// Correct storage initialization.
	storage, err := imap.InitStorage(config)
	if err != nil {
		t.Fatalf("[imap.TestInitStorage] Expected correct storage initialization but failed with: '%s'\n", err.Error())
	}

	// Close the socket.
	storage.Socket.Close()
}
