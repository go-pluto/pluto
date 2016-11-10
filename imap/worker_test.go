package imap_test

import (
	"log"
	"testing"
	"time"

	"github.com/numbleroot/pluto/imap"
	"github.com/numbleroot/pluto/utils"
)

// Functions

// TestInitWorker executes a white-box unit test on the
// implemented InitWorker() function.
func TestInitWorker(t *testing.T) {

	var err error

	// Create needed test environment.
	config, _, err := utils.CreateTestEnv()
	if err != nil {
		log.Fatal(err)
	}

	// Correct storage initialization.
	storage, err := imap.InitStorage(config)
	if err != nil {
		t.Fatalf("[imap.TestInitWorker] Expected correct storage initialization but failed with: '%s'\n", err.Error())
	}

	go func() {

		// Close the socket after 500ms.
		time.AfterFunc((500 * time.Millisecond), func() {
			storage.Socket.Close()
		})

		// Run the storage node.
		_ = storage.Run()
	}()

	// Correct worker initialization.
	worker, err := imap.InitWorker(config, "worker-1")
	if err != nil {
		t.Fatalf("[imap.TestInitWorker] Expected correct worker-1 initialization but failed with: '%s'\n", err.Error())
	}

	worker.Socket.Close()

	time.Sleep(1 * time.Second)
}
