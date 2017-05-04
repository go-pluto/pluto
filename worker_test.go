package main

import (
	"log"
	"testing"
	"time"

	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/imap"
)

// Functions

// TestInitWorker executes a white-box unit test on the
// implemented InitWorker() function.
func TestInitWorker(t *testing.T) {

	// Read configuration from file.
	conf, err := config.LoadConfig("./test-config.toml")
	if err != nil {
		t.Fatalf("[imap.TestInitWorker] Expected loading of configuration file not to fail but: '%s'\n", err.Error())
	}

	workerConf := conf.Workers["worker-1"]

	// Set different ports for this test to
	// avoid conflicting binds.
	conf.Distributor.Port = "39933"
	workerConf.MailPort = "40001"
	workerConf.SyncPort = "50001"
	conf.Workers["worker-1"] = workerConf
	conf.Storage.MailPort = "41000"
	conf.Storage.SyncPort = "51000"

	go func() {

		// Correct storage initialization.
		storage, err := imap.InitStorage(conf)
		if err != nil {
			t.Fatalf("[imap.TestInitWorker] Expected correct storage initialization but failed with: '%s'\n", err.Error())
		}

		// Close the socket after 500ms.
		time.AfterFunc((600 * time.Millisecond), func() {
			log.Printf("[imap.TestInitWorker] Closing storage socket.\n")
			storage.MailSocket.Close()
			storage.SyncSocket.Close()
		})

		// Run the storage node.
		_ = storage.Run()
	}()

	time.Sleep(400 * time.Millisecond)

	// Correct worker initialization.
	worker, err := imap.InitWorker(conf, "worker-1")
	if err != nil {
		t.Fatalf("[imap.TestInitWorker] Expected correct worker-1 initialization but failed with: '%s'\n", err.Error())
	}

	worker.MailSocket.Close()
	worker.SyncSocket.Close()

	time.Sleep(800 * time.Millisecond)
}
