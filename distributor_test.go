package main

import (
	"log"
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
		t.Fatalf("[imap.TestInitDistributor] Expected loading of configuration file not to fail but: '%s'\n", err.Error())
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
			t.Fatalf("[imap.TestInitDistributor] Expected correct storage initialization but failed with: '%s'\n", err.Error())
		}

		// Close the socket after 500ms.
		time.AfterFunc((1000 * time.Millisecond), func() {
			log.Printf("[imap.TestInitDistributor] Closing storage socket.\n")
			storage.MailSocket.Close()
			storage.SyncSocket.Close()
		})

		// Run the storage node.
		_ = storage.Run()
	}()

	time.Sleep(400 * time.Millisecond)

	go func() {

		// Correct worker initialization.
		worker, err := imap.InitWorker(conf, "worker-1")
		if err != nil {
			t.Fatalf("[imap.TestInitDistributor] Expected correct worker-1 initialization but failed with: '%s'\n", err.Error())
		}

		// Close the socket after 500ms.
		time.AfterFunc((600 * time.Millisecond), func() {
			log.Printf("[imap.TestInitDistributor] Closing worker-1 socket.\n")
			worker.MailSocket.Close()
			worker.SyncSocket.Close()
		})

		// Run the worker.
		_ = worker.Run()
	}()

	time.Sleep(400 * time.Millisecond)

	authenticator, err := initAuthenticator(conf)
	if err != nil {
		log.Fatalf("[imap.TestInitDistributor] Failed to initialize authenticator: %v", err)
	}

	// Correct distributor initialization.
	distr, err := imap.InitDistributor(conf, authenticator)
	if err != nil {
		t.Fatalf("[imap.TestInitDistributor] Expected correct distributor initialization but failed with: '%s'\n", err.Error())
	}

	distr.Socket.Close()

	time.Sleep(800 * time.Millisecond)
}
