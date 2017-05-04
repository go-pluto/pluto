package main

import (
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
		t.Fatalf("[imap.TestInitWorker] Expected loading of configuration file not to fail but: %v\n", err)
	}

	// Set different ports for this test to
	// avoid conflicting binds.
	workerConf := conf.Workers["worker-1"]
	workerConf.MailPort = "40001"
	workerConf.SyncPort = "50001"
	conf.Workers["worker-1"] = workerConf

	// Correct worker initialization.
	worker, err := imap.InitWorker(conf, "worker-1")
	if err != nil {
		t.Fatalf("[imap.TestInitWorker] Expected correct worker-1 initialization but failed with: %v\n", err)
	}

	worker.MailSocket.Close()
	worker.SyncSocket.Close()

	time.Sleep(800 * time.Millisecond)
}
