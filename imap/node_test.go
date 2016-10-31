package imap

import (
	"log"
	"testing"

	"github.com/numbleroot/pluto/utils"
)

// Functions

// TestInitNode executes a white-box unit test on the
// implemented InitNode() function.
func TestInitNode(t *testing.T) {

	var err error

	// Synchronization channel.
	ready := make(chan bool)

	// Create needed test environment.
	Config, _, err := utils.CreateTestEnv()
	if err != nil {
		log.Fatal(err)
	}

	// All types indicated.
	_, err = InitNode(Config, true, "worker-1", true)
	if err.Error() != "[imap.InitNode] One node can not be of multiple types, please provide exclusively '-distributor' or '-worker WORKER-ID' or '-storage'.\n" {
		t.Fatalf("[imap.TestInitNode] Expected '%s' but received '%s'\n", "[imap.InitNode] One node can not be of multiple types, please provide exclusively '-distributor' or '-worker WORKER-ID' or '-storage'.\n", err.Error())
	}

	// Two types indicated.
	_, err = InitNode(Config, true, "abc", false)
	if err.Error() != "[imap.InitNode] One node can not be of multiple types, please provide exclusively '-distributor' or '-worker WORKER-ID' or '-storage'.\n" {
		t.Fatalf("[imap.TestInitNode] Expected '%s' but received '%s'\n", "[imap.InitNode] One node can not be of multiple types, please provide exclusively '-distributor' or '-worker WORKER-ID' or '-storage'.\n", err.Error())
	}

	// Two types indicated.
	_, err = InitNode(Config, true, "", true)
	if err.Error() != "[imap.InitNode] One node can not be of multiple types, please provide exclusively '-distributor' or '-worker WORKER-ID' or '-storage'.\n" {
		t.Fatalf("[imap.TestInitNode] Expected '%s' but received '%s'\n", "[imap.InitNode] One node can not be of multiple types, please provide exclusively '-distributor' or '-worker WORKER-ID' or '-storage'.\n", err.Error())
	}

	// Two types indicated.
	_, err = InitNode(Config, false, "worker-1", true)
	if err.Error() != "[imap.InitNode] One node can not be of multiple types, please provide exclusively '-distributor' or '-worker WORKER-ID' or '-storage'.\n" {
		t.Fatalf("[imap.TestInitNode] Expected '%s' but received '%s'\n", "[imap.InitNode] One node can not be of multiple types, please provide exclusively '-distributor' or '-worker WORKER-ID' or '-storage'.\n", err.Error())
	}

	// No type indicated.
	_, err = InitNode(Config, false, "", false)
	if err.Error() != "[imap.InitNode] Node must be of one type, either '-distributor' or '-worker WORKER-ID' or '-storage'.\n" {
		t.Fatalf("[imap.TestInitNode] Expected '%s' but received '%s'\n", "[imap.InitNode] Node must be of one type, either '-distributor' or '-worker WORKER-ID' or '-storage'.\n", err.Error())
	}

	// Non-existent worker ID.
	_, err = InitNode(Config, false, "abc", false)
	if (err.Error() != "[imap.InitNode] Specified worker ID does not exist in config file. Please provide a valid one, for example 'worker-1'.\n") && (err.Error() != "[imap.InitNode] Specified worker ID does not exist in config file. Please provide a valid one, for example 'worker-2'.\n") && (err.Error() != "[imap.InitNode] Specified worker ID does not exist in config file. Please provide a valid one, for example 'worker-3'.\n") {
		t.Fatalf("[imap.TestInitNode] Expected '%s' but received '%s'\n", "[imap.InitNode] Specified worker ID does not exist in config file. Please provide a valid one, for example 'worker-1'.\n", err.Error())
	}

	go func(ready chan bool) {

		// Correct storage initialization.
		nodeStorage, errStorage := InitNode(Config, false, "", true)
		if errStorage != nil {
			t.Fatalf("[imap.TestInitNode] Expected correct storage initialization but failed with: '%s'\n", errStorage.Error())
		}

		// Signal readiness to next goroutine.
		ready <- true

		// Run the storage node.
		errStorage = nodeStorage.RunNode()
		if errStorage != nil {
			t.Fatalf("[imap.TestInitNode] Expected correct storage run but failed with: '%s'\n", errStorage.Error())
		}

		// Close the socket.
		nodeStorage.Socket.Close()
	}(ready)

	// Wait for storage node to have started.
	<-ready

	go func(ready chan bool) {

		// Correct worker initialization.
		nodeWorker, errWorker := InitNode(Config, false, "worker-1", false)
		if errWorker != nil {
			t.Fatalf("[imap.TestInitNode] Expected correct worker-1 initialization but failed with: '%s'\n", errWorker.Error())
		}

		// Signal readiness to next goroutine.
		ready <- true

		// Run the worker.
		errWorker = nodeWorker.RunNode()
		if errWorker != nil {
			t.Fatalf("[imap.TestInitNode] Expected correct worker-1 run but failed with: '%s'\n", errWorker.Error())
		}

		// Close the socket.
		nodeWorker.Socket.Close()
	}(ready)

	// Wait for worker nodes to have started.
	<-ready

	// Correct distributor initialization.
	nodeDistributor, errDistributor := InitNode(Config, true, "", false)
	if errDistributor != nil {
		t.Fatalf("[imap.TestInitNode] Expected correct distributor initialization but failed with: '%s'\n", errDistributor.Error())
	}

	nodeDistributor.Socket.Close()
}
