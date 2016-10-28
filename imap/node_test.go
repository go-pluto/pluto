package imap

import (
	"log"
	"testing"
	"time"

	"github.com/numbleroot/pluto/utils"
)

// Functions

// TestInitNode executes a white-box unit test on the
// implemented InitNode() function.
func TestInitNode(t *testing.T) {

	var err error

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

	go func() {

		// Correct storage initialization.
		_, errStorage := InitNode(Config, false, "", true)
		if errStorage != nil {
			t.Fatalf("[imap.TestInitNode] Expected correct storage initialization but failed with: '%s'\n", errStorage.Error())
		}
	}()

	// Wait shortly for node to spawn.
	time.Sleep(500 * time.Millisecond)

	go func() {

		// Correct worker initialization.
		_, errWorker := InitNode(Config, false, "worker-1", false)
		if errWorker != nil {
			t.Fatalf("[imap.TestInitNode] Expected correct worker-1 initialization but failed with: '%s'\n", errWorker.Error())
		}
	}()

	// Wait shortly for nodes to spawn.
	time.Sleep(500 * time.Millisecond)

	// Correct distributor initialization.
	_, errDistributor := InitNode(Config, true, "", false)
	if errDistributor != nil {
		t.Fatalf("[imap.TestInitNode] Expected correct distributor initialization but failed with: '%s'\n", errDistributor.Error())
	}
}
