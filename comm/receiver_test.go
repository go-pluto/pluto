package comm

import (
	"os"
	"sync"
	"testing"
	"time"

	"io/ioutil"
	"path/filepath"

	"github.com/go-kit/kit/log"
	"github.com/stretchr/testify/assert"
)

// TestTriggerMsgApplier executes a white-box unit
// test on implemented TriggerMsgApplier() function.
func TestTriggerMsgApplier(t *testing.T) {

	// Create logger connected to test struct.
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stdout))

	// Create temporary directory.
	dir, err := ioutil.TempDir("", "TestTriggerMsgApplier-")
	assert.Nilf(t, err, "failed to create temporary directory: %v", err)
	defer os.RemoveAll(dir)

	// Create temporary log file.
	tmpLogFile := filepath.Join(dir, "log")
	_, err = os.Create(tmpLogFile)
	assert.Nilf(t, err, "failed to create temporary log file: %v", err)

	// Create temporary vector clock file.
	tmpVClockFile := filepath.Join(dir, "vclock")
	_, err = os.Create(tmpVClockFile)
	assert.Nilf(t, err, "failed to create temporary vector clock file: %v", err)

	// Create channels for CRDT update application.
	applyCRDTUpdChan := make(chan Msg)
	doneCRDTUpdChan := make(chan struct{})

	// Define which nodes we want to simulate.
	nodes := []string{"other-node"}

	// Bundle information in Receiver struct.
	recv := &Receiver{
		lock:             &sync.Mutex{},
		logger:           logger,
		name:             "test-node",
		msgInLog:         make(chan struct{}, 1),
		socket:           nil,
		tlsConfig:        nil,
		incVClock:        make(chan string),
		updVClock:        make(chan map[string]uint32),
		vclock:           make(map[string]uint32),
		stopTrigger:      make(chan struct{}),
		applyCRDTUpdChan: applyCRDTUpdChan,
		doneCRDTUpdChan:  doneCRDTUpdChan,
		nodes:            nodes,
	}

	// Run trigger function.
	go func() {
		recv.TriggerMsgApplier(2)
	}()

	// Stop trigger function after specific number of seconds.
	go func() {
		<-time.After(7 * time.Second)
		recv.stopTrigger <- struct{}{}
		close(recv.msgInLog)
	}()

	numSignals := 0

	for _ = range recv.msgInLog {
		numSignals++
	}

	assert.Equalf(t, 3, numSignals, "expected to receive 3 triggers but actually received %d", numSignals)
}
