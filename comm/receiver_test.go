package comm

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"sync"
	"testing"
	"time"

	"io/ioutil"
	"path/filepath"

	"github.com/go-kit/kit/log"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

// Variables

var (
	confStatus = uint32(0)

	writeInc1 = []byte("hello")
	writeInc2 = []byte("\nwhat\r\tabout!\"§$%&/()=strange#+?`?`?°°°characters")
	writeInc3 = []byte("∰☕✔😉")
	writeInc4 = []byte("1234567890")
	writeInc5 = []byte(fmt.Sprintf("%g", math.MaxFloat64))

	checkInc1 = []byte("5;hello")
	checkInc2 = []byte("53;\nwhat\r\tabout!\"§$%&/()=strange#+?`?`?°°°characters")
	checkInc3 = []byte("13;∰☕✔😉")
	checkInc4 = []byte("10;1234567890")
	checkInc5 = []byte(fmt.Sprintf("23;%g", math.MaxFloat64))

	writeApply1 = []byte{0x31, 0x31, 0x38, 0x3b, 0x0a, 0x08, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x2d, 0x31, 0x12, 0x0b, 0x0a, 0x07, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x10, 0x00, 0x12, 0x0c, 0x0a, 0x08, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x2d, 0x31, 0x10, 0x01, 0x1a, 0x06, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x22, 0x47, 0x0a, 0x05, 0x75, 0x73, 0x65, 0x72, 0x31, 0x12, 0x0a, 0x75, 0x6e, 0x69, 0x76, 0x65, 0x72, 0x73, 0x69, 0x74, 0x79, 0x1a, 0x32, 0x0a, 0x0a, 0x75, 0x6e, 0x69, 0x76, 0x65, 0x72, 0x73, 0x69, 0x74, 0x79, 0x12, 0x24, 0x61, 0x61, 0x35, 0x39, 0x35, 0x38, 0x35, 0x66, 0x2d, 0x35, 0x61, 0x35, 0x66, 0x2d, 0x34, 0x65, 0x61, 0x39, 0x2d, 0x38, 0x38, 0x37, 0x63, 0x2d, 0x37, 0x34, 0x61, 0x62, 0x32, 0x65, 0x33, 0x66, 0x31, 0x66, 0x34, 0x61}
	writeApply2 = []byte{0x31, 0x34, 0x32, 0x3b, 0x0a, 0x08, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x2d, 0x31, 0x12, 0x0b, 0x0a, 0x07, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x10, 0x00, 0x12, 0x0c, 0x0a, 0x08, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x2d, 0x31, 0x10, 0x01, 0x1a, 0x06, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x22, 0x5f, 0x0a, 0x05, 0x75, 0x73, 0x65, 0x72, 0x32, 0x12, 0x16, 0x4c, 0x6f, 0x6e, 0x67, 0x41, 0x6e, 0x64, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x65, 0x73, 0x74, 0x69, 0x6e, 0x67, 0x4e, 0x61, 0x6d, 0x65, 0x1a, 0x3e, 0x0a, 0x16, 0x4c, 0x6f, 0x6e, 0x67, 0x41, 0x6e, 0x64, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x65, 0x73, 0x74, 0x69, 0x6e, 0x67, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x24, 0x35, 0x32, 0x35, 0x61, 0x33, 0x66, 0x34, 0x30, 0x2d, 0x37, 0x63, 0x32, 0x63, 0x2d, 0x34, 0x62, 0x39, 0x61, 0x2d, 0x39, 0x34, 0x63, 0x38, 0x2d, 0x61, 0x33, 0x34, 0x33, 0x32, 0x66, 0x32, 0x35, 0x61, 0x32, 0x38, 0x61, 0x31, 0x34, 0x32, 0x3b, 0x0a, 0x08, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x2d, 0x31, 0x12, 0x0b, 0x0a, 0x07, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x10, 0x00, 0x12, 0x0c, 0x0a, 0x08, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x2d, 0x31, 0x10, 0x02, 0x1a, 0x06, 0x64, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x2a, 0x5f, 0x0a, 0x05, 0x75, 0x73, 0x65, 0x72, 0x32, 0x12, 0x16, 0x4c, 0x6f, 0x6e, 0x67, 0x41, 0x6e, 0x64, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x65, 0x73, 0x74, 0x69, 0x6e, 0x67, 0x4e, 0x61, 0x6d, 0x65, 0x1a, 0x3e, 0x0a, 0x16, 0x4c, 0x6f, 0x6e, 0x67, 0x41, 0x6e, 0x64, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x65, 0x73, 0x74, 0x69, 0x6e, 0x67, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x24, 0x35, 0x32, 0x35, 0x61, 0x33, 0x66, 0x34, 0x30, 0x2d, 0x37, 0x63, 0x32, 0x63, 0x2d, 0x34, 0x62, 0x39, 0x61, 0x2d, 0x39, 0x34, 0x63, 0x38, 0x2d, 0x61, 0x33, 0x34, 0x33, 0x32, 0x66, 0x32, 0x35, 0x61, 0x32, 0x38, 0x61, 0x31, 0x30, 0x36, 0x3b, 0x0a, 0x08, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x2d, 0x31, 0x12, 0x0b, 0x0a, 0x07, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x10, 0x00, 0x12, 0x0c, 0x0a, 0x08, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x2d, 0x31, 0x10, 0x03, 0x1a, 0x06, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x22, 0x3b, 0x0a, 0x05, 0x75, 0x73, 0x65, 0x72, 0x32, 0x12, 0x04, 0x74, 0x65, 0x73, 0x74, 0x1a, 0x2c, 0x0a, 0x04, 0x74, 0x65, 0x73, 0x74, 0x12, 0x24, 0x61, 0x64, 0x38, 0x38, 0x37, 0x37, 0x62, 0x30, 0x2d, 0x64, 0x33, 0x38, 0x34, 0x2d, 0x34, 0x66, 0x33, 0x38, 0x2d, 0x38, 0x32, 0x61, 0x63, 0x2d, 0x61, 0x30, 0x32, 0x66, 0x64, 0x37, 0x38, 0x66, 0x61, 0x39, 0x33, 0x61}

	checkApply2a = []byte{0x31, 0x34, 0x32, 0x3b, 0x0a, 0x08, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x2d, 0x31, 0x12, 0x0b, 0x0a, 0x07, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x10, 0x00, 0x12, 0x0c, 0x0a, 0x08, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x2d, 0x31, 0x10, 0x02, 0x1a, 0x06, 0x64, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x2a, 0x5f, 0x0a, 0x05, 0x75, 0x73, 0x65, 0x72, 0x32, 0x12, 0x16, 0x4c, 0x6f, 0x6e, 0x67, 0x41, 0x6e, 0x64, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x65, 0x73, 0x74, 0x69, 0x6e, 0x67, 0x4e, 0x61, 0x6d, 0x65, 0x1a, 0x3e, 0x0a, 0x16, 0x4c, 0x6f, 0x6e, 0x67, 0x41, 0x6e, 0x64, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x65, 0x73, 0x74, 0x69, 0x6e, 0x67, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x24, 0x35, 0x32, 0x35, 0x61, 0x33, 0x66, 0x34, 0x30, 0x2d, 0x37, 0x63, 0x32, 0x63, 0x2d, 0x34, 0x62, 0x39, 0x61, 0x2d, 0x39, 0x34, 0x63, 0x38, 0x2d, 0x61, 0x33, 0x34, 0x33, 0x32, 0x66, 0x32, 0x35, 0x61, 0x32, 0x38, 0x61, 0x31, 0x30, 0x36, 0x3b, 0x0a, 0x08, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x2d, 0x31, 0x12, 0x0b, 0x0a, 0x07, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x10, 0x00, 0x12, 0x0c, 0x0a, 0x08, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x2d, 0x31, 0x10, 0x03, 0x1a, 0x06, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x22, 0x3b, 0x0a, 0x05, 0x75, 0x73, 0x65, 0x72, 0x32, 0x12, 0x04, 0x74, 0x65, 0x73, 0x74, 0x1a, 0x2c, 0x0a, 0x04, 0x74, 0x65, 0x73, 0x74, 0x12, 0x24, 0x61, 0x64, 0x38, 0x38, 0x37, 0x37, 0x62, 0x30, 0x2d, 0x64, 0x33, 0x38, 0x34, 0x2d, 0x34, 0x66, 0x33, 0x38, 0x2d, 0x38, 0x32, 0x61, 0x63, 0x2d, 0x61, 0x30, 0x32, 0x66, 0x64, 0x37, 0x38, 0x66, 0x61, 0x39, 0x33, 0x61}
	checkApply2b = []byte{0x31, 0x30, 0x36, 0x3b, 0x0a, 0x08, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x2d, 0x31, 0x12, 0x0b, 0x0a, 0x07, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x10, 0x00, 0x12, 0x0c, 0x0a, 0x08, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x2d, 0x31, 0x10, 0x03, 0x1a, 0x06, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x22, 0x3b, 0x0a, 0x05, 0x75, 0x73, 0x65, 0x72, 0x32, 0x12, 0x04, 0x74, 0x65, 0x73, 0x74, 0x1a, 0x2c, 0x0a, 0x04, 0x74, 0x65, 0x73, 0x74, 0x12, 0x24, 0x61, 0x64, 0x38, 0x38, 0x37, 0x37, 0x62, 0x30, 0x2d, 0x64, 0x33, 0x38, 0x34, 0x2d, 0x34, 0x66, 0x33, 0x38, 0x2d, 0x38, 0x32, 0x61, 0x63, 0x2d, 0x61, 0x30, 0x32, 0x66, 0x64, 0x37, 0x38, 0x66, 0x61, 0x39, 0x33, 0x61}
)

// Functions

// TestTriggerMsgApplier executes a white-box unit
// test on implemented TriggerMsgApplier() function.
func TestTriggerMsgApplier(t *testing.T) {

	// Create logger.
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stdout))

	// Bundle information in Receiver struct.
	recv := &Receiver{
		lock:        &sync.Mutex{},
		logger:      logger,
		name:        "worker-1",
		msgInLog:    make(chan struct{}, 1),
		stopTrigger: make(chan struct{}),
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

	for range recv.msgInLog {
		numSignals++
	}

	assert.Equalf(t, 3, numSignals, "expected to receive 3 triggers but actually received %d", numSignals)
}

// TestIncoming executes a white-box unit
// test on implemented Incoming() function.
func TestIncoming(t *testing.T) {

	// Create logger.
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stdout))

	// Create temporary directory.
	dir, err := ioutil.TempDir("", "TestIncoming-")
	assert.Nilf(t, err, "failed to create temporary directory: %v", err)
	defer os.RemoveAll(dir)

	// Create path to temporary log file.
	tmpLogFile := filepath.Join(dir, "log")

	// Open log file for writing.
	write, err := os.OpenFile(tmpLogFile, (os.O_CREATE | os.O_WRONLY | os.O_APPEND), 0600)
	assert.Nilf(t, err, "failed to open temporary log file for writing: %v", err)

	// Open log file for updating.
	upd, err := os.OpenFile(tmpLogFile, os.O_RDWR, 0600)
	assert.Nilf(t, err, "failed to open temporary log file for updating: %v", err)

	// Bundle information in Receiver struct.
	recv := &Receiver{
		lock:     &sync.Mutex{},
		logger:   logger,
		name:     "worker-1",
		msgInLog: make(chan struct{}, 1),
		writeLog: write,
		updLog:   upd,
	}

	// Reset position in update file to beginning.
	_, err = recv.updLog.Seek(0, os.SEEK_SET)
	assert.Nilf(t, err, "expected resetting of position in update log not to fail but received: %v", err)

	// Value 1.
	// Write first value to log file.
	conf, err := recv.Incoming(context.Background(), &BinMsg{
		Data: writeInc1,
	})
	assert.Nilf(t, err, "expected nil error for Incoming() but received: %v", err)

	// Wait for signal that new message was written to log.
	<-recv.msgInLog

	// Validate received confirmation struct.
	assert.Equalf(t, confStatus, conf.Status, "expected conf to carry Status=0 but found: %v", conf.Status)

	// Read content of log file for inspection.
	content, err := ioutil.ReadFile(tmpLogFile)
	assert.Nilf(t, err, "expected nil error for ReadFile() but received: %v", err)
	assert.Equalf(t, checkInc1, content, "expected '%s' in log file but found: %v", checkInc1, content)

	// Value 2.
	// Write second value to file.
	conf, err = recv.Incoming(context.Background(), &BinMsg{
		Data: writeInc2,
	})
	assert.Nilf(t, err, "expected nil error for Incoming() but received: %v", err)

	<-recv.msgInLog

	assert.Equalf(t, confStatus, conf.Status, "expected conf to carry Status=0 but found: %v", conf.Status)

	content, err = ioutil.ReadFile(tmpLogFile)
	assert.Nilf(t, err, "expected nil error for ReadFile() but received: %v", err)

	content = bytes.TrimPrefix(content, checkInc1)
	assert.Equalf(t, checkInc2, content, "expected '%s' in log file but found: %v", checkInc2, content)

	// Value 3.
	// Write third value to file.
	conf, err = recv.Incoming(context.Background(), &BinMsg{
		Data: writeInc3,
	})
	assert.Nilf(t, err, "expected nil error for Incoming() but received: %v", err)

	<-recv.msgInLog

	assert.Equalf(t, confStatus, conf.Status, "expected conf to carry Status=0 but found: %v", conf.Status)

	content, err = ioutil.ReadFile(tmpLogFile)
	assert.Nilf(t, err, "expected nil error for ReadFile() but received: %v", err)

	content = bytes.TrimPrefix(content, checkInc1)
	content = bytes.TrimPrefix(content, checkInc2)
	assert.Equalf(t, checkInc3, content, "expected '%s' in log file but found: %v", checkInc3, content)

	// Value 4.
	// Write fourth value to file.
	conf, err = recv.Incoming(context.Background(), &BinMsg{
		Data: writeInc4,
	})
	assert.Nilf(t, err, "expected nil error for Incoming() but received: %v", err)

	<-recv.msgInLog

	assert.Equalf(t, confStatus, conf.Status, "expected conf to carry Status=0 but found: %v", conf.Status)

	content, err = ioutil.ReadFile(tmpLogFile)
	assert.Nilf(t, err, "expected nil error for ReadFile() but received: %v", err)

	content = bytes.TrimPrefix(content, checkInc1)
	content = bytes.TrimPrefix(content, checkInc2)
	content = bytes.TrimPrefix(content, checkInc3)
	assert.Equalf(t, checkInc4, content, "expected '%s' in log file but found: %v", checkInc4, content)

	// Value 5.
	// Write fifth value to file.
	conf, err = recv.Incoming(context.Background(), &BinMsg{
		Data: writeInc5,
	})
	assert.Nilf(t, err, "expected nil error for Incoming() but received: %v", err)

	<-recv.msgInLog

	assert.Equalf(t, confStatus, conf.Status, "expected conf to carry Status=0 but found: %v", conf.Status)

	content, err = ioutil.ReadFile(tmpLogFile)
	assert.Nilf(t, err, "expected nil error for ReadFile() but received: %v", err)

	content = bytes.TrimPrefix(content, checkInc1)
	content = bytes.TrimPrefix(content, checkInc2)
	content = bytes.TrimPrefix(content, checkInc3)
	content = bytes.TrimPrefix(content, checkInc4)
	assert.Equalf(t, checkInc5, content, "expected '%s' in log file but found: %v", checkInc5, content)
}

// TestApplyStoredMsgs executes a white-box unit
// test on implemented ApplyStoredMsgs() function.
func TestApplyStoredMsgs(t *testing.T) {

	// Create logger.
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stdout))

	// Create temporary directory.
	dir, err := ioutil.TempDir("", "TestApplyStoredMsgs-")
	assert.Nilf(t, err, "failed to create temporary directory: %v", err)
	defer os.RemoveAll(dir)

	// Create path to temporary log files.
	tmpLogFile := filepath.Join(dir, "log")
	tmpVClockFile := filepath.Join(dir, "vclock")

	// Write binary encoded test message to log file.
	err = ioutil.WriteFile(tmpLogFile, writeApply1, 0600)
	assert.Nilf(t, err, "expected writing test content 1 to log file not to fail but received: %v", err)

	// Open log file for writing.
	write, err := os.OpenFile(tmpLogFile, (os.O_CREATE | os.O_WRONLY | os.O_APPEND), 0600)
	assert.Nilf(t, err, "failed to open temporary log file for writing: %v", err)

	// Open log file for updating.
	upd, err := os.OpenFile(tmpLogFile, os.O_RDWR, 0600)
	assert.Nilf(t, err, "failed to open temporary log file for updating: %v", err)

	// Open log file of last known vector clock values.
	vclockLog, err := os.OpenFile(tmpVClockFile, (os.O_CREATE | os.O_RDWR), 0600)
	assert.Nilf(t, err, "failed to open temporary vector clock file: %v", err)

	// Simulate nodes.
	nodes := map[string]string{
		"other-node-1": "10.0.0.1",
		"other-node-2": "10.10.0.23",
		"other-node-3": "10.255.0.91",
	}

	// Bundle information in Receiver struct.
	recv := &Receiver{
		lock:             &sync.Mutex{},
		logger:           logger,
		name:             "worker-1",
		msgInLog:         make(chan struct{}, 1),
		writeLog:         write,
		updLog:           upd,
		vclock:           make(map[string]uint32),
		vclockLog:        vclockLog,
		stopApply:        make(chan struct{}),
		applyCRDTUpdChan: make(chan Msg),
		doneCRDTUpdChan:  make(chan struct{}),
		nodes:            nodes,
	}

	// Reset position in update file to beginning.
	_, err = recv.updLog.Seek(0, os.SEEK_SET)
	assert.Nilf(t, err, "expected resetting of position in update log not to fail but received: %v", err)

	// Reset position in vector clock file to beginning.
	_, err = recv.vclockLog.Seek(0, os.SEEK_SET)
	assert.Nilf(t, err, "expected resetting of position in vector clock file not to fail but received: %v", err)

	// Set vector clock entries to 0.
	for node := range nodes {
		recv.vclock[node] = 0
	}

	// Including the entry of this node.
	recv.vclock[recv.name] = 0

	// Run apply function to test.
	go func() {
		recv.ApplyStoredMsgs()
	}()

	// Send msgInLog trigger to start apply function.
	recv.msgInLog <- struct{}{}

	// Receive message to apply in correct channel.
	msg, ok := <-recv.applyCRDTUpdChan
	assert.Equalf(t, true, ok, "expected waiting for message on channel to succeed but received: %v", ok)

	// Signal waiting apply function that message was
	// applied successfully at CRDT level.
	recv.doneCRDTUpdChan <- struct{}{}

	// Stop apply function.
	recv.stopApply <- struct{}{}

	// Check received message for correctness.
	assert.Equalf(t, "worker-1", msg.Replica, "expected 'worker-1' as Replica in msg but received: %v", msg.Replica)
	assert.Equalf(t, map[string]uint32{"worker-1": uint32(1), "storage": uint32(0)}, msg.Vclock, "expected 'worker-1:1 storage:0' as Vclock in msg but received: %v", msg.Vclock)
	assert.Equalf(t, "create", msg.Operation, "expected 'create' as Operation in msg but received: %v", msg.Operation)
	assert.Equalf(t, (*Msg_DELETE)(nil), msg.Delete, "expected no Delete entry in msg but received: %v", msg.Delete)
	assert.Equalf(t, (*Msg_RENAME)(nil), msg.Rename, "expected no Rename entry in msg but received: %v", msg.Rename)
	assert.Equalf(t, (*Msg_APPEND)(nil), msg.Append, "expected no Append entry in msg but received: %v", msg.Append)
	assert.Equalf(t, (*Msg_EXPUNGE)(nil), msg.Expunge, "expected no Expunge entry in msg but received: %v", msg.Expunge)
	assert.Equalf(t, (*Msg_STORE)(nil), msg.Store, "expected no Store entry in msg but received: %v", msg.Store)
	assert.Equalf(t, (*Msg_COPY)(nil), msg.Copy, "expected no Copy entry in msg but received: %v", msg.Copy)
	assert.Equalf(t, "user1", msg.Create.User, "expected 'user1' as msg.Create.User but received: %v", msg.Create.User)
	assert.Equalf(t, "university", msg.Create.Mailbox, "expected 'university' as msg.Create.Mailbox but received: %v", msg.Create.Mailbox)
	assert.Equalf(t, "university", msg.Create.AddMailbox.Value, "expected 'university' as msg.Create.AddMailbox.Value but received: %v", msg.Create.AddMailbox.Value)
	assert.Equalf(t, "aa59585f-5a5f-4ea9-887c-74ab2e3f1f4a", msg.Create.AddMailbox.Tag, "expected 'aa59585f-5a5f-4ea9-887c-74ab2e3f1f4a' as msg.Create.AddMailbox.Tag but received: %v", msg.Create.AddMailbox.Tag)
	assert.Equalf(t, ([]byte)(nil), msg.Create.AddMailbox.Contents, "expected no Contents as msg.Create.AddMailbox.Contents but received: %v", msg.Create.AddMailbox.Contents)

	// Check file system content of log file.
	content, err := ioutil.ReadFile(tmpLogFile)
	assert.Nilf(t, err, "expected nil error for ReadFile() but received: %v", err)
	assert.Equalf(t, []byte{}, content, "expected '%s' in log file but found: %v", []byte{}, content)

	// Check file system content of vector clock file.
	content, err = ioutil.ReadFile(tmpVClockFile)
	assert.Nilf(t, err, "expected nil error for ReadFile() but received: %v", err)
	assert.True(t, bytes.Contains(content, []byte("worker-1:1")), "expected 'worker-1:1' to be present in vector clock file but was not")

	// Write second binary encoded test message to log file.
	err = ioutil.WriteFile(tmpLogFile, writeApply2, 0600)
	assert.Nilf(t, err, "expected writing test content 2 to log file not to fail but received: %v", err)

	// Reset vector clock internally.
	recv.vclock["worker-1"] = uint32(0)
	err = recv.SaveVClockEntries()
	assert.Nilf(t, err, "expected reset writing of vector clock file not to fail but received: %v", err)

	// Run apply function again for second test.
	go func() {
		recv.ApplyStoredMsgs()
	}()

	// Send msgInLog trigger to start apply function.
	recv.msgInLog <- struct{}{}

	// Receive message to apply in correct channel.
	msg, ok = <-recv.applyCRDTUpdChan
	assert.Equalf(t, true, ok, "expected waiting for message on channel to succeed but received: %v", ok)

	// Signal waiting apply function that message was
	// applied successfully at CRDT level.
	recv.doneCRDTUpdChan <- struct{}{}

	time.Sleep(1 * time.Second)

	// Check received message for correctness.
	assert.Equalf(t, "worker-1", msg.Replica, "expected 'worker-1' as Replica in msg but received: %v", msg.Replica)
	assert.Equalf(t, map[string]uint32{"worker-1": uint32(1), "storage": uint32(0)}, msg.Vclock, "expected 'worker-1:1 storage:0' as Vclock in msg but received: %v", msg.Vclock)
	assert.Equalf(t, "create", msg.Operation, "expected 'create' as Operation in msg but received: %v", msg.Operation)
	assert.Equalf(t, (*Msg_DELETE)(nil), msg.Delete, "expected no Delete entry in msg but received: %v", msg.Delete)
	assert.Equalf(t, (*Msg_RENAME)(nil), msg.Rename, "expected no Rename entry in msg but received: %v", msg.Rename)
	assert.Equalf(t, (*Msg_APPEND)(nil), msg.Append, "expected no Append entry in msg but received: %v", msg.Append)
	assert.Equalf(t, (*Msg_EXPUNGE)(nil), msg.Expunge, "expected no Expunge entry in msg but received: %v", msg.Expunge)
	assert.Equalf(t, (*Msg_STORE)(nil), msg.Store, "expected no Store entry in msg but received: %v", msg.Store)
	assert.Equalf(t, (*Msg_COPY)(nil), msg.Copy, "expected no Copy entry in msg but received: %v", msg.Copy)
	assert.Equalf(t, "user2", msg.Create.User, "expected 'user2' as msg.Create.User but received: %v", msg.Create.User)
	assert.Equalf(t, "LongAndInterestingName", msg.Create.Mailbox, "expected 'LongAndInterestingName' as msg.Create.Mailbox but received: %v", msg.Create.Mailbox)
	assert.Equalf(t, "LongAndInterestingName", msg.Create.AddMailbox.Value, "expected 'LongAndInterestingName' as msg.Create.AddMailbox.Value but received: %v", msg.Create.AddMailbox.Value)
	assert.Equalf(t, "525a3f40-7c2c-4b9a-94c8-a3432f25a28a", msg.Create.AddMailbox.Tag, "expected '525a3f40-7c2c-4b9a-94c8-a3432f25a28a' as msg.Create.AddMailbox.Tag but received: %v", msg.Create.AddMailbox.Tag)
	assert.Equalf(t, ([]byte)(nil), msg.Create.AddMailbox.Contents, "expected no Contents as msg.Create.AddMailbox.Contents but received: %v", msg.Create.AddMailbox.Contents)

	// Check file system content of log file.
	content, err = ioutil.ReadFile(tmpLogFile)
	assert.Nilf(t, err, "expected nil error for ReadFile() but received: %v", err)
	assert.Equalf(t, checkApply2a, content, "expected '%s' in log file but found: %v", checkApply2a, content)

	// Check file system content of vector clock file.
	content, err = ioutil.ReadFile(tmpVClockFile)
	assert.Nilf(t, err, "expected nil error for ReadFile() but received: %v", err)
	assert.True(t, bytes.Contains(content, []byte("worker-1:1")), "expected 'worker-1:1' to be present in vector clock file but was not")

	// Send msgInLog trigger to start apply function.
	recv.msgInLog <- struct{}{}

	// Receive message to apply in correct channel.
	msg, ok = <-recv.applyCRDTUpdChan
	assert.Equalf(t, true, ok, "expected waiting for message on channel to succeed but received: %v", ok)

	// Signal waiting apply function that message was
	// applied successfully at CRDT level.
	recv.doneCRDTUpdChan <- struct{}{}

	time.Sleep(1 * time.Second)

	// Check received message for correctness.
	assert.Equalf(t, "worker-1", msg.Replica, "expected 'worker-1' as Replica in msg but received: %v", msg.Replica)
	assert.Equalf(t, map[string]uint32{"worker-1": uint32(2), "storage": uint32(0)}, msg.Vclock, "expected 'worker-1:2 storage:0' as Vclock in msg but received: %v", msg.Vclock)
	assert.Equalf(t, "delete", msg.Operation, "expected 'delete' as Operation in msg but received: %v", msg.Operation)
	assert.Equalf(t, (*Msg_CREATE)(nil), msg.Create, "expected no Create entry in msg but received: %v", msg.Create)
	assert.Equalf(t, (*Msg_RENAME)(nil), msg.Rename, "expected no Rename entry in msg but received: %v", msg.Rename)
	assert.Equalf(t, (*Msg_APPEND)(nil), msg.Append, "expected no Append entry in msg but received: %v", msg.Append)
	assert.Equalf(t, (*Msg_EXPUNGE)(nil), msg.Expunge, "expected no Expunge entry in msg but received: %v", msg.Expunge)
	assert.Equalf(t, (*Msg_STORE)(nil), msg.Store, "expected no Store entry in msg but received: %v", msg.Store)
	assert.Equalf(t, (*Msg_COPY)(nil), msg.Copy, "expected no Copy entry in msg but received: %v", msg.Copy)
	assert.Equalf(t, "user2", msg.Delete.User, "expected 'user2' as msg.Delete.User but received: %v", msg.Delete.User)
	assert.Equalf(t, "LongAndInterestingName", msg.Delete.Mailbox, "expected 'LongAndInterestingName' as msg.Delete.Mailbox but received: %v", msg.Delete.Mailbox)
	assert.Equalf(t, "LongAndInterestingName", msg.Delete.RmvMailbox[0].Value, "expected 'LongAndInterestingName' as msg.Delete.RmvMailbox[0].Value but received: %v", msg.Delete.RmvMailbox[0].Value)
	assert.Equalf(t, "525a3f40-7c2c-4b9a-94c8-a3432f25a28a", msg.Delete.RmvMailbox[0].Tag, "expected '525a3f40-7c2c-4b9a-94c8-a3432f25a28a' as msg.Delete.RmvMailbox[0].Tag but received: %v", msg.Delete.RmvMailbox[0].Tag)
	assert.Equalf(t, ([]byte)(nil), msg.Delete.RmvMailbox[0].Contents, "expected no Contents as msg.Delete.RmvMailbox[0].Contents but received: %v", msg.Delete.RmvMailbox[0].Contents)

	// Check file system content of log file.
	content, err = ioutil.ReadFile(tmpLogFile)
	assert.Nilf(t, err, "expected nil error for ReadFile() but received: %v", err)
	assert.Equalf(t, checkApply2b, content, "expected '%s' in log file but found: %v", checkApply2b, content)

	// Check file system content of vector clock file.
	content, err = ioutil.ReadFile(tmpVClockFile)
	assert.Nilf(t, err, "expected nil error for ReadFile() but received: %v", err)
	assert.True(t, bytes.Contains(content, []byte("worker-1:2")), "expected 'worker-1:2' to be present in vector clock file but was not")

	// Send msgInLog trigger to start apply function.
	recv.msgInLog <- struct{}{}

	// Receive message to apply in correct channel.
	msg, ok = <-recv.applyCRDTUpdChan
	assert.Equalf(t, true, ok, "expected waiting for message on channel to succeed but received: %v", ok)

	// Signal waiting apply function that message was
	// applied successfully at CRDT level.
	recv.doneCRDTUpdChan <- struct{}{}

	// Stop apply function.
	recv.stopApply <- struct{}{}
	close(recv.msgInLog)

	// Check received message for correctness.
	assert.Equalf(t, "worker-1", msg.Replica, "expected 'worker-1' as Replica in msg but received: %v", msg.Replica)
	assert.Equalf(t, map[string]uint32{"worker-1": uint32(3), "storage": uint32(0)}, msg.Vclock, "expected 'worker-1:3 storage:0' as Vclock in msg but received: %v", msg.Vclock)
	assert.Equalf(t, "create", msg.Operation, "expected 'create' as Operation in msg but received: %v", msg.Operation)
	assert.Equalf(t, (*Msg_DELETE)(nil), msg.Delete, "expected no Delete entry in msg but received: %v", msg.Delete)
	assert.Equalf(t, (*Msg_RENAME)(nil), msg.Rename, "expected no Rename entry in msg but received: %v", msg.Rename)
	assert.Equalf(t, (*Msg_APPEND)(nil), msg.Append, "expected no Append entry in msg but received: %v", msg.Append)
	assert.Equalf(t, (*Msg_EXPUNGE)(nil), msg.Expunge, "expected no Expunge entry in msg but received: %v", msg.Expunge)
	assert.Equalf(t, (*Msg_STORE)(nil), msg.Store, "expected no Store entry in msg but received: %v", msg.Store)
	assert.Equalf(t, (*Msg_COPY)(nil), msg.Copy, "expected no Copy entry in msg but received: %v", msg.Copy)
	assert.Equalf(t, "user2", msg.Create.User, "expected 'user2' as msg.Create.User but received: %v", msg.Create.User)
	assert.Equalf(t, "test", msg.Create.Mailbox, "expected 'test' as msg.Create.Mailbox but received: %v", msg.Create.Mailbox)
	assert.Equalf(t, "test", msg.Create.AddMailbox.Value, "expected 'test' as msg.Create.AddMailbox.Value but received: %v", msg.Create.AddMailbox.Value)
	assert.Equalf(t, "ad8877b0-d384-4f38-82ac-a02fd78fa93a", msg.Create.AddMailbox.Tag, "expected 'ad8877b0-d384-4f38-82ac-a02fd78fa93a' as msg.Create.AddMailbox.Tag but received: %v", msg.Create.AddMailbox.Tag)
	assert.Equalf(t, ([]byte)(nil), msg.Create.AddMailbox.Contents, "expected no Contents as msg.Create.AddMailbox.Contents but received: %v", msg.Create.AddMailbox.Contents)

	// Check file system content of log file.
	content, err = ioutil.ReadFile(tmpLogFile)
	assert.Nilf(t, err, "expected nil error for ReadFile() but received: %v", err)
	assert.Equalf(t, []byte{}, content, "expected '%s' in log file but found: %v", []byte{}, content)

	// Check file system content of vector clock file.
	content, err = ioutil.ReadFile(tmpVClockFile)
	assert.Nilf(t, err, "expected nil error for ReadFile() but received: %v", err)
	assert.True(t, bytes.Contains(content, []byte("worker-1:3")), "expected 'worker-1:3' to be present in vector clock file but was not")
}
