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

	write1 = []byte("hello")
	write2 = []byte("\nwhat\r\tabout!\"§$%&/()=strange#+?`?`?°°°characters")
	write3 = []byte("∰☕✔😉")
	write4 = []byte("1234567890")
	write5 = []byte(fmt.Sprintf("%g", math.MaxFloat64))

	check1 = []byte("5;hello")
	check2 = []byte("53;\nwhat\r\tabout!\"§$%&/()=strange#+?`?`?°°°characters")
	check3 = []byte("13;∰☕✔😉")
	check4 = []byte("10;1234567890")
	check5 = []byte(fmt.Sprintf("23;%g", math.MaxFloat64))
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
		name:        "test-node",
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

	for _ = range recv.msgInLog {
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
		name:     "test-node",
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
		Data: write1,
	})
	assert.Nilf(t, err, "expected nil error for Incoming() but received: %v", err)

	// Wait for signal that new message was written to log.
	<-recv.msgInLog

	// Validate received confirmation struct.
	assert.Equalf(t, confStatus, conf.Status, "expected conf to carry Status=0 but found: %v", conf.Status)

	// Read content of log file for inspection.
	content, err := ioutil.ReadFile(tmpLogFile)
	assert.Nilf(t, err, "expected nil error for ReadFile() but received: %v", err)

	// Check for correct content.
	assert.Equalf(t, check1, content, "expected '%s' in log file but found: %v", check1, content)

	// Value 2.
	// Write second value to file.
	conf, err = recv.Incoming(context.Background(), &BinMsg{
		Data: write2,
	})
	assert.Nilf(t, err, "expected nil error for Incoming() but received: %v", err)

	<-recv.msgInLog

	assert.Equalf(t, confStatus, conf.Status, "expected conf to carry Status=0 but found: %v", conf.Status)

	content, err = ioutil.ReadFile(tmpLogFile)
	assert.Nilf(t, err, "expected nil error for ReadFile() but received: %v", err)

	content = bytes.TrimPrefix(content, check1)

	assert.Equalf(t, check2, content, "expected '%s' in log file but found: %v", check2, content)

	// Value 3.
	// Write third value to file.
	conf, err = recv.Incoming(context.Background(), &BinMsg{
		Data: write3,
	})
	assert.Nilf(t, err, "expected nil error for Incoming() but received: %v", err)

	<-recv.msgInLog

	assert.Equalf(t, confStatus, conf.Status, "expected conf to carry Status=0 but found: %v", conf.Status)

	content, err = ioutil.ReadFile(tmpLogFile)
	assert.Nilf(t, err, "expected nil error for ReadFile() but received: %v", err)

	content = bytes.TrimPrefix(content, check1)
	content = bytes.TrimPrefix(content, check2)

	assert.Equalf(t, check3, content, "expected '%s' in log file but found: %v", check3, content)

	// Value 4.
	// Write fourth value to file.
	conf, err = recv.Incoming(context.Background(), &BinMsg{
		Data: write4,
	})
	assert.Nilf(t, err, "expected nil error for Incoming() but received: %v", err)

	<-recv.msgInLog

	assert.Equalf(t, confStatus, conf.Status, "expected conf to carry Status=0 but found: %v", conf.Status)

	content, err = ioutil.ReadFile(tmpLogFile)
	assert.Nilf(t, err, "expected nil error for ReadFile() but received: %v", err)

	content = bytes.TrimPrefix(content, check1)
	content = bytes.TrimPrefix(content, check2)
	content = bytes.TrimPrefix(content, check3)

	assert.Equalf(t, check4, content, "expected '%s' in log file but found: %v", check4, content)

	// Value 5.
	// Write fifth value to file.
	conf, err = recv.Incoming(context.Background(), &BinMsg{
		Data: write5,
	})
	assert.Nilf(t, err, "expected nil error for Incoming() but received: %v", err)

	<-recv.msgInLog

	assert.Equalf(t, confStatus, conf.Status, "expected conf to carry Status=0 but found: %v", conf.Status)

	content, err = ioutil.ReadFile(tmpLogFile)
	assert.Nilf(t, err, "expected nil error for ReadFile() but received: %v", err)

	content = bytes.TrimPrefix(content, check1)
	content = bytes.TrimPrefix(content, check2)
	content = bytes.TrimPrefix(content, check3)
	content = bytes.TrimPrefix(content, check4)

	assert.Equalf(t, check5, content, "expected '%s' in log file but found: %v", check5, content)
}
