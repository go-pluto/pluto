package comm

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"io/ioutil"

	"github.com/go-kit/kit/log/level"
)

// SaveVClockEntries writes the current status of vector
// clock to log file. It expects to be the only goroutine
// currently operating on the log file.
func (recv *Receiver) SaveVClockEntries() error {

	vclockString := ""

	// Construct string of current vector clock.
	for node, entry := range recv.vclock {

		if vclockString == "" {
			vclockString = fmt.Sprintf("%s:%d", node, entry)
		} else {
			vclockString = fmt.Sprintf("%s;%s:%d", vclockString, node, entry)
		}
	}

	// Over-write old vector clock log. Reset position
	// of read-write head to beginning.
	_, err := recv.vclockLog.Seek(0, os.SEEK_SET)
	if err != nil {
		return err
	}

	// Write vclock string to file.
	newNumOfBytes, err := recv.vclockLog.WriteString(vclockString)
	if err != nil {
		return err
	}

	// Truncate file to just written content.
	err = recv.vclockLog.Truncate(int64(newNumOfBytes))
	if err != nil {
		return err
	}

	// Make sure to write to stable storage before returning.
	err = recv.vclockLog.Sync()
	if err != nil {
		return err
	}

	return nil
}

// SetVClockEntries fetches saved vector clock entries
// from log file and sets them in internal vector clock.
// It expects to be the only goroutine currently operating
// on receiver.
func (recv *Receiver) SetVClockEntries() error {

	// Read all log contents.
	storedVClockBytes, err := ioutil.ReadAll(recv.vclockLog)
	if err != nil {
		return err
	}
	storedVClock := string(storedVClockBytes)

	// If log was empty (e.g., initially), return
	// success because we do not have anything to set.
	if storedVClock == "" {
		return nil
	}

	// Otherwise, split at semicola.
	pairs := strings.Split(string(storedVClock), ";")

	for _, pair := range pairs {

		// Split pairs at colon.
		entry := strings.Split(pair, ":")

		// Convert entry string to uint32.
		entryNumberBig, err := strconv.ParseUint(entry[1], 10, 32)
		if err != nil {
			return err
		}
		entryNumber := uint32(entryNumberBig)

		// Set elements in vector clock of receiver.
		recv.vclock[entry[0]] = entryNumber
	}

	return nil
}

// IncVClockEntry waits for an incoming name of a node on a
// channel defined during initialization and passed on to the
// sender. If the node is present in vector clock map, its
// value is incremented by one.
func (recv *Receiver) IncVClockEntry() {

	for {

		// Wait for name of node on channel.
		entry, ok := <-recv.incVClock
		if ok {

			recv.vclockLock.Lock()

			_, exists := recv.vclock[entry]
			if exists {

				// Increment the node's vector clock value.
				recv.vclock[entry]++

				// Make a deep copy of current vector clock
				// map to pass back via channel to sender.
				updatedVClock := make(map[string]uint32)
				for node, value := range recv.vclock {
					updatedVClock[node] = value
				}

				// Save updated vector clock to log file.
				err := recv.SaveVClockEntries()
				if err != nil {
					level.Error(recv.logger).Log(
						"msg", "saving updated vector clock to file failed",
						"err", err,
					)
					os.Exit(1)
				}

				// Send back the updated vector clock on other
				// defined channel to sender.
				recv.updVClock <- updatedVClock
			}

			recv.vclockLock.Unlock()
		}
	}
}
