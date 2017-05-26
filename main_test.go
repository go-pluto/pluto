package main

import (
	stdlog "log"
	"os"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/numbleroot/pluto/distributor"
	"github.com/numbleroot/pluto/imap"
	"github.com/numbleroot/pluto/utils"
)

// Functions

// TestMain executes initialization and teardown
// code needed for all tests in package main.
func TestMain(m *testing.M) {

	var err error

	// Create needed test environment.
	testEnv, err = utils.CreateTestEnv("./test-config.toml")
	if err != nil {
		stdlog.Fatal(err)
	}

	// Run all nodes in background.
	RunAllNodes(testEnv, "worker-1")

	// Run all tests of this package.
	success := m.Run()

	// Give background synchronization enough
	// time to finish communication.
	time.Sleep(10 * time.Second)

	// Tear down test setup.
	TearDownNormalSetup(testEnv)

	// Return with test return value.
	os.Exit(success)
}

// RunAllNodes runs all needed nodes for a proper multi-node
// test setup in background. It also handles shutdown of these
// nodes when appropriate signals are received.
func RunAllNodes(testEnv *utils.TestEnv, workerName string) {

	go func() {

		// Initialize storage node.
		storage, err := imap.InitStorage(testEnv.Config)
		if err != nil {
			stdlog.Fatal(err)
		}

		go func() {

			// Wait for shutdown signal on channel.
			<-testEnv.DownStorage

			stdlog.Printf("[utils.RunAllNodes] Closing storage socket")

			// Shut down storage node.
			storage.MailSocket.Close()
			storage.SyncSocket.Close()

			// Signal back successful shutdown.
			testEnv.DoneStorage <- struct{}{}
		}()

		// Run the storage node.
		_ = storage.Run()
	}()

	// Wait shortly for storage node to have started.
	time.Sleep(100 * time.Millisecond)

	go func() {

		// Initialize workerName worker node.
		worker, err := imap.InitWorker(log.NewNopLogger(), testEnv.Config, workerName)
		if err != nil {
			stdlog.Fatal(err)
		}

		go func() {

			// Wait for shutdown signal on channel.
			<-testEnv.DownWorker

			stdlog.Printf("[utils.RunAllNodes] Closing %s socket", workerName)

			// Shut down worker node.
			worker.MailSocket.Close()
			worker.SyncSocket.Close()

			// Signal back successful shutdown.
			testEnv.DoneWorker <- struct{}{}
		}()

		// Run the worker node.
		_ = worker.Run()
	}()

	// Wait shortly for worker node to have started.
	time.Sleep(100 * time.Millisecond)

	go func() {

		authenticator, err := initAuthenticator(testEnv.Config)
		if err != nil {
			stdlog.Fatal(err)
		}

		intConnectioner, err := NewInternalConnection(
			testEnv.Config.Distributor.InternalTLS.CertLoc,
			testEnv.Config.Distributor.InternalTLS.KeyLoc,
			testEnv.Config.RootCertLoc,
			testEnv.Config.IntlConnRetry,
		)
		if err != nil {
			stdlog.Fatal(err)
		}

		conn, err := publicDistributorConn(testEnv.Config.Distributor)
		if err != nil {
			stdlog.Fatal(err)
		}
		defer conn.Close()

		distr := distributor.NewService(authenticator, intConnectioner, testEnv.Config.Workers)

		go func() {

			// Wait for shutdown signal on channel.
			<-testEnv.DownDistr

			stdlog.Printf("[utils.RunAllNodes] Closing distributor socket")

			// Signal back successful shutdown.
			testEnv.DoneDistr <- struct{}{}
		}()

		// Run the distributor node.
		_ = distr.Run(conn, testEnv.Config.IMAP.Greeting)
	}()

	// Wait shortly for worker node to have started.
	time.Sleep(500 * time.Millisecond)
}

// TearDownNormalSetup takes care of shutting down the normally
// running nodes by sending shutdown signals and expecting
// success answers.
func TearDownNormalSetup(testEnv *utils.TestEnv) {

	// Signal to all nodes running background that they
	// are supposed to shut down now.
	testEnv.DownDistr <- struct{}{}
	testEnv.DownWorker <- struct{}{}
	testEnv.DownStorage <- struct{}{}

	// Wait for them to signal success back.
	<-testEnv.DoneDistr
	<-testEnv.DoneWorker
	<-testEnv.DoneStorage

	// Wait shortly.
	time.Sleep(500 * time.Millisecond)
}
