package main

import (
	"log"
	"time"

	"github.com/numbleroot/pluto/imap"
	"github.com/numbleroot/pluto/utils"
)

// Structs

// RunAllNodes runs all needed nodes for a proper multi-node
// test setup in background. It also handles shutdown of these
// nodes when appropriate signals are received.
func RunAllNodes(testEnv *utils.TestEnv, workerName string) {

	go func() {

		// Initialize storage node.
		storage, err := imap.InitStorage(testEnv.Config)
		if err != nil {
			log.Fatal(err)
		}

		go func() {

			// Wait for shutdown signal on channel.
			<-testEnv.DownStorage

			log.Printf("[utils.RunAllNodes] Closing storage socket.\n")

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
		worker, err := imap.InitWorker(testEnv.Config, workerName)
		if err != nil {
			log.Fatal(err)
		}

		go func() {

			// Wait for shutdown signal on channel.
			<-testEnv.DownWorker

			log.Printf("[utils.RunAllNodes] Closing '%s' socket.\n", workerName)

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
			log.Fatal(err)
		}

		// Initialize distributor node.
		distr, err := imap.InitDistributor(testEnv.Config, authenticator)
		if err != nil {
			log.Fatal(err)
		}

		go func() {

			// Wait for shutdown signal on channel.
			<-testEnv.DownDistr

			log.Printf("[utils.RunAllNodes] Closing distributor socket.\n")

			// Shut down distributor node.
			distr.Socket.Close()

			// Signal back successful shutdown.
			testEnv.DoneDistr <- struct{}{}
		}()

		// Run the distributor node.
		_ = distr.Run()
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
