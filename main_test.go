package main

import (
	"crypto/tls"
	stdlog "log"
	"os"
	"testing"
	"time"

	"github.com/numbleroot/pluto/crypto"
	"github.com/numbleroot/pluto/distributor"
	"github.com/numbleroot/pluto/storage"
	"github.com/numbleroot/pluto/utils"
	"github.com/numbleroot/pluto/worker"
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

		intlTLSConfig, err := crypto.NewInternalTLSConfig(testEnv.Config.Storage.TLS.CertLoc, testEnv.Config.Storage.TLS.KeyLoc, testEnv.Config.RootCertLoc)
		if err != nil {
			stdlog.Fatal(err)
		}

		// Create needed sockets. First, mail socket.
		mailSocket, err := tls.Listen("tcp", testEnv.Config.Storage.ListenMailAddr, intlTLSConfig)
		if err != nil {
			stdlog.Fatal(err)
		}
		defer mailSocket.Close()

		var storageS storage.Service
		storageS = storage.NewService(&intlConn{intlTLSConfig, testEnv.Config.IntlConnRetry}, mailSocket, testEnv.Config.Storage, testEnv.Config.Workers)

		go func() {

			// Wait for shutdown signal on channel.
			<-testEnv.DownStorage

			stdlog.Printf("[utils.RunAllNodes] Closing storage socket")

			//// Shut down storage node.
			//storage.MailSocket.Close()
			//storage.SyncSocket.Close()

			// Signal back successful shutdown.
			testEnv.DoneStorage <- struct{}{}
		}()

		// Run the storage node.
		if err := storageS.Run(); err != nil {
			stdlog.Fatal(err)
		}
	}()

	// Wait shortly for storage node to have started.
	time.Sleep(100 * time.Millisecond)

	go func() {

		workerConfig, ok := testEnv.Config.Workers[workerName]
		if !ok {
			stdlog.Fatal("can't find correct worker config")
		}

		intlTLSConfig, err := crypto.NewInternalTLSConfig(workerConfig.TLS.CertLoc, workerConfig.TLS.KeyLoc, testEnv.Config.RootCertLoc)
		if err != nil {
			stdlog.Fatal(err)
		}

		// Create needed sockets. First, mail socket.
		mailSocket, err := tls.Listen("tcp", workerConfig.ListenMailAddr, intlTLSConfig)
		if err != nil {
			stdlog.Fatal(err)
		}
		defer mailSocket.Close()

		var workerS worker.Service
		workerS = worker.NewService(&intlConn{intlTLSConfig, testEnv.Config.IntlConnRetry}, mailSocket, workerConfig, workerName)

		go func() {

			// Wait for shutdown signal on channel.
			<-testEnv.DownWorker

			stdlog.Printf("[utils.RunAllNodes] Closing %s socket", workerName)

			//// Shut down worker node.
			//worker.MailSocket.Close()
			//worker.SyncSocket.Close()

			// Signal back successful shutdown.
			testEnv.DoneWorker <- struct{}{}
		}()

		// Run the worker node.
		if err := workerS.Run(); err != nil {
			stdlog.Fatal(err)
		}
	}()

	// Wait shortly for worker node to have started.
	time.Sleep(100 * time.Millisecond)

	go func() {

		authenticator, err := initAuthenticator(testEnv.Config)
		if err != nil {
			stdlog.Fatal(err)
		}

		intlTLSConfig, err := crypto.NewInternalTLSConfig(testEnv.Config.Distributor.InternalTLS.CertLoc, testEnv.Config.Distributor.InternalTLS.KeyLoc, testEnv.Config.RootCertLoc)
		if err != nil {
			stdlog.Fatal(err)
		}

		distr := distributor.NewService(authenticator, &intlConn{intlTLSConfig, testEnv.Config.IntlConnRetry}, testEnv.Config.Workers)

		go func() {

			// Wait for shutdown signal on channel.
			<-testEnv.DownDistr

			stdlog.Printf("[utils.RunAllNodes] Closing distributor socket")

			// Signal back successful shutdown.
			testEnv.DoneDistr <- struct{}{}
		}()

		publicTLSConfig, err := crypto.NewPublicTLSConfig(testEnv.Config.Distributor.PublicTLS.CertLoc, testEnv.Config.Distributor.PublicTLS.KeyLoc)
		if err != nil {
			stdlog.Fatal(err)
		}

		mailSocket, err := tls.Listen("tcp", testEnv.Config.Distributor.ListenMailAddr, publicTLSConfig)
		if err != nil {
			stdlog.Fatal(err)
		}
		defer mailSocket.Close()

		// Run the distributor node.
		if err := distr.Run(mailSocket, testEnv.Config.IMAP.Greeting); err != nil {
			stdlog.Fatal(err)
		}
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
