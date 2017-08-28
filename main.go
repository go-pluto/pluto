package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"

	"crypto/tls"
	"io/ioutil"
	"path/filepath"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/go-pluto/maildir"
	"github.com/go-pluto/pluto/auth"
	"github.com/go-pluto/pluto/comm"
	"github.com/go-pluto/pluto/config"
	"github.com/go-pluto/pluto/crypto"
	"github.com/go-pluto/pluto/distributor"
	"github.com/go-pluto/pluto/storage"
	"github.com/go-pluto/pluto/worker"
	"github.com/satori/go.uuid"
	"google.golang.org/grpc/grpclog"
)

// Functions

// initAuthenticator of the correct implementation specified
// in the config to be used in the imap.Distributor.
func initAuthenticator(config *config.Config) (distributor.Authenticator, error) {

	switch config.Distributor.AuthAdapter {
	case "AuthPostgres":
		// Connect to PostgreSQL database.
		return auth.NewPostgresAuthenticator(
			config.Distributor.AuthPostgres.IP,
			config.Distributor.AuthPostgres.Port,
			config.Distributor.AuthPostgres.Database,
			config.Distributor.AuthPostgres.User,
			config.Distributor.AuthPostgres.Password,
			config.Distributor.AuthPostgres.UseTLS,
		)
	default: // AuthFile
		// Open authentication file and read user information.
		return auth.NewFile(
			config.Distributor.AuthFile.File,
			config.Distributor.AuthFile.Separator,
		)
	}
}

// initLogger initializes a JSON gokit-logger set
// to the according log level supplied via cli flag.
func initLogger(loglevel string) log.Logger {

	// Specify verbosity of gRPC components (only ERROR).
	grpclog.SetLoggerV2(grpclog.NewLoggerV2(ioutil.Discard, ioutil.Discard, os.Stdout))

	logger := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
	logger = log.With(logger,
		"ts", log.DefaultTimestampUTC,
		"caller", log.Caller(5),
	)

	switch strings.ToLower(loglevel) {
	case "info":
		logger = level.NewFilter(logger, level.AllowInfo())
	case "warn":
		logger = level.NewFilter(logger, level.AllowWarn())
	case "error":
		logger = level.NewFilter(logger, level.AllowError())
	default:
		logger = level.NewFilter(logger, level.AllowDebug())
	}

	return logger
}

// createUserFiles adds the required files and folders
// for the number of test users we make use of in our tests.
// This concerns Maildir and CRDT files and folders.
func createUserFiles(crdtLayerRoot string, maildirRoot string, start int, end int) error {

	err := os.MkdirAll(maildirRoot, 0755)
	if err != nil {
		return err
	}

	err = os.MkdirAll(crdtLayerRoot, 0755)
	if err != nil {
		return err
	}

	for i := start; i <= end; i++ {

		mail := maildir.Dir(filepath.Join(maildirRoot, fmt.Sprintf("user%d", i)))
		err := mail.Create()
		if err != nil && os.IsNotExist(err) {
			return err
		}

		crdtFolder := filepath.Join(crdtLayerRoot, fmt.Sprintf("user%d", i))
		err = os.MkdirAll(crdtFolder, 0755)
		if err != nil {
			return err
		}

		structureFile := filepath.Join(crdtFolder, "structure.crdt")

		if !exists(structureFile) {

			data := fmt.Sprintf("SU5CT1g=;%s", uuid.NewV4().String())
			err := ioutil.WriteFile(structureFile, []byte(data), 0644)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// exists returns true if a file exists.
func exists(path string) bool {

	if _, err := os.Stat(path); err != nil {

		if os.IsNotExist(err) {
			return false
		}

		return false
	}

	return true
}

func main() {

	var err error

	// Set CPUs usable by pluto to all available.
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Parse command-line flag that defines a config path.
	configFlag := flag.String("config", "config.toml", "Provide path to configuration file in TOML syntax.")
	loglevelFlag := flag.String("loglevel", "debug", "This flag sets the default logging level.")
	distributorFlag := flag.Bool("distributor", false, "Append this flag to indicate that this process should take the role of the distributor.")
	workerFlag := flag.String("worker", "", "If this process is intended to run as one of the IMAP worker nodes, specify which of the ones defined in your config file this should be.")
	storageFlag := flag.Bool("storage", false, "Append this flag to indicate that this process should take the role of the storage node.")
	flag.Parse()

	logger := initLogger(*loglevelFlag)

	// Read configuration from file.
	conf, err := config.LoadConfig(*configFlag)
	if err != nil {
		level.Error(logger).Log(
			"msg", "failed to load config",
			"err", err,
		)
		os.Exit(1)
	}

	plutoMetrics := NewPlutoMetrics(conf.Distributor.PrometheusAddr)

	// Initialize and run a node of the pluto
	// system based on passed command line flag.
	if *distributorFlag {

		// Add node name context to subsequent log outputs.
		logger = log.With(logger, "node", conf.Distributor.Name)

		// Run an HTTP server in a goroutine to expose this distributor's metrics.
		go runPromHTTP(logger, conf.Distributor.PrometheusAddr)

		authenticator, err := initAuthenticator(conf)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to initialize an authenticator",
				"err", err,
			)
			os.Exit(1)
		}

		publicTLSConfig, err := crypto.NewPublicTLSConfig(conf.Distributor.PublicCertLoc, conf.Distributor.PublicKeyLoc)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to create public TLS config",
				"err", err,
			)
			os.Exit(1)
		}

		mailSocket, err := tls.Listen("tcp", conf.Distributor.ListenMailAddr, publicTLSConfig)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to listen for public mail TLS connections",
				"err", err,
			)
			os.Exit(1)
		}
		defer mailSocket.Close()

		level.Info(logger).Log(
			"msg", "accepting public mail connections",
			"public_addr", conf.Distributor.PublicMailAddr,
			"listen_addr", conf.Distributor.ListenMailAddr,
		)

		intlTLSConfig, err := crypto.NewInternalTLSConfig(conf.Distributor.InternalCertLoc, conf.Distributor.InternalKeyLoc, conf.RootCertLoc)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to create internal TLS config",
				"err", err,
			)
			os.Exit(1)
		}

		var distrS distributor.Service
		distrS = distributor.NewService(conf.Distributor.Name, logger, plutoMetrics.Distributor, authenticator, intlTLSConfig, conf.Workers, conf.Storage.PublicMailAddr)

		if err := distrS.Run(mailSocket, conf.IMAP.Greeting); err != nil {
			level.Error(logger).Log(
				"msg", "failed to run",
				"err", err,
			)
			os.Exit(1)
		}

	} else if *workerFlag != "" {

		// Check if supplied worker with workerName actually is configured.
		wConfig, ok := conf.Workers[*workerFlag]
		if !ok {

			// Retrieve first valid worker ID to provide feedback.
			var workerID string
			for workerID = range conf.Workers {
				break
			}

			level.Error(logger).Log(
				"msg", fmt.Sprintf("specified worker ID does not exist in config file, use for example '%s'", workerID),
			)
			os.Exit(1)
		}

		// Add node name context to subsequent log outputs.
		logger = log.With(logger, "node", wConfig.Name)

		// Run an HTTP server in a goroutine to expose this worker's metrics.
		go runPromHTTP(logger, wConfig.PrometheusAddr)

		// Create all non-existent files and folders for
		// all users this worker is responsible for.
		err := createUserFiles(wConfig.CRDTLayerRoot, wConfig.MaildirRoot, wConfig.UserStart, wConfig.UserEnd)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to create user files",
				"err", err,
			)
			os.Exit(1)
		}

		tlsConfig, err := crypto.NewInternalTLSConfig(wConfig.CertLoc, wConfig.KeyLoc, conf.RootCertLoc)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to create internal TLS config",
				"err", err,
			)
			os.Exit(1)
		}

		var workerS worker.Service
		workerS = worker.NewService(wConfig.Name, tlsConfig, conf)

		// Create needed synchronization socket used by gRPC.
		syncSocket, err := net.Listen("tcp", wConfig.ListenSyncAddr)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to open synchronization socket",
				"err", err,
			)
			os.Exit(1)
		}
		defer syncSocket.Close()

		// Retrive name and peers of the one synchronization
		// subnet this worker is part of.
		var subnet string
		var peers map[string]string
		for subnet, peers = range wConfig.Peers {
			break
		}

		// Initialize channels for this node.
		applyCRDTUpd := make(chan comm.Msg)
		doneCRDTUpd := make(chan struct{})

		// Construct path to receiving and sending CRDT logs
		// for the subnet this worker node is part of.
		recvCRDTLog := filepath.Join(wConfig.CRDTLayerRoot, fmt.Sprintf("%s-receiving.log", subnet))
		sendCRDTLog := filepath.Join(wConfig.CRDTLayerRoot, fmt.Sprintf("%s-sending.log", subnet))
		vclockLog := filepath.Join(wConfig.CRDTLayerRoot, fmt.Sprintf("%s-vclock.log", subnet))

		// Initialize receiving goroutine for sync operations.
		incVClock, updVClock, err := comm.InitReceiver(logger, wConfig.Name, wConfig.ListenSyncAddr, wConfig.PublicSyncAddr, recvCRDTLog, vclockLog, syncSocket, tlsConfig, applyCRDTUpd, doneCRDTUpd, peers)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to initialize receiver",
				"err", err,
			)
			os.Exit(1)
		}

		// Init sending part of CRDT communication and send messages in background.
		syncSendChan, err := comm.InitSender(logger, wConfig.Name, sendCRDTLog, tlsConfig, incVClock, updVClock, peers)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to initialize sender",
				"err", err,
			)
			os.Exit(1)
		}

		// Apply CRDT updates in background.
		go workerS.ApplyCRDTUpd(applyCRDTUpd, doneCRDTUpd)

		// Run required initialization code for worker.
		err = workerS.Init(logger, conf.IMAP.HierarchySeparator, syncSendChan)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to initilize service",
				"err", err,
			)
			os.Exit(1)
		}

		// Create socket for gRPC IMAP connections.
		mailSocket, err := net.Listen("tcp", wConfig.ListenMailAddr)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to open socket for proxied mail traffic",
				"err", err,
			)
			os.Exit(1)
		}
		defer mailSocket.Close()

		level.Info(logger).Log(
			"msg", "accepting proxied mail connections",
			"public_addr", wConfig.PublicMailAddr,
			"listen_addr", wConfig.ListenMailAddr,
		)

		// Run main handler routine on gRPC-served IMAP socket.
		err = workerS.Serve(mailSocket)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to run Serve() on IMAP gRPC socket",
				"err", err,
			)
		}

	} else if *storageFlag {

		// Add node name context to subsequent log outputs.
		logger = log.With(logger, "node", conf.Storage.Name)

		// Run an HTTP server in a goroutine to expose this storage's metrics.
		go runPromHTTP(logger, conf.Storage.PrometheusAddr)

		tlsConfig, err := crypto.NewInternalTLSConfig(conf.Storage.CertLoc, conf.Storage.KeyLoc, conf.RootCertLoc)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to create internal TLS config",
				"err", err,
			)
			os.Exit(1)
		}

		var storageS storage.Service
		storageS = storage.NewService(conf.Storage.Name, tlsConfig, conf)

		syncSockets := make(map[string]net.Listener)
		peersToSubnet := make(map[string]string)
		syncSendChans := make(map[string]chan comm.Msg)

		for subnet, syncAddrs := range conf.Storage.SyncAddrs {

			// Create needed synchronization sockets used by gRPC.

			var err error
			syncSockets[subnet], err = net.Listen("tcp", syncAddrs["Listen"])
			if err != nil {
				level.Error(logger).Log(
					"msg", "failed to open synchronization socket",
					"sync_addr", syncAddrs["Listen"],
					"err", err,
				)
				os.Exit(1)
			}
			defer syncSockets[subnet].Close()
		}

		for subnet, peers := range conf.Storage.Peers {

			for worker := range peers {

				// Build reverse mapping from peer name
				// to subnet this peer is part of.
				peersToSubnet[worker] = subnet

				c, found := conf.Workers[worker]
				if found {

					// Create all non-existent files and folders on
					// storage for all users the currently examined
					// worker is responsible for.
					err := createUserFiles(conf.Storage.CRDTLayerRoot, conf.Storage.MaildirRoot, c.UserStart, c.UserEnd)
					if err != nil {
						level.Error(logger).Log(
							"msg", "failed to create user files",
							"err", err,
						)
						os.Exit(1)
					}
				}
			}

			// Initialize channels for this node.
			applyCRDTUpd := make(chan comm.Msg)
			doneCRDTUpd := make(chan struct{})

			// Construct path to receiving and sending CRDT logs
			// for the current subnet.
			recvCRDTLog := filepath.Join(conf.Storage.CRDTLayerRoot, fmt.Sprintf("%s-receiving.log", subnet))
			sendCRDTLog := filepath.Join(conf.Storage.CRDTLayerRoot, fmt.Sprintf("%s-sending.log", subnet))
			vclockLog := filepath.Join(conf.Storage.CRDTLayerRoot, fmt.Sprintf("%s-vclock.log", subnet))

			level.Debug(logger).Log(
				"msg", "sync addresses used",
				"subnet", subnet,
				"listen", conf.Storage.SyncAddrs[subnet]["Listen"],
				"public", conf.Storage.SyncAddrs[subnet]["Public"],
				"socket", syncSockets[subnet].Addr(),
			)

			// Initialize a receiving goroutine for sync operations
			// for each worker node.
			incVClock, updVClock, err := comm.InitReceiver(logger, conf.Storage.Name, conf.Storage.SyncAddrs[subnet]["Listen"], conf.Storage.SyncAddrs[subnet]["Public"], recvCRDTLog, vclockLog, syncSockets[subnet], tlsConfig, applyCRDTUpd, doneCRDTUpd, peers)
			if err != nil {
				level.Error(logger).Log(
					"msg", "failed to initialize receiver",
					"err", err,
				)
				os.Exit(1)
			}

			// Init sending part of CRDT communication and send messages in background.
			syncSendChans[subnet], err = comm.InitSender(logger, conf.Storage.Name, sendCRDTLog, tlsConfig, incVClock, updVClock, peers)
			if err != nil {
				level.Error(logger).Log(
					"msg", "failed to initialize sender",
					"err", err,
				)
				os.Exit(1)
			}

			// Apply CRDT updates in background.
			go storageS.ApplyCRDTUpd(applyCRDTUpd, doneCRDTUpd)
		}

		// Run required initialization code for storage.
		err = storageS.Init(logger, conf.IMAP.HierarchySeparator, peersToSubnet, syncSendChans)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to initilize service",
				"err", err,
			)
			os.Exit(1)
		}

		// Create socket for gRPC IMAP connections.
		mailSocket, err := net.Listen("tcp", conf.Storage.ListenMailAddr)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to open socket for proxied mail traffic",
				"err", err,
			)
			os.Exit(1)
		}
		defer mailSocket.Close()

		level.Info(logger).Log(
			"msg", "accepting proxied mail connections",
			"public_addr", conf.Storage.PublicMailAddr,
			"listen_addr", conf.Storage.ListenMailAddr,
		)

		// Run main handler routine on gRPC-served IMAP socket.
		err = storageS.Serve(mailSocket)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to run Serve() on IMAP gRPC socket",
				"err", err,
			)
		}

	} else {

		// If no flags were specified, print usage
		// and return with failure value.
		flag.Usage()
		os.Exit(1)
	}
}
