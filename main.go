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

	if err := os.MkdirAll(maildirRoot, 0755); err != nil {
		return err
	}

	if err := os.MkdirAll(crdtLayerRoot, 0755); err != nil {
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

		inboxLog := filepath.Join(crdtFolder, "INBOX.log")
		mailboxStructureLog := filepath.Join(crdtFolder, "mailbox-structure.log")

		if !exists(inboxLog) {

			err := ioutil.WriteFile(inboxLog, nil, 0644)
			if err != nil {
				return err
			}
		}

		if !exists(mailboxStructureLog) {

			data := fmt.Sprintf("SU5CT1g=;%s\n", uuid.NewV4().String())
			err := ioutil.WriteFile(mailboxStructureLog, []byte(data), 0644)
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
				"node", conf.Distributor.Name,
				"err", err,
			)
			os.Exit(1)
		}

		mailSocket, err := tls.Listen("tcp", conf.Distributor.ListenMailAddr, publicTLSConfig)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to listen for public mail TLS connections",
				"node", conf.Distributor.Name,
				"err", err,
			)
			os.Exit(1)
		}
		defer mailSocket.Close()

		level.Info(logger).Log(
			"msg", "accepting public mail connections",
			"node", conf.Distributor.Name,
			"public_addr", conf.Distributor.PublicMailAddr,
			"listen_addr", conf.Distributor.ListenMailAddr,
		)

		intlTLSConfig, err := crypto.NewInternalTLSConfig(conf.Distributor.InternalCertLoc, conf.Distributor.InternalKeyLoc, conf.RootCertLoc)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to create internal TLS config",
				"node", conf.Distributor.Name,
				"err", err,
			)
			os.Exit(1)
		}

		var distrS distributor.Service
		distrS = distributor.NewService(conf.Distributor.Name, logger, plutoMetrics.Distributor, authenticator, intlTLSConfig, conf.Workers, conf.Storage.PublicMailAddr)

		if err := distrS.Run(mailSocket, conf.IMAP.Greeting); err != nil {
			level.Error(logger).Log(
				"msg", "failed to run",
				"node", conf.Distributor.Name,
				"err", err,
			)
			os.Exit(1)
		}

	} else if *workerFlag != "" {

		// Check if supplied worker with workerName actually is configured.
		workerConfig, ok := conf.Workers[*workerFlag]
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

		// Run an HTTP server in a goroutine to expose this worker's metrics.
		go runPromHTTP(logger, workerConfig.PrometheusAddr)

		// Create all non-existent files and folders for
		// all users this worker is responsible for.
		err := createUserFiles(workerConfig.CRDTLayerRoot, workerConfig.MaildirRoot, workerConfig.UserStart, workerConfig.UserEnd)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to create user files",
				"node", workerConfig.Name,
				"err", err,
			)
			os.Exit(1)
		}

		tlsConfig, err := crypto.NewInternalTLSConfig(workerConfig.CertLoc, workerConfig.KeyLoc, conf.RootCertLoc)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to create internal TLS config",
				"node", workerConfig.Name,
				"err", err,
			)
			os.Exit(1)
		}

		var workerS worker.Service
		workerS = worker.NewService(workerConfig.Name, logger, tlsConfig, conf)

		// Create needed synchronization socket used by gRPC.
		syncSocket, err := net.Listen("tcp", workerConfig.ListenSyncAddr)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to open synchronization socket",
				"node", workerConfig.Name,
				"err", err,
			)
			os.Exit(1)
		}
		defer syncSocket.Close()

		// Retrive name of the one synchronization subnet
		// this worker is part of.
		var subnetName string
		for subnetName = range workerConfig.Peers {
			break
		}

		// TODO: CONTINUE HERE.

		// Initialize channels for this node.
		applyCRDTUpd := make(chan comm.Msg)
		doneCRDTUpd := make(chan struct{})

		// Construct path to receiving and sending CRDT logs for storage node.
		recvCRDTLog := filepath.Join(workerConfig.CRDTLayerRoot, subnetName, "-receiving.log")
		sendCRDTLog := filepath.Join(workerConfig.CRDTLayerRoot, subnetName, "-sending.log")
		vclockLog := filepath.Join(workerConfig.CRDTLayerRoot, subnetName, "-vclock.log")

		// Initialize receiving goroutine for sync operations.
		incVClock, updVClock, err := comm.InitReceiver(logger, workerConfig.Name, workerConfig.ListenSyncAddr, workerConfig.PublicSyncAddr, recvCRDTLog, vclockLog, syncSocket, tlsConfig, applyCRDTUpd, doneCRDTUpd, []string{"storage"})
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to initialize receiver",
				"node", workerConfig.Name,
				"err", err,
			)
			os.Exit(1)
		}

		// Create subnet to distribute CRDT changes in.
		curCRDTSubnet := make(map[string]string)
		curCRDTSubnet["storage"] = conf.Storage.PublicSyncAddr

		// Init sending part of CRDT communication and send messages in background.
		syncSendChan, err := comm.InitSender(logger, workerConfig.Name, sendCRDTLog, tlsConfig, incVClock, updVClock, curCRDTSubnet)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to initialize sender",
				"node", workerConfig.Name,
				"err", err,
			)
			os.Exit(1)
		}

		// Apply CRDT updates in background.
		go workerS.ApplyCRDTUpd(applyCRDTUpd, doneCRDTUpd)

		// Run required initialization code for worker.
		err = workerS.Init(syncSendChan)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to initilize service",
				"node", workerConfig.Name,
				"err", err,
			)
			os.Exit(1)
		}

		// Create socket for gRPC IMAP connections.
		mailSocket, err := net.Listen("tcp", workerConfig.ListenMailAddr)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to open socket for proxied mail traffic",
				"node", workerConfig.Name,
				"err", err,
			)
			os.Exit(1)
		}
		defer mailSocket.Close()

		level.Info(logger).Log(
			"msg", "accepting proxied mail connections",
			"node", workerConfig.Name,
			"public_addr", workerConfig.PublicMailAddr,
			"listen_addr", workerConfig.ListenMailAddr,
		)

		// Run main handler routine on gRPC-served IMAP socket.
		err = workerS.Serve(mailSocket)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to run Serve() on IMAP gRPC socket",
				"node", workerConfig.Name,
				"err", err,
			)
		}

	} else if *storageFlag {

		// Run an HTTP server in a goroutine to expose this storage's metrics.
		go runPromHTTP(logger, conf.Storage.PrometheusAddr)

		tlsConfig, err := crypto.NewInternalTLSConfig(conf.Storage.CertLoc, conf.Storage.KeyLoc, conf.RootCertLoc)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to create internal TLS config",
				"node", conf.Storage.Name,
				"err", err,
			)
			os.Exit(1)
		}

		var storageS storage.Service
		storageS = storage.NewService(conf.Storage.Name, logger, tlsConfig, conf, conf.Workers)

		// Create needed synchronization socket used by gRPC.
		syncSocket, err := net.Listen("tcp", conf.Storage.ListenSyncAddr)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to open synchronization socket",
				"node", conf.Storage.Name,
				"err", err,
			)
			os.Exit(1)
		}
		defer syncSocket.Close()

		syncSendChans := make(map[string]chan comm.Msg)

		for name, worker := range conf.Workers {

			// Create all non-existent files and folders on
			// storage for all users the currently examined
			// worker is responsible for.
			if err := createUserFiles(conf.Storage.CRDTLayerRoot, conf.Storage.MaildirRoot, worker.UserStart, worker.UserEnd); err != nil {
				level.Error(logger).Log(
					"msg", "failed to create user files",
					"node", conf.Storage.Name,
					"err", err,
				)
				os.Exit(1)
			}

			// Initialize channels for this node.
			applyCRDTUpd := make(chan comm.Msg)
			doneCRDTUpd := make(chan struct{})

			// Construct path to receiving and sending CRDT logs for
			// current worker node.
			recvCRDTLog := filepath.Join(conf.Storage.CRDTLayerRoot, fmt.Sprintf("receiving-%s.log", name))
			sendCRDTLog := filepath.Join(conf.Storage.CRDTLayerRoot, fmt.Sprintf("sending-%s.log", name))
			vclockLog := filepath.Join(conf.Storage.CRDTLayerRoot, fmt.Sprintf("vclock-%s.log", name))

			// Initialize a receiving goroutine for sync operations
			// for each worker node.
			incVClock, updVClock, err := comm.InitReceiver(logger, conf.Storage.Name, conf.Storage.ListenSyncAddr, conf.Storage.PublicSyncAddr, recvCRDTLog, vclockLog, syncSocket, tlsConfig, applyCRDTUpd, doneCRDTUpd, []string{name})
			if err != nil {
				level.Error(logger).Log(
					"msg", "failed to initialize receiver",
					"node", conf.Storage.Name,
					"err", err,
				)
				os.Exit(1)
			}

			// Create subnet to distribute CRDT changes in.
			curCRDTSubnet := make(map[string]string)
			curCRDTSubnet[name] = worker.PublicSyncAddr

			// Init sending part of CRDT communication and send messages in background.
			syncSendChans[name], err = comm.InitSender(logger, conf.Storage.Name, sendCRDTLog, tlsConfig, incVClock, updVClock, curCRDTSubnet)
			if err != nil {
				level.Error(logger).Log(
					"msg", "failed to initialize sender",
					"node", conf.Storage.Name,
					"err", err,
				)
				os.Exit(1)
			}

			// Apply CRDT updates in background.
			go storageS.ApplyCRDTUpd(applyCRDTUpd, doneCRDTUpd)
		}

		// Run required initialization code for storage.
		err = storageS.Init(syncSendChans)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to initilize service",
				"node", conf.Storage.Name,
				"err", err,
			)
			os.Exit(1)
		}

		// Create socket for gRPC IMAP connections.
		mailSocket, err := net.Listen("tcp", conf.Storage.ListenMailAddr)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to open socket for proxied mail traffic",
				"node", conf.Storage.Name,
				"err", err,
			)
			os.Exit(1)
		}
		defer mailSocket.Close()

		level.Info(logger).Log(
			"msg", "accepting proxied mail connections",
			"node", conf.Storage.Name,
			"public_addr", conf.Storage.PublicMailAddr,
			"listen_addr", conf.Storage.ListenMailAddr,
		)

		// Run main handler routine on gRPC-served IMAP socket.
		err = storageS.Serve(mailSocket)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to run Serve() on IMAP gRPC socket",
				"node", conf.Storage.Name,
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
