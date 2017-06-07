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
	"github.com/numbleroot/maildir"
	"github.com/numbleroot/pluto/auth"
	"github.com/numbleroot/pluto/comm"
	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/crypto"
	"github.com/numbleroot/pluto/distributor"
	"github.com/numbleroot/pluto/storage"
	"github.com/numbleroot/pluto/worker"
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
		"caller", log.DefaultCaller,
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

// publicDistributorConn listens on config supplied socket
// for incoming public TLS connections to the distributor.
func publicDistributorConn(conf config.Distributor) (net.Listener, error) {

	// Load public TLS config based on config values.
	tlsConfig, err := crypto.NewPublicTLSConfig(conf.PublicTLS.CertLoc, conf.PublicTLS.KeyLoc)
	if err != nil {
		return nil, err
	}

	return tls.Listen("tcp", fmt.Sprintf("%s:%s", conf.ListenIP, conf.Port), tlsConfig)
}

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
		if err := os.MkdirAll(crdtFolder, 0755); err != nil {
			return err
		}

		inboxLog := filepath.Join(crdtFolder, "INBOX.log")
		mailboxStructureLog := filepath.Join(crdtFolder, "mailbox-structure.log")

		if !exists(inboxLog) {
			if err := ioutil.WriteFile(inboxLog, nil, 0644); err != nil {
				return err
			}
		}

		if !exists(mailboxStructureLog) {
			data := "SU5CT1g=;" + uuid.NewV4().String() + "\n"
			if err := ioutil.WriteFile(mailboxStructureLog, []byte(data), 0644); err != nil {
				return err
			}
		}
	}

	return nil
}

// exists returns true if a file exists
func exists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			// not found
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
		go runPromHTTP(conf.Distributor.PrometheusAddr)

		auther, err := initAuthenticator(conf)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to initialize an authenticator",
				"err", err,
			)
			os.Exit(1)
		}

		publicTLSConfig, err := crypto.NewPublicTLSConfig(conf.Distributor.PublicTLS.CertLoc, conf.Distributor.PublicTLS.KeyLoc)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to create public TLS config for distributor",
				"err", err,
			)
			os.Exit(1)
		}

		mailSocket, err := tls.Listen("tcp", fmt.Sprintf("%s:%s", conf.Distributor.ListenIP, conf.Distributor.Port), publicTLSConfig)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to listen for public mail TLS connections on distributor",
				"err", err,
			)
			os.Exit(1)
		}
		defer mailSocket.Close()

		level.Info(logger).Log(
			"msg", fmt.Sprintf("distributor is accepting public mail connections at %s", fmt.Sprintf("%s:%s", conf.Distributor.ListenIP, conf.Distributor.Port)),
		)

		intlTLSConfig, err := crypto.NewInternalTLSConfig(conf.Distributor.InternalTLS.CertLoc, conf.Distributor.InternalTLS.KeyLoc, conf.RootCertLoc)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to create internal TLS config for distributor",
				"err", err,
			)
			os.Exit(1)
		}

		var distrS distributor.Service
		distrS = distributor.NewService(auther, &intlConn{intlTLSConfig, conf.IntlConnRetry}, conf.Workers)
		distrS = distributor.NewLoggingService(distrS, logger)
		distrS = distributor.NewMetricsService(distrS, plutoMetrics.Distributor.Logins, plutoMetrics.Distributor.Logouts)

		if err := distrS.Run(mailSocket, conf.IMAP.Greeting); err != nil {
			level.Error(logger).Log(
				"msg", "failed to run distributor",
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

		if err := createUserFiles(workerConfig.CRDTLayerRoot, workerConfig.MaildirRoot, workerConfig.UserStart, workerConfig.UserEnd); err != nil {
			level.Error(logger).Log(
				"msg", "failed to create user files",
				"err", err,
			)
			os.Exit(1)
		}

		tlsConfig, err := crypto.NewInternalTLSConfig(workerConfig.TLS.CertLoc, workerConfig.TLS.KeyLoc, conf.RootCertLoc)
		if err != nil {
			level.Error(logger).Log(
				"msg", fmt.Sprintf("failed to create internal TLS config for %s", *workerFlag),
				"err", err,
			)
			os.Exit(1)
		}

		// Create needed sockets. First, mail socket.
		mailSocket, err := tls.Listen("tcp", fmt.Sprintf("%s:%s", workerConfig.ListenIP, workerConfig.MailPort), tlsConfig)
		if err != nil {
			level.Error(logger).Log(
				"msg", fmt.Sprintf("failed to listen for mail TLS connections on %s", *workerFlag),
				"err", err,
			)
			os.Exit(1)
		}
		defer mailSocket.Close()

		level.Info(logger).Log(
			"msg", fmt.Sprintf("%s is accepting mail connections at %s", *workerFlag, fmt.Sprintf("%s:%s", workerConfig.ListenIP, workerConfig.MailPort)),
		)

		// Second, synchronization socket later used by gRPC.
		syncSocket, err := tls.Listen("tcp", fmt.Sprintf("%s:%s", workerConfig.ListenIP, workerConfig.SyncPort), tlsConfig)
		if err != nil {
			level.Error(logger).Log(
				"msg", fmt.Sprintf("failed to listen for synchronization TLS connections on %s", *workerFlag),
				"err", err,
			)
			os.Exit(1)
		}
		defer syncSocket.Close()

		var workerS worker.Service
		workerS = worker.NewService(&intlConn{tlsConfig, conf.IntlConnRetry}, mailSocket, workerConfig, *workerFlag)
		workerS = worker.NewLoggingService(workerS, logger)

		// Initialize channels for this node.
		applyCRDTUpd := make(chan string)
		doneCRDTUpd := make(chan struct{})
		downRecv := make(chan struct{})
		downSender := make(chan struct{})

		// Construct path to receiving and sending CRDT logs for storage node.
		recvCRDTLog := filepath.Join(workerConfig.CRDTLayerRoot, "receiving.log")
		sendCRDTLog := filepath.Join(workerConfig.CRDTLayerRoot, "sending.log")
		vclockLog := filepath.Join(workerConfig.CRDTLayerRoot, "vclock.log")

		// Initialize receiving goroutine for sync operations.
		chanIncVClockWorker, chanUpdVClockWorker, err := comm.InitReceiver(*workerFlag, recvCRDTLog, vclockLog, syncSocket, tlsConfig, applyCRDTUpd, doneCRDTUpd, downRecv, []string{"storage"})
		if err != nil {
			level.Error(logger).Log(
				"msg", fmt.Sprintf("failed to initialize receiver for %s", *workerFlag),
				"err", err,
			)
			os.Exit(1)
		}

		// Create subnet to distribute CRDT changes in.
		curCRDTSubnet := make(map[string]string)
		curCRDTSubnet["storage"] = fmt.Sprintf("%s:%s", conf.Storage.PublicIP, conf.Storage.SyncPort)

		// Init sending part of CRDT communication and send messages in background.
		syncSendChan, err := comm.InitSender(*workerFlag, sendCRDTLog, tlsConfig, conf.IntlConnTimeout, conf.IntlConnRetry, chanIncVClockWorker, chanUpdVClockWorker, downSender, curCRDTSubnet)
		if err != nil {
			level.Error(logger).Log(
				"msg", fmt.Sprintf("failed to initialize sender for %s", *workerFlag),
				"err", err,
			)
			os.Exit(1)
		}

		err = workerS.Init(syncSendChan)
		if err != nil {
			level.Error(logger).Log(
				"msg", fmt.Sprintf("failed to initilize service of %s", *workerFlag),
				"err", err,
			)
			os.Exit(1)
		}

		// Apply CRDT updates in background.
		go workerS.ApplyCRDTUpd(applyCRDTUpd, doneCRDTUpd)

		if err := workerS.Run(); err != nil {
			level.Error(logger).Log(
				"msg", "failed to run worker",
				"err", err,
			)
		}
	} else if *storageFlag {

		tlsConfig, err := crypto.NewInternalTLSConfig(conf.Storage.TLS.CertLoc, conf.Storage.TLS.KeyLoc, conf.RootCertLoc)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to create internal TLS config for storage",
				"err", err,
			)
			os.Exit(1)
		}

		// Create needed sockets. First, mail socket.
		mailSocket, err := tls.Listen("tcp", fmt.Sprintf("%s:%s", conf.Storage.ListenIP, conf.Storage.MailPort), tlsConfig)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to listen for mail TLS connections on storage",
				"err", err,
			)
			os.Exit(1)
		}
		defer mailSocket.Close()

		level.Info(logger).Log(
			"msg", fmt.Sprintf("storage is accepting mail connections at %s", fmt.Sprintf("%s:%s", conf.Storage.ListenIP, conf.Storage.MailPort)),
		)

		// Second, synchronization socket later used by gRPC.
		syncSocket, err := tls.Listen("tcp", fmt.Sprintf("%s:%s", conf.Storage.ListenIP, conf.Storage.SyncPort), tlsConfig)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to listen for synchronization TLS connections on storage",
				"err", err,
			)
			os.Exit(1)
		}
		defer syncSocket.Close()

		var storageS storage.Service
		storageS = storage.NewService(&intlConn{tlsConfig, conf.IntlConnRetry}, mailSocket, conf.Storage, conf.Workers)
		storageS = storage.NewLoggingService(storageS, logger)

		syncSendChans := make(map[string]chan string)

		for name, worker := range conf.Workers {

			// Initialize channels for this node.
			applyCRDTUpd := make(chan string)
			doneCRDTUpd := make(chan struct{})
			downRecv := make(chan struct{})
			downSender := make(chan struct{})

			// Construct path to receiving and sending CRDT logs for
			// current worker node.
			recvCRDTLog := filepath.Join(conf.Storage.CRDTLayerRoot, fmt.Sprintf("receiving-%s.log", name))
			sendCRDTLog := filepath.Join(conf.Storage.CRDTLayerRoot, fmt.Sprintf("sending-%s.log", name))
			vclockLog := filepath.Join(conf.Storage.CRDTLayerRoot, fmt.Sprintf("vclock-%s.log", name))

			// Initialize a receiving goroutine for sync operations
			// for each worker node.
			chanIncVClockWorker, chanUpdVClockWorker, err := comm.InitReceiver("storage", recvCRDTLog, vclockLog, syncSocket, tlsConfig, applyCRDTUpd, doneCRDTUpd, downRecv, []string{name})
			if err != nil {
				level.Error(logger).Log(
					"msg", "failed to initialize receiver for storage",
					"err", err,
				)
				os.Exit(1)
			}

			// Create subnet to distribute CRDT changes in.
			curCRDTSubnet := make(map[string]string)
			curCRDTSubnet[name] = fmt.Sprintf("%s:%s", worker.PublicIP, worker.SyncPort)

			// Init sending part of CRDT communication and send messages in background.
			syncSendChans[name], err = comm.InitSender("storage", sendCRDTLog, tlsConfig, conf.IntlConnTimeout, conf.IntlConnRetry, chanIncVClockWorker, chanUpdVClockWorker, downSender, curCRDTSubnet)
			if err != nil {
				level.Error(logger).Log(
					"msg", "failed to initialize sender for storage",
					"err", err,
				)
				os.Exit(1)
			}

			err = storageS.Init(syncSendChans)
			if err != nil {
				level.Error(logger).Log(
					"msg", "failed to initilize service of storage",
					"err", err,
				)
				os.Exit(1)
			}

			// Apply CRDT updates in background.
			go storageS.ApplyCRDTUpd(applyCRDTUpd, doneCRDTUpd)
		}

		if err := storageS.Run(); err != nil {
			level.Error(logger).Log(
				"msg", "failed to run storage",
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
