package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"

	"crypto/tls"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/numbleroot/pluto/auth"
	"github.com/numbleroot/pluto/config"
	"github.com/numbleroot/pluto/crypto"
	"github.com/numbleroot/pluto/distributor"
	"github.com/numbleroot/pluto/imap"
)

// Functions

// initAuthenticator of the correct implementation specified in the config
// to be used in the imap.Distributor.
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

// Load public TLS config based on config values.
func publicDistributorConn(conf config.Distributor) (net.Listener, error) {
	tlsConfig, err := crypto.NewPublicTLSConfig(conf.PublicTLS.CertLoc, conf.PublicTLS.KeyLoc)
	if err != nil {
		return nil, err
	}

	// Start to listen for incoming public connections on defined IP and port.
	return tls.Listen("tcp", fmt.Sprintf("%s:%s", conf.ListenIP, conf.Port), tlsConfig)
}

func main() {

	var err error

	// Set CPUs usable by pluto to all available.
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Parse command-line flag that defines a config path.
	configFlag := flag.String("config", "config.toml", "Provide path to configuration file in TOML syntax.")
	distributorFlag := flag.Bool("distributor", false, "Append this flag to indicate that this process should take the role of the distributor.")
	workerFlag := flag.String("worker", "", "If this process is intended to run as one of the IMAP worker nodes, specify which of the ones defined in your config file this should be.")
	storageFlag := flag.Bool("storage", false, "Append this flag to indicate that this process should take the role of the storage node.")
	loglevelFlag := flag.String("loglevel", "debug", "This flag sets the default logging level.")
	flag.Parse()

	logger := initLogger(*loglevelFlag)

	// Read configuration from file.
	conf, err := config.LoadConfig(*configFlag)
	if err != nil {
		level.Error(logger).Log(
			"msg", "failed to load the config", "err", err,
		)
		os.Exit(1)
	}

	// Initialize and run a node of the pluto
	// system based on passed command line flag.
	if *distributorFlag {

		authenticator, err := initAuthenticator(conf)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to initialize an authenticator",
				"err", err,
			)
			os.Exit(2)
		}

		intConnectioner, err := NewInternalConnection(
			conf.Distributor.InternalTLS.CertLoc,
			conf.Distributor.InternalTLS.KeyLoc,
			conf.RootCertLoc,
			conf.IntlConnRetry,
		)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to initialize internal connectioner",
				"err", err,
			)
		}

		conn, err := publicDistributorConn(conf.Distributor)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to create public distributor connection",
				"err", err,
			)
			os.Exit(2)
		}
		defer conn.Close()

		ds := distributor.NewService(authenticator, intConnectioner, conf.Workers)

		if err := ds.Run(conn, conf.IMAP.Greeting); err != nil {
			level.Error(logger).Log(
				"msg", "failed to run distributor",
				"err", err,
			)
		}
	} else if *workerFlag != "" {

		// Initialize a worker.
		worker, err := imap.InitWorker(logger, conf, *workerFlag)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to initialize imap worker",
				"err", err,
			)
			os.Exit(5)
		}
		defer worker.MailSocket.Close()
		defer worker.SyncSocket.Close()

		// Loop on incoming requests.
		err = worker.Run()
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to start the initialized worker node",
				"err", err,
			)
			os.Exit(6)
		}
	} else if *storageFlag {

		// Initialize storage.
		storage, err := imap.InitStorage(conf)
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to initialize imap storage node",
				"err", err,
			)
			os.Exit(7)
		}
		defer storage.MailSocket.Close()
		defer storage.SyncSocket.Close()

		// Loop on incoming requests.
		err = storage.Run()
		if err != nil {
			level.Error(logger).Log(
				"msg", "failed to start the initialized storage node",
				"err", err,
			)
			os.Exit(8)
		}
	} else {
		// If no flags were specified, print usage
		// and return with failure value.
		flag.Usage()
		os.Exit(9)
	}
}
