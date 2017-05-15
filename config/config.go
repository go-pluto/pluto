package config

import (
	"fmt"
	"strings"

	"path/filepath"

	"github.com/BurntSushi/toml"
)

// Structs

// Config holds all information parsed from
// supplied config file.
type Config struct {
	RootCertLoc     string
	IntlConnTimeout int
	IntlConnRetry   int
	IMAP            IMAP
	Distributor     Distributor
	Workers         map[string]Worker
	Storage         Storage
}

// IMAP is the IMAP server related part
// of the TOML config file.
type IMAP struct {
	Greeting           string
	HierarchySeparator string
}

// Distributor describes the configuration of
// the first entry point of a pluto setup, the
// IMAP request authenticator and distributor.
type Distributor struct {
	PublicIP     string
	ListenIP     string
	Port         string
	AuthAdapter  string
	PublicTLS    TLS
	InternalTLS  TLS
	AuthFile     *AuthFile
	AuthPostgres *AuthPostgres
}

// Worker contains the connection and user sharding
// information for an individual IMAP worker node.
type Worker struct {
	PublicIP      string
	ListenIP      string
	MailPort      string
	SyncPort      string
	UserStart     int
	UserEnd       int
	MaildirRoot   string
	CRDTLayerRoot string
	TLS           TLS
}

// Storage configures the global database node
// storing all user data in a very safe manner.
type Storage struct {
	PublicIP      string
	ListenIP      string
	MailPort      string
	SyncPort      string
	MaildirRoot   string
	CRDTLayerRoot string
	TLS           TLS
}

// TLS contains Transport Layer Security relevant
// parameters. Use this to provide paths to your
// TLS certificate and key.
type TLS struct {
	CertLoc string
	KeyLoc  string
}

// AuthPostgres defines parameters for connecting
// to a Postgres database for authenticating users.
type AuthPostgres struct {
	IP       string
	Port     uint16
	Database string
	User     string
	Password string
	UseTLS   bool
}

// AuthFile provides information on authenticating
// user taken from a designated authorization text file.
type AuthFile struct {
	File      string
	Separator string
}

// Functions

// LoadConfig takes in the path to the main config
// file of pluto in TOML syntax and places the values
// from the file in the corresponding struct.
func LoadConfig(configFile string) (*Config, error) {

	conf := new(Config)

	// Parse values from TOML file into struct.
	if _, err := toml.DecodeFile(configFile, conf); err != nil {
		return nil, fmt.Errorf("[config.LoadConfig] Failed to read in TOML config file at '%s' with: %v", configFile, err)
	}

	// Retrieve absolute path of pluto directory.
	// Start with current directory.
	absPlutoPath, err := filepath.Abs("./")
	if err != nil {
		return nil, fmt.Errorf("[config.LoadConfig] Could not get absolute path of current directory: %v", err)
	}

	// Check if path ends in 'pluto'.
	if strings.HasSuffix(absPlutoPath, "pluto") != true {

		// If not, use the directory one level above.
		absPlutoPath, err = filepath.Abs("../")
		if err != nil {
			return nil, fmt.Errorf("[config.LoadConfig] Could not get absolute path of pluto directory: %v", err)
		}
	}

	// Prefix each relative path in config with
	// just obtained absolute path to pluto directory.

	// RootCertLoc
	if filepath.IsAbs(conf.RootCertLoc) != true {
		conf.RootCertLoc = filepath.Join(absPlutoPath, conf.RootCertLoc)
	}

	// Distributor.PublicTLS.CertLoc
	if filepath.IsAbs(conf.Distributor.PublicTLS.CertLoc) != true {
		conf.Distributor.PublicTLS.CertLoc = filepath.Join(absPlutoPath, conf.Distributor.PublicTLS.CertLoc)
	}

	// Distributor.PublicTLS.KeyLoc
	if filepath.IsAbs(conf.Distributor.PublicTLS.KeyLoc) != true {
		conf.Distributor.PublicTLS.KeyLoc = filepath.Join(absPlutoPath, conf.Distributor.PublicTLS.KeyLoc)
	}

	// Distributor.InternalTLS.CertLoc
	if filepath.IsAbs(conf.Distributor.InternalTLS.CertLoc) != true {
		conf.Distributor.InternalTLS.CertLoc = filepath.Join(absPlutoPath, conf.Distributor.InternalTLS.CertLoc)
	}

	// Distributor.InternalTLS.KeyLoc
	if filepath.IsAbs(conf.Distributor.InternalTLS.KeyLoc) != true {
		conf.Distributor.InternalTLS.KeyLoc = filepath.Join(absPlutoPath, conf.Distributor.InternalTLS.KeyLoc)
	}

	if conf.Distributor.AuthAdapter == "AuthFile" {

		// Distributor.AuthFile.File
		if filepath.IsAbs(conf.Distributor.AuthFile.File) != true {
			conf.Distributor.AuthFile.File = filepath.Join(absPlutoPath, conf.Distributor.AuthFile.File)
		}
	}

	for name, worker := range conf.Workers {

		// Workers[worker].MaildirRoot
		if filepath.IsAbs(worker.MaildirRoot) != true {
			worker.MaildirRoot = filepath.Join(absPlutoPath, worker.MaildirRoot)
		}

		// Workers[worker].CRDTLayerRoot
		if filepath.IsAbs(worker.CRDTLayerRoot) != true {
			worker.CRDTLayerRoot = filepath.Join(absPlutoPath, worker.CRDTLayerRoot)
		}

		// Workers[worker].TLS.CertLoc
		if filepath.IsAbs(worker.TLS.CertLoc) != true {
			worker.TLS.CertLoc = filepath.Join(absPlutoPath, worker.TLS.CertLoc)
		}

		// Workers[worker].TLS.KeyLoc
		if filepath.IsAbs(worker.TLS.KeyLoc) != true {
			worker.TLS.KeyLoc = filepath.Join(absPlutoPath, worker.TLS.KeyLoc)
		}

		// Assign worker config back to main config.
		conf.Workers[name] = worker
	}

	// Storage.MaildirRoot
	if filepath.IsAbs(conf.Storage.MaildirRoot) != true {
		conf.Storage.MaildirRoot = filepath.Join(absPlutoPath, conf.Storage.MaildirRoot)
	}

	// Storage.CRDTLayerRoot
	if filepath.IsAbs(conf.Storage.CRDTLayerRoot) != true {
		conf.Storage.CRDTLayerRoot = filepath.Join(absPlutoPath, conf.Storage.CRDTLayerRoot)
	}

	// Storage.TLS.CertLoc
	if filepath.IsAbs(conf.Storage.TLS.CertLoc) != true {
		conf.Storage.TLS.CertLoc = filepath.Join(absPlutoPath, conf.Storage.TLS.CertLoc)
	}

	// Storage.TLS.KeyLoc
	if filepath.IsAbs(conf.Storage.TLS.KeyLoc) != true {
		conf.Storage.TLS.KeyLoc = filepath.Join(absPlutoPath, conf.Storage.TLS.KeyLoc)
	}

	return conf, nil
}
