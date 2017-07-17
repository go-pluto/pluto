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
	RootCertLoc string
	IMAP        IMAP
	Distributor Distributor
	Workers     map[string]Worker
	Storage     Storage
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
	Name            string
	PublicMailAddr  string
	ListenMailAddr  string
	PrometheusAddr  string
	PublicCertLoc   string
	PublicKeyLoc    string
	InternalCertLoc string
	InternalKeyLoc  string
	AuthAdapter     string
	AuthFile        *AuthFile
	AuthPostgres    *AuthPostgres
}

// Worker contains the connection and user sharding
// information for an individual IMAP worker node.
type Worker struct {
	Name           string
	PublicMailAddr string
	ListenMailAddr string
	PublicSyncAddr string
	ListenSyncAddr string
	PrometheusAddr string
	CertLoc        string
	KeyLoc         string
	UserStart      int
	UserEnd        int
	MaildirRoot    string
	CRDTLayerRoot  string
	Peers          map[string]map[string]string
}

// Storage configures the global database node
// storing all user data in a very safe manner.
type Storage struct {
	Name           string
	PublicMailAddr string
	ListenMailAddr string
	PublicSyncAddr string
	ListenSyncAddr string
	PrometheusAddr string
	CertLoc        string
	KeyLoc         string
	MaildirRoot    string
	CRDTLayerRoot  string
	Peers          map[string]map[string]string
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
	_, err := toml.DecodeFile(configFile, conf)
	if err != nil {
		return nil, fmt.Errorf("failed to read in TOML config file at '%s' with: %v", configFile, err)
	}

	// Make sure each worker is only at most part
	// of one synchronization subnet.
	for _, worker := range conf.Workers {

		if len(worker.Peers) > 1 {
			return nil, fmt.Errorf("worker cannot be part of more than one synchronization subnet")
		}
	}

	// Retrieve absolute path of pluto directory.
	// Start with current directory.
	absPlutoPath, err := filepath.Abs("./")
	if err != nil {
		return nil, fmt.Errorf("could not get absolute path of current directory: %v", err)
	}

	// Check if path ends in 'pluto'.
	if strings.HasSuffix(absPlutoPath, "pluto") != true {

		// If not, use the directory one level above.
		absPlutoPath, err = filepath.Abs("../")
		if err != nil {
			return nil, fmt.Errorf("could not get absolute path of pluto directory: %v", err)
		}
	}

	// Prefix each relative path in config with
	// just obtained absolute path to pluto directory.

	// RootCertLoc
	if filepath.IsAbs(conf.RootCertLoc) != true {
		conf.RootCertLoc = filepath.Join(absPlutoPath, conf.RootCertLoc)
	}

	// Distributor.PublicCertLoc
	if filepath.IsAbs(conf.Distributor.PublicCertLoc) != true {
		conf.Distributor.PublicCertLoc = filepath.Join(absPlutoPath, conf.Distributor.PublicCertLoc)
	}

	// Distributor.PublicKeyLoc
	if filepath.IsAbs(conf.Distributor.PublicKeyLoc) != true {
		conf.Distributor.PublicKeyLoc = filepath.Join(absPlutoPath, conf.Distributor.PublicKeyLoc)
	}

	// Distributor.InternalCertLoc
	if filepath.IsAbs(conf.Distributor.InternalCertLoc) != true {
		conf.Distributor.InternalCertLoc = filepath.Join(absPlutoPath, conf.Distributor.InternalCertLoc)
	}

	// Distributor.InternalKeyLoc
	if filepath.IsAbs(conf.Distributor.InternalKeyLoc) != true {
		conf.Distributor.InternalKeyLoc = filepath.Join(absPlutoPath, conf.Distributor.InternalKeyLoc)
	}

	if conf.Distributor.AuthAdapter == "AuthFile" {

		// Distributor.AuthFile.File
		if filepath.IsAbs(conf.Distributor.AuthFile.File) != true {
			conf.Distributor.AuthFile.File = filepath.Join(absPlutoPath, conf.Distributor.AuthFile.File)
		}
	}

	for name, worker := range conf.Workers {

		// Workers[worker].CertLoc
		if filepath.IsAbs(worker.CertLoc) != true {
			worker.CertLoc = filepath.Join(absPlutoPath, worker.CertLoc)
		}

		// Workers[worker].KeyLoc
		if filepath.IsAbs(worker.KeyLoc) != true {
			worker.KeyLoc = filepath.Join(absPlutoPath, worker.KeyLoc)
		}

		// Workers[worker].MaildirRoot
		if filepath.IsAbs(worker.MaildirRoot) != true {
			worker.MaildirRoot = filepath.Join(absPlutoPath, worker.MaildirRoot)
		}

		// Workers[worker].CRDTLayerRoot
		if filepath.IsAbs(worker.CRDTLayerRoot) != true {
			worker.CRDTLayerRoot = filepath.Join(absPlutoPath, worker.CRDTLayerRoot)
		}

		// Assign worker config back to main config.
		delete(conf.Workers, name)
		conf.Workers[worker.Name] = worker
	}

	// Storage.CertLoc
	if filepath.IsAbs(conf.Storage.CertLoc) != true {
		conf.Storage.CertLoc = filepath.Join(absPlutoPath, conf.Storage.CertLoc)
	}

	// Storage.KeyLoc
	if filepath.IsAbs(conf.Storage.KeyLoc) != true {
		conf.Storage.KeyLoc = filepath.Join(absPlutoPath, conf.Storage.KeyLoc)
	}

	// Storage.MaildirRoot
	if filepath.IsAbs(conf.Storage.MaildirRoot) != true {
		conf.Storage.MaildirRoot = filepath.Join(absPlutoPath, conf.Storage.MaildirRoot)
	}

	// Storage.CRDTLayerRoot
	if filepath.IsAbs(conf.Storage.CRDTLayerRoot) != true {
		conf.Storage.CRDTLayerRoot = filepath.Join(absPlutoPath, conf.Storage.CRDTLayerRoot)
	}

	return conf, nil
}
