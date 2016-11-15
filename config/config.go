package config

import (
	"fmt"

	"github.com/BurntSushi/toml"
)

// Structs

// Config holds all information parsed from
// supplied config file.
type Config struct {
	RootCertLoc string
	Distributor Distributor
	Workers     map[string]Worker
	Storage     Storage
}

// Distributor describes the configuration of
// the first entry point of a pluto setup, the
// IMAP request authenticator and distributor.
type Distributor struct {
	IP             string
	Port           string
	AuthAdapter    string
	PublicTLS      TLS
	InternalTLS    TLS
	IMAP           IMAP
	AuthFile       *AuthFile
	AuthPostgreSQL *AuthPostgreSQL
}

// Worker contains the connection and user sharding
// information for an individual IMAP worker node.
type Worker struct {
	IP            string
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
	IP       string
	SyncPort string
	TLS      TLS
}

// TLS contains Transport Layer Security relevant
// parameters. Use this to provide paths to your
// TLS certificate and key.
type TLS struct {
	CertLoc string
	KeyLoc  string
}

// IMAP is the IMAP server related part
// of the TOML config file.
type IMAP struct {
	Greeting string
}

// AuthPostgreSQL defines parameters for connecting
// to a PostgreSQL database for authenticating users.
type AuthPostgreSQL struct {
	IP       string
	Port     string
	Database string
	User     string
	SSLMode  string
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
		return nil, fmt.Errorf("[config.LoadConfig] Failed to read in TOML config file at '%s' with: %s\n", configFile, err.Error())
	}

	return conf, nil
}
