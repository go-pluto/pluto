package config

import (
	"fmt"

	"github.com/BurntSushi/toml"
)

// Structs

// Config holds all information parsed from
// supplied config file.
type Config struct {
	IP   string
	Port string
	TLS  TLS
	IMAP IMAP
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
	Auth     Auth
}

// Auth stores the system's facility to identify
// user sessions, i.e. log in a user or deny access.
type Auth struct {
	Adaptor  string
	IP       string
	Port     string
	Database string
	User     string
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
