package config

import (
	"log"

	"github.com/BurntSushi/toml"
)

// Structs

// Config holds all information
// parsed from supplied config file.
type Config struct {
	IP   string
	Port string
}

// Functions

// LoadConfig takes in the path to the main config
// file of pluto in TOML syntax and places the values
// from the file in the corresponding struct.
func LoadConfig(configFile string) *Config {

	conf := new(Config)

	// Parse values from TOML file into struct.
	if _, err := toml.DecodeFile(configFile, conf); err != nil {
		log.Fatalf("[config.LoadConfig] Failed to read in TOML config file at '%s' with: %s\n", configFile, err.Error())
	}

	return conf
}
