// Package config provides functions to read in
// various configuration files into definded types.
package config

import (
	"log"

	"github.com/BurntSushi/toml"
	"github.com/numbleroot/pluto/types"
)

// Functions

// LoadConfig takes in the path to the main config
// file of pluto in TOML syntax and places the values
// from the file in the corresponding struct.
func LoadConfig(configFile string) *types.Config {

	conf := new(types.Config)

	// Parse values from TOML file into struct.
	if _, err := toml.DecodeFile(configFile, conf); err != nil {
		log.Fatalf("[config.LoadConfig] Failed to read in TOML config file at '%s' with: %s\n", configFile, err)
	}

	return conf
}
