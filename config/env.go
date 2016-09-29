package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

// Structs

// Env holds information specific to the
// system where pluto is deployed. This
// enables host adaptions without needing
// to maintain two different config files.
// Use the .env file to populate secrets
// within the system.
type Env struct {
	Secret string
}

// Functions

// LoadEnv looks for an .env file in the directory
// of pluto and reads in all defined values.
func LoadEnv() *Env {

	// Load environment file.
	err := godotenv.Load()
	if err != nil {
		log.Fatal("[config.LoadEnv] Failed to read in .env file with: %s\n", err.Error())
	}

	env := new(Env)

	// Fill variables from .env into struct.
	env.Secret = os.Getenv("SECRET")

	return env
}
