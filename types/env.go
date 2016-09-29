package types

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
