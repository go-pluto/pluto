// Package types holds all structs used in pluto
// in one place accessible to the rest of the system
// via a simple import.
package types

// Structs

// Config holds all information
// parsed from supplied config file.
type Config struct {
	IP   string
	Port string
}
