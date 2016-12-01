package auth

import (
	"github.com/numbleroot/pluto/config"
)

// Interfaces

// PlainAuthenticator defines the methods required to
// perform an IMAP AUTH=PLAIN authentication in order
// to reach authenticated state (also LOGIN).
type PlainAuthenticator interface {

	// GetWorkerForUser allows us to route an IMAP request to the
	// worker node responsible for a specific user.
	GetWorkerForUser(workers map[string]config.Worker, id int) (string, error)

	// AuthenticatePlain will be implemented by each of the
	// authentication methods of type PLAIN to perform the
	// actual part of checking supplied credentials.
	AuthenticatePlain(username string, password string, clientAddr string) (int, string, error)
}
