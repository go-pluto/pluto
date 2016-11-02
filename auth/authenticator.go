package auth

import (
	"github.com/numbleroot/pluto/config"
)

// Interfaces

// PlainAuthenticator defines the methods required to
// perform an IMAP AUTH=PLAIN authentication in order
// to reach authenticated state (also LOGIN).
type PlainAuthenticator interface {

	// Each authenticator of this interface needs to implement
	// a function that returns an ID notion for a given user.
	GetOriginalIDOfUser(username string) int

	// To be able to route an IMAP request to the responsible
	// worker node we need to be able to tell which of them it is.
	GetWorkerForUser(workers map[string]config.Worker, id int) (*string, error)

	// AuthenticatePlain will be implemented by each of the
	// authentication methods of type PLAIN to perform the
	// actual part of checking supplied credentials.
	AuthenticatePlain(username string, password string) (int, error)
}
