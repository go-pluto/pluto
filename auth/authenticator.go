package auth

// Interfaces

// PlainAuthenticator defines the methods required to
// perform an IMAP AUTH=PLAIN authentication in order
// to reach authenticated state.
type PlainAuthenticator interface {
	AuthenticatePlain(username string, password string) error
}
