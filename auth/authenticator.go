package auth

// Interfaces

// PlainAuthenticator defines the methods required to
// perform an IMAP AUTH=PLAIN authentication in order
// to reach authenticated state (also LOGIN).
type PlainAuthenticator interface {

	// Each authenticator of this interface needs to implement
	// a function that returns an ID notion for a given user.
	GetOriginalIDOfUser(username string) int

	// Using this function, we can look up the random token
	// assigned to the user's session.
	GetTokenOfUser(username string) string

	// With this function an exisiting token gets set to the
	// empty string again, effectively logging the user out.
	DeleteTokenOfUser(id int)

	// AuthenticatePlain will be implemented by each of the
	// authentication methods of type PLAIN to perform the
	// actual part of checking supplied credentials.
	AuthenticatePlain(username string, password string) (*int, *string, error)
}
