package auth

import (
	"fmt"

	"crypto/tls"

	"github.com/numbleroot/pluto/config"
	"gopkg.in/jackc/pgx.v2"
)

// Structs

// PostgresAuthenticator carries all relevant information
// needed to allow the PostgreSQL-based authenticator to
// properly authenticate incoming client requests.
type PostgresAuthenticator struct {
	Conn *pgx.Conn
}

// Functions

// NewPostgresAuthenticator expects to be supplied with
// PostgreSQL database connection information from the
// config file. It then tries to connect to the database
// and returns an initialized struct above.
func NewPostgresAuthenticator(ip string, port uint16, db string, user string, password string, useTLS bool) (*PostgresAuthenticator, error) {

	// Prepare a default TLS config if useTLS is set to true.
	// Otherwise, this config will be nil and therefore disable TLS.
	var dbTLSConfig *tls.Config
	if useTLS {
		dbTLSConfig = new(tls.Config)
	}

	// Create a new connection config using the imported pgx drivers.
	connConfig := pgx.ConnConfig{
		Host:           ip,
		Port:           port,
		Database:       db,
		User:           user,
		Password:       password,
		TLSConfig:      dbTLSConfig,
		UseFallbackTLS: false,
	}

	// Connect to PostgreSQL database based on above config.
	conn, err := pgx.Connect(connConfig)
	if err != nil {
		return nil, fmt.Errorf("[auth.NewPostgresAuthenticator] Could not connect to specified PostgreSQL database: %s\n", err.Error())
	}

	return &PostgresAuthenticator{
		Conn: conn,
	}, nil
}

// GetWorkerForUser returns the name of the worker node
// that is responsible for handling the user's mailbox.
func (p *PostgresAuthenticator) GetWorkerForUser(workers map[string]config.Worker, id int) (string, error) {

	for name, worker := range workers {

		// Range over all available workers and see which worker
		// is responsible for the range of user IDs that contains
		// the supplied user ID.
		if id >= worker.UserStart && id <= worker.UserEnd {
			return name, nil
		}
	}

	return "", fmt.Errorf("no worker responsible for user ID %d", id)
}

// AuthenticatePlain is used to perform the actual process
// of looking up if the client supplied user credentials exist
// and match with an user entry in the PostgreSQL database.
func (p *PostgresAuthenticator) AuthenticatePlain(username string, password string, clientAddr string) (int, string, error) {

	return 0, "", nil
}
