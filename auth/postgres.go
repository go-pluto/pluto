package auth

import (
	"fmt"

	"crypto/sha512"
	"crypto/tls"
	"encoding/base64"

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

	var dbUserID int

	// Create new SHA512 hash.
	shaHash := sha512.New()

	// Input supplied password into hash function.
	_, err := shaHash.Write([]byte(password))
	if err != nil {
		return -1, "", fmt.Errorf("failed to write password to hash: %s", err.Error())
	}

	// Produce the actual hash and save it.
	hashedPassword := shaHash.Sum(nil)

	// Encode the hashed text with base64.
	encHashedPassword := base64.StdEncoding.EncodeToString(hashedPassword)

	// Prepare query for database.
	query := fmt.Sprintf("SELECT id FROM users WHERE username = '%s' AND password = '{SHA512}%s'", username, encHashedPassword)

	// Query database for user matching all criteria.
	err = p.Conn.QueryRow(query).Scan(&dbUserID)
	if err != nil {

		// Check what type of error we received.
		if err == pgx.ErrNoRows {
			return -1, "", fmt.Errorf("username not found in users table or password wrong")
		}

		return -1, "", fmt.Errorf("error while trying to locate user: %s", err.Error())
	}

	// Build the deterministic client-specific session identifier.
	clientID := fmt.Sprintf("%s:%s", clientAddr, username)

	return dbUserID, clientID, nil
}
