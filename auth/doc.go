/*
Package auth defines potentially multiple mechanisms to determine whether supplied
user credentials in an IMAP session can be found in a defined user information system.
Examples include an authenticator based on a user table in a PostgreSQL database and
a simple (potentially insecure) plain user text file. It is easily possible to implement
new authenticators that fit specific requirements.
*/
package auth
