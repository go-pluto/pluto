/*
Package imap implements all three node types a pluto setup consists of: distributor, worker,
and storage.

Handler functions for the various implemented IMAP commands usually return a boolean value
indicating whether correct communication between pluto and connected clients was achieved
but not whether commands were handled correctly according to IMAP semantics. This means that
if a fatal error occurred during handling e.g. a LOGIN request which prevents the system with
a high probability from handling future commands correctly as well, the responsible handler
function will return false. But in case an user error occurred such as a missing name and/or
password accompanying the LOGIN command and pluto was able to send back a useful error message
to the client, this function returns true because communication went according to planned
handling pipeline.

Please refer to https://tools.ietf.org/html/rfc3501#section-3 for full documentation
on the states and https://tools.ietf.org/html/rfc3501 for the full IMAP v4 rev1 RFC.
*/
package imap
