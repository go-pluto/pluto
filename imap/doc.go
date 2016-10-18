/*
Package imap implements the IMAP state machine that all IMAP connections go through.
It also provides control structures for all different node types this state machine
depends on, namely the distributor, worker and storage node.

The following four states exist:
    * not authenticated
    * authenticated
    * mailbox
    * logout

Please refer to https://tools.ietf.org/html/rfc3501#section-3 for full documentation
on the states and https://tools.ietf.org/html/rfc3501 for the full IMAP v4 rev1 RFC.
*/
package imap
