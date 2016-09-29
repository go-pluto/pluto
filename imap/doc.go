/*
Package imap implements the IMAP state machine that all IMAP connections go through.

The following four states exist:
    * not authenticated
    * authenticated
    * mailbox
    * logout

Please refer to https://tools.ietf.org/html/rfc3501#section-3 for full documentation
on the states and https://tools.ietf.org/html/rfc3501 for the full IMAP v4 rev1 RFC.
*/
package imap
