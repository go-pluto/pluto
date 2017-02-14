/*
Package comm implements network communication capabilities that are reliable and
causally-ordered among multiple nodes. Vector clocks are used to ensure causality.
Currently, communication is blocking on a sending node that fails to deliver an
earlier message. Numerous message formats and required parses are provided to
transform received marshalled CRDT messages into structured ones.
*/
package comm
