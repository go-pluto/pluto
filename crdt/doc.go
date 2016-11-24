/*
Package crdt implements the needed operation-based observed-removed set (ORSet) structure
upon that the CmRDT parts of pluto are built. Please note, that for correct operation
and results we expect the broadcast communication to all other replicas to be reliable
and causally-ordered.

The operation-based ORSet implementation of this package is a practical derivation from
its specification by Shapiro, Preguiça, Baquero and Zawirski, available under:
https://hal.inria.fr/inria-00555588/document
*/
package crdt
