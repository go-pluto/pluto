/*
Package crdt implements the operation-based observed-removed set (ORSet) structure
upon that the CmRDT parts of pluto are built.

CAUTION! Consider these two requirements:
* For correct operation and results we expect the broadcast communication to all
  other replicas to be reliable and causally-ordered as provided by, for example,
  pluto's package comm.
* Access to the functions this package provides is expected to be synchronized
  explicitly by some outside measures, e.g. by wrapping calls to this package
  with a mutex lock if concurrent access is possible. This package does not(!)
  synchronize access by itself.

The operation-based ORSet implementation of this package is a practical derivation
from its specification by Shapiro, Preguiça, Baquero and Zawirski, available under:
https://hal.inria.fr/inria-00555588/document
*/
package crdt
