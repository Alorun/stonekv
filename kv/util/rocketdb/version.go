package rocketdb

// #include "c.h"
import "C"

// GetMajorVersion returns the major version of the rocketdb engine.
func GetMajorVersion() int {
	return int(C.rocketdb_major_version())
}

// GetMinorVersion returns the minor version of the rocketdb engine.
func GetMinorVersion() int {
	return int(C.rocketdb_minor_version())
}
