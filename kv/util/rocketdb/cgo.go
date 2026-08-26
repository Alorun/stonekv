package rocketdb

// Centralized cgo build directives for the whole package.
//
// Header path (cdeps/include) and the clean release static library
// (cdeps/lib/librocketdb.a) are bundled in this package, so building StoneKV
// does not depend on an external RocketDB checkout.
//
// Link deps: -lrocketdb plus the C++ runtime (-lstdc++) and -lpthread.
// No snappy/zstd needed (compression symbols are self-contained in the lib).

// #cgo CFLAGS: -I${SRCDIR}/cdeps/include
// #cgo LDFLAGS: -L${SRCDIR}/cdeps/lib -lrocketdb -lstdc++ -lpthread
import "C"
