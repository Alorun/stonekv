package rocketdb

// Centralized cgo build directives for the whole package.
//
// Header path (cdeps/include) and the clean release static library
// (cdeps/lib) are exposed inside the package via symlinks, so these
// directives are independent of where the rocketdb engine repo lives
// relative to tinykv.
//
//   cdeps/include -> <rocketdb>/include        (c.h + export.h)
//   cdeps/lib     -> <rocketdb>/build-release  (clean, non-asan librocketdb.a)
//
// Link deps: -lrocketdb plus the C++ runtime (-lstdc++) and -lpthread.
// No snappy/zstd needed (compression symbols are self-contained in the lib).

// #cgo CFLAGS: -I${SRCDIR}/cdeps/include
// #cgo LDFLAGS: -L${SRCDIR}/cdeps/lib -lrocketdb -lstdc++ -lpthread
import "C"
