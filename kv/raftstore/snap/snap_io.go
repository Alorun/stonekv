package snap

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
)

// Snapshot CF files used to be badger SST files, ingested via
// badger.DB.IngestExternalFiles. The rocketdb engine exposes no SST ingest, so
// snapshot CF files now use a simple self-describing record stream instead:
//
//	record := keyLen(uint32 BE) | valueLen(uint32 BE) | key | value
//
// The format only has to be consistent between cfFileWriter (build side) and
// readCFFile (apply side); the meta file's size/checksum are still computed
// over the raw bytes, and network streaming treats the file as opaque bytes,
// so nothing else needs to change.

type cfFileWriter struct {
	f *os.File
	w *bufio.Writer
}

func newCFFileWriter(f *os.File) *cfFileWriter {
	return &cfFileWriter{f: f, w: bufio.NewWriter(f)}
}

// Add appends one key/value record. The key is written verbatim (it already
// carries the "<cf>_" prefix when produced by the snapshot builder).
func (b *cfFileWriter) Add(key, value []byte) error {
	var hdr [8]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(len(key)))
	binary.BigEndian.PutUint32(hdr[4:8], uint32(len(value)))
	if _, err := b.w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := b.w.Write(key); err != nil {
		return err
	}
	if _, err := b.w.Write(value); err != nil {
		return err
	}
	return nil
}

// Finish flushes buffered records to the underlying file.
func (b *cfFileWriter) Finish() error {
	return b.w.Flush()
}

// Close closes the underlying file.
func (b *cfFileWriter) Close() {
	b.f.Close()
}

// readCFFile streams every key/value record from a snapshot CF file written by
// cfFileWriter, calling fn for each pair.
func readCFFile(path string, fn func(key, value []byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	var hdr [8]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		klen := binary.BigEndian.Uint32(hdr[0:4])
		vlen := binary.BigEndian.Uint32(hdr[4:8])
		key := make([]byte, klen)
		if _, err := io.ReadFull(r, key); err != nil {
			return err
		}
		value := make([]byte, vlen)
		if _, err := io.ReadFull(r, value); err != nil {
			return err
		}
		if err := fn(key, value); err != nil {
			return err
		}
	}
}
