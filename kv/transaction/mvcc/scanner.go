package mvcc

import (
	"bytes"

	"github.com/Alorun/stonekv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	txn *MvccTxn
	iter engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Create a iterator on write CF.
	iter := txn.Reader.IterCF(engine_util.CfWrite)

	// Use TsMax encoding, ensure it falls under the "latest version" entry of startkey. 
	iter.Seek(EncodeKey(startKey, TsMax))
	return &Scanner{
		txn: 	txn,
		iter: 	iter,
	}
}

func (scan *Scanner) Close() {
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	if !scan.iter.Valid() {
		return nil, nil, nil
	}

	item := scan.iter.Item()
	userKey := DecodeUserKey(scan.iter.Item().Key())

	// Find the most recently visible key (commitTs <= startTs)
	scan.iter.Seek(EncodeKey(userKey, scan.txn.StartTS))
	if !scan.iter.Valid() {
		return nil, nil, nil
	}

	item = scan.iter.Item()
	foundKey := DecodeUserKey(item.Key())
	if !bytes.Equal(foundKey, userKey) {
		// If there are no equal, proceed directly to the next key.
		return scan.Next()
	}

	writeValue, err := item.Value()
	if err != nil {
		return nil, nil, err
	}
	write, err := ParseWrite(writeValue)
	if err != nil {
		return nil, nil, err
	}

	// The loop continues until the next new key.
	for {
		scan.iter.Next()
		if !scan.iter.Valid() {
			break
		}
		if !bytes.Equal(DecodeUserKey(scan.iter.Item().Key()), userKey) {
			break
		}
	}

	// The key already is deleted
	if write.Kind == WriteKindDelete {
		return scan.Next()
	}

	value, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(userKey, write.StartTS))
	if err != nil {
		return nil, nil, err
	}

	return userKey, value, nil
}
