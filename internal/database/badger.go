package database

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
	log "github.com/sirupsen/logrus"
	"os"
)

func newBadgerDB(dir string, compression options.CompressionType) (*badger.DB, error) {
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, err
	}
	opts := badger.DefaultOptions(dir).
		WithLogger(log.StandardLogger()).
		WithCompression(compression)

	if compression == options.ZSTD {
		opts.WithZSTDCompressionLevel(5)
	}

	return badger.Open(opts)
}

func walk(db *badger.DB, opts badger.IteratorOptions, fn PerItemFunction) error {
	return db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if fn(item) {
				break
			}
		}
		return nil
	})
}
