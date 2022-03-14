package database

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
	"github.com/rs/zerolog/log"
	"os"
)

type logger struct{
	prefix string
}

func (l logger) Errorf(fmt string, args ...interface{}) {
	log.Error().Msgf(l.prefix + fmt, args...)
}

func (l logger) Warningf(fmt string, args ...interface{}) {
	log.Warn().Msgf(l.prefix + fmt, args...)
}

func (l logger) Infof(fmt string, args ...interface{}) {
	log.Debug().Msgf(l.prefix + fmt, args...)
}

func (l logger) Debugf(fmt string, args ...interface{}) {
	log.Debug().Msgf(l.prefix + fmt, args...)
}

func newBadgerDB(dir string, compression options.CompressionType) (*badger.DB, error) {
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, err
	}
	opts := badger.DefaultOptions(dir).
		WithLogger(logger{prefix: "Badger: "}).
		WithCompression(compression)

	if compression == options.ZSTD {
		opts.WithZSTDCompressionLevel(5)
	}

	return badger.Open(opts)
}

func walk(db *badger.DB, opts badger.IteratorOptions, fn PerItemFunc) error {
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
