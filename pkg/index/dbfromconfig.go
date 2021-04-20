package index

/*
	Usually it should be avoided to reference viper in pkg, but this functionality is used by
	cmd index and server, so it's an exception.
*/

import (
	"github.com/dgraph-io/badger/v2/options"
	"github.com/nlnwa/gowarcserver/pkg/compressiontype"
)

type DbConfig struct {
	compression string
	dir         string
}

func NewDbConfig(compresion string, dir string) *DbConfig {
	return &DbConfig{compression: compresion, dir: dir}
}

// TODO: test somehow?
// Create a database based on the viper settings set by the user
func DbFromConfig(config *DbConfig) (*Db, error) {
	compression, cErr := compressiontype.FromString(config.compression)
	if cErr != nil {
		return nil, cErr
	}

	db, dbErr := NewIndexDb(config.dir, options.CompressionType(compression))
	if dbErr != nil {
		return nil, dbErr
	}

	return db, nil
}
