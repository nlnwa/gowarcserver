package dbfromviper

/*
	Usually it should be avoided to reference viper in pkg, but this functionality is used by
	cmd index and server, so it's an exception.
*/

import (
	"github.com/dgraph-io/badger/v2/options"
	"github.com/nlnwa/gowarcserver/pkg/compressiontype"
	"github.com/nlnwa/gowarcserver/pkg/index"
	"github.com/spf13/viper"
)

// TODO: test somehow?
// Create a database based on the viper settings set by the user
func DbFromViper() (*index.Db, error) {
	compressionString := viper.GetString("compression")
	compression, cErr := compressiontype.FromString(compressionString)
	if cErr != nil {
		return nil, cErr
	}

	dbDir := viper.GetString("indexdir")
	db, dbErr := index.NewIndexDb(dbDir, options.CompressionType(compression))
	if dbErr != nil {
		return nil, dbErr
	}

	return db, nil
}
