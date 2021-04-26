package index

/*
	Usually it should be avoided to reference viper in pkg, but this functionality is used by
	cmd index and server, so it's an exception.
*/

type DbConfig struct {
	dir         string
	compression string
	mask        int32
}

func NewDbConfig(dir string, compresion string, mask int32) *DbConfig {
	return &DbConfig{dir: dir, compression: compresion, mask: mask}
}
