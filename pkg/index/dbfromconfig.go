package index

type DbConfig struct {
	dir         string
	compression string
	mask        int32
}

func NewDbConfig(dir string, compression string, mask int32) *DbConfig {
	return &DbConfig{dir: dir, compression: compression, mask: mask}
}
