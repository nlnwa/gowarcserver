module github.com/nlnwa/gowarcserver

go 1.15

require (
	github.com/dgraph-io/badger/v3 v3.2103.1
	github.com/fsnotify/fsnotify v1.4.9
	github.com/golang/protobuf v1.5.2
	github.com/gorilla/handlers v1.5.1
	github.com/gorilla/mux v1.8.0
	github.com/nlnwa/gowarc v1.0.0-alpha.12
	github.com/nlnwa/whatwg-url v0.1.0
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/cobra v1.2.1
	github.com/spf13/viper v1.8.1
	google.golang.org/protobuf v1.27.1
)

// HACK: as there are issues with sum value of v1.12.0, make sure to update this when badger updates its dependencies
// See issue: https://github.com/google/flatbuffers/issues/6466
replace github.com/google/flatbuffers v1.12.0 => github.com/google/flatbuffers v1.12.1
