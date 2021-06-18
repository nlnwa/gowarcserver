![Docker](https://github.com/nlnwa/gowarcserver/workflows/Docker/badge.svg)

# gowarcserver

The gowarc server module. This tool can be used to index and serve warc files

# Requirements

go 1.15 or newer

# Build

Run `go build ./cmd/warcserver/`

# Config file

You can configure certain aspect of gowarcserver with a config file. Here are all posible fields. These can also be overwritten by environment variables with same name


| Name          | Type           | Description                                                                          | Default   |
| ------------- | -------------  | -----------                                                                          | -------   |
| warcDir       |  List of paths | The path to directories where warcs that should be auto indexed                      | ["."]     |
| indexDir      |  path          | The root directory for index files                                                   | "."       |
| autoIndex     |  bool          | Whether gowarc should index from the warcdir(s) when serving automatically or not    | true      |
| warcPort      |  int           | The port that the serve command will use if not overridden as argument to serve      | 9999      |
| logLevel      |  string        | Change the application log level manually                                            | "info"    |
| compression   |  string        | Change the db table compression. Legal values are: 'none', 'snappy', 'zstd'          | "none"    |
| idDb          |  bool          | true *Disables* id db, false *Enables* id db                                         | false     |
| fileDb        |  bool          | true *Disables* file db, false *Enables* file db                                     | false     | 
| cdxDb         |  bool          | true *Disables* cdx db, false *Enables* cdx db                                       | false     |
| childUrls     |  []string      | Register urls pointing to gowarcserver processes which are 'children'                | [""]      |
| childQueryTimeout | int        | How long in miliseconds a request to a child can take before resulting in timeout    | 300       |
