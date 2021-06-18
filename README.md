![Docker](https://github.com/nlnwa/gowarcserver/workflows/Docker/badge.svg)

# gowarcserver

The gowarc server module. This tool can be used to index and serve warc files

# Requirements

go 1.15 or newer

# Build

Run `go build ./cmd/warcserver/`

# Local linting
The project CI uses [golangci-lint](https://golangci-lint.run) to lint any incoming PR. It's recommended to set up the linter locally to save everyone involved some time. You can do so by following the steps described in golangci-lint's [local installation](https://golangci-lint.run/usage/install/#local-installation) guide. 

Note that in the case of **linux** the guide expects that you have `$GOPATH/bin` included in your `PATH` variable.

When you are able to run `golangci-lint --version` in your terminal of choice, then it's also recommended to set up the optional git hook which you can read more about in the [githook folder](https://github.com/nlnwa/gowarcserver/tree/master/githooks)  

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
