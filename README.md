![Docker](https://github.com/nlnwa/gowarcserver/workflows/Docker/badge.svg)

# gowarcserver

The gowarc server module. This tool can be used to index and serve warc files

# Requirements

go 1.13 or newer

# Build

Run `go build ./cmd/warcserver/`

# Config file

You can configure certain aspect of gowarcserver with a config file. Here are all posible fields. These can also be overwritten by environment variables with same name


| Name          | Type           | Description                                                                          | Default   |
| ------------- | -------------  | -----------                                                                          | -------   |
| warcdir       |  List of paths | The path to directories where warcs that should be auto indexed                      | ["."]     |
| indexdir      |  path          | The root directory for index files                                                   | "."       |
| autoindex     |  bool          | Whether gowarc should index from the warcdir(s) when serving automatically or not    | true      |
| warcport      |  int           | The port that the serve command will use if not overridden as argument to serve      | 9999      |
| loglevel      |  string        | Change the application log level manually                                            | "info"    |
