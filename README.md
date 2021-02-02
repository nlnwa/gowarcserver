![Docker](https://github.com/nlnwa/gowarc/workflows/Docker/badge.svg)

# go-warc

A tool for handling everything warc, written in go.

# Requirements

go 1.13 or newer

# Build

Run `go build ./cmd/warc/`

# Config file

You can configure certain aspect of gowarc with a config file. Here are all posible fields. These can also be overwritten by enviournment variables with same name


| Name          | Type           | Description                                                                          | Default   |
| ------------- | -------------  | -----------                                                                          | -------   |
| warcdir       |  List of paths | The path to directories where warcs that should be auto indexed                      | ["."]     |
| indexdir      |  path          | The root directory for index files                                                   | "."       |
| autoindex     |  bool          | Whether gowarc should index from the warcdir(s) when serving automatically or not    | true      |
| warcport      |  int           | The port that the serve command will use if not overridden as argument to serve      | 9999      |
| loglevel      |  string        | Change the application log level manually                                            | "info"    |
