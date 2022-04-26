![Test](https://github.com/nlnwa/gowarcserver/actions/workflows/test.yaml/badge.svg)
![Release](https://github.com/nlnwa/gowarcserver/actions/workflows/release.yaml/badge.svg)

# gowarcserver

A tool to index and serve contents of WARC files.

## Development

### Requirements

go version 1.16 or newer

### Build

    go build

### Test

    go test ./...

### Lint

The GitHub Actions test workflow uses [golangci-lint](https://golangci-lint.run) for linting.

Install the linter locally by
following the steps described in
golangci-lint's [local installation](https://golangci-lint.run/usage/install/#local-installation) guide.  Note that on **linux** the guide expects you to have `$GOPATH/bin` included in your `PATH` variable.

    golangci-lint run -E "bodyclose" -E "dogsled" -E "durationcheck" -E "errorlint" -E "forcetypeassert" -E "noctx" -E "exhaustive" -E "exportloopref" --timeout 3m0s

## Configuration

gowarcserver can be configured with a config file, environment variables and flags. Flags has precedence over
environment variables that has precedence over config file entries. An environment variable match the uppercased flag
name with underscore in place of dash.

| Name                  | Type              | Description                                                                   | Default       | Sub command |
| -------------         | -------------     | -----------                                                                   | -------       | ------- |
| config                | string            | Path to configuration file                                                    | ./config.yaml | global |
| log-level             | string            | Log level. Legal values are "trace" , "debug", "info", "warn"  or "error"     | "info"        | global |
| port                  | int               | Server port                                                                   | 9999          | serve |
| index                 | bool              | Enable indexing when running server                                           | true          | serve |
| watch                 | bool              | Update index when files change                                                | false         | serve |
| log-requests          | bool              | Enable request logging                                                        | false         | serve |
| dirs                  | list of paths     | Comma separated list of directories to index                                  | ["."]         | index, serve |
| db-dir                | path              | Location of index database                                                    | "."           | index, serve |
| max-depth             | int               | Maximum index recursion depth                                                 | 4             | index, serve |
| include               | list of suffixes  | Only index files that match one of these suffixes                             | []            | index, serve |
| workers               | int               | Number of index workers                                                       | 8             | index, serve |
| compression           | string            | Database compression type. Legal values are: 'none', 'snappy', 'zstd'         | "snappy"      | index, serve |
| bloom                 | bool              | Enable bloom filter when indexing with "toc" format                           | true          | index |
| bloom-capacity        | uint              | Estimated bloom filter capacity                                               | 1000          | index |
| bloom-fp              | float64           | Estimated bloom filter false positive rate                                    | 0.01          | index |
| child-urls            | []string          | Urls pointing to other gowarcserver processes running a server                | []            | proxy |
| child-query-timeout   | Duration          | Child query timeout a request to a child can take before resulting in timeout | 300ms         | proxy |
