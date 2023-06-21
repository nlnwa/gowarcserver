# gowarcserver

![Test](https://github.com/nlnwa/gowarcserver/actions/workflows/test.yaml/badge.svg)
![Release](https://github.com/nlnwa/gowarcserver/actions/workflows/release.yaml/badge.svg)

A tool to index and serve contents of WARC files.

## Development

### Requirements

go version 1.19 or newer

### Build

    go build

### Test

    go test ./...

### Lint

The GitHub Actions test workflow uses [golangci-lint](https://golangci-lint.run) for linting.

Install the linter locally by following the steps described in golangci-lint's
[local installation](https://golangci-lint.run/usage/install/#local-installation) guide.

Note that on **linux** the guide expects you to have `$GOPATH/bin` included in your `PATH` variable.

    golangci-lint run -E "bodyclose" -E "dogsled" -E "durationcheck" -E "errorlint" -E "forcetypeassert" -E "noctx" -E "exhaustive" -E "exportloopref" --timeout 3m0s
