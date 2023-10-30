FROM golang:1.21 as build

WORKDIR /build

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

ARG VERSION

# -trimpath remove file system paths from executable
# -ldflags arguments passed to go tool link:
#   -s disable symbol table
#   -w disable DWARF generation
#   -X add string value definition of the form importpath.name=value
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -trimpath \
    -ldflags "-s -w -X github.com/nlnwa/gowarcserver/cmd/version.Version=${VERSION}"


FROM gcr.io/distroless/base-debian11
COPY --from=build /build/gowarcserver /
EXPOSE 9999

ENTRYPOINT ["/gowarcserver"]
CMD ["serve"]
