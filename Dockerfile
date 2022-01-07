FROM golang:1.16 as build

WORKDIR /build

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

# -trimpath remove file system paths from executable
# -ldflags arguments passed to go tool link:
#   -s disable symbol table
#   -w disable DWARF generation
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags "-s -w"


FROM gcr.io/distroless/base
COPY --from=build /build/gowarcserver /
EXPOSE 9999

ENTRYPOINT ["/gowarcserver"]
CMD ["serve"]
