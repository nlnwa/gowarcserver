FROM golang:1.13 as build

WORKDIR /build

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

# Compile the binary statically, so it can be run without dynamic libraries.
RUN go test ./... && \ 
    CGO_ENABLED=0 GOOS=linux go install -a -ldflags '-extldflags "-s -w -static"' ./cmd/warcserver

# Now copy it into our base image.
FROM gcr.io/distroless/base
COPY --from=build /go/bin/warcserver /
EXPOSE 9999

ENTRYPOINT ["/warcserver"]
CMD ["serve"]
