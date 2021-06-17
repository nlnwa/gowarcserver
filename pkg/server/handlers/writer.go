package handlers

import (
	"bytes"
	"net/http"
)

// Used by the gowarcserver handlers to write a local respons from a request.
// This makes it easier to uniformly deal with aggregated request results and local results
// It implements the ResponseWriter interface in order to achieve this

// Writer is an implementation of http.ResponseWriter that records body, header and status code.
type Writer struct {
	Head http.Header
	Body *bytes.Buffer
	Code int
}

func NewWriter() *Writer {
	return &Writer{
		Code:           200,
	}
}

func (l *Writer) Bytes() []byte {
	return l.Body.Bytes()
}

func (l *Writer) Header() http.Header {
	m := l.Head
	if m == nil {
		m = make(http.Header)
		l.Head = m
	}
	return m
}

func (l *Writer) Write(bytes []byte) (n int, err error) {
	return l.Body.Write(bytes)
}

func (l *Writer) WriteHeader(statusCode int) {
	l.Code = statusCode
}
