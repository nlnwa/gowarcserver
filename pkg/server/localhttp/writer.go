package localhttp

import (
	"bytes"
	"errors"
	"net/http"
)

// Used by the gowarcserver handlers to write a local respons from a request.
// This makes it easier to uniformly deal with aggregated request results and local results
// It implements the ResponseWriter interface in order to achieve this
type Writer struct {
	body   *bytes.Buffer
	header *http.Header
	status int
}

func NewWriter() *Writer {
	body := bytes.NewBuffer([]byte{})
	header := http.Header(make(map[string][]string))
	return &Writer{
		body:   body,
		header: &header,
		status: -1,
	}
}

func (l *Writer) Bytes() []byte {
	if l.body == nil {
		return []byte{}
	}
	return l.body.Bytes()
}

func (l *Writer) Header() http.Header {
	if l.header == nil {
		return nil
	}
	return *l.header
}

func (l *Writer) Write(bytes []byte) (n int, err error) {
	if l.body == nil {
		return 0, errors.New("writer missing buffer")
	}
	return l.body.Write(bytes)
}

func (l *Writer) WriteHeader(statusCode int) {
	l.status = statusCode
}
