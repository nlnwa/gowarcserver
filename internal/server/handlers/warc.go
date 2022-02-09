package handlers

import (
	"fmt"
	"github.com/nlnwa/gowarc"
	"io"
	"net/http"
)

func RenderRecord(w http.ResponseWriter, record gowarc.WarcRecord) error {
	marshaler := gowarc.NewMarshaler()
	_, _, err := marshaler.Marshal(w, record, 0)
	if err != nil {
		return fmt.Errorf("failed to write warc record: %w", err)
	}
	return nil
}


// RenderContent renders the HTTP payload.
func RenderContent(w http.ResponseWriter, r gowarc.HttpResponseBlock) error {
	// Write headers
	for key, values := range *r.HttpHeader() {
		for i, value := range values {
			if i == 0 {
				w.Header().Set(key, value)
			} else {
				w.Header().Add(key, value)
			}
		}
	}

	// Write status line
	w.WriteHeader(r.HttpStatusCode())

	// Write Payload
	p, err := r.PayloadBytes()
	if err != nil {
		return fmt.Errorf("failed to retrieve payload bytes: %w", err)
	}
	_, err = io.Copy(w, p)
	if err != nil {
		return fmt.Errorf("failed to write content: %w", err)
	}

	return nil
}
