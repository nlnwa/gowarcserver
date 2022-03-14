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
	p, err := r.PayloadBytes()
	if err != nil {
		return fmt.Errorf("failed to retrieve payload bytes: %w", err)
	}

	return render(w, *r.HttpHeader(), r.HttpStatusCode(), p)
}

func RenderRedirect(w http.ResponseWriter, location string) {
	w.Header().Set("Location", location)
	w.Header().Set("Content-Length", "0")
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	// Write status line
	w.WriteHeader(http.StatusFound)
}

func render(w http.ResponseWriter, h http.Header, code int, r io.Reader) error {
	// Write headers
	for key, values := range h {
		for i, value := range values {
			if i == 0 {
				w.Header().Set(key, value)
			} else {
				w.Header().Add(key, value)
			}
		}
	}

	// Write status line
	w.WriteHeader(code)

	// We are done if no reader
	if r == nil {
		return nil
	}

	// Write HTTP payload
	_, err := io.Copy(w, r)
	if err != nil {
		return fmt.Errorf("failed to write HTTP payload: %w", err)
	}

	return nil
}
