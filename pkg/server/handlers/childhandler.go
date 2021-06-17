package handlers

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"
)

func PredicateFn(r *http.Response) bool {
	return r.StatusCode == 200 && r.ContentLength > 1
}

type ChannelFn func(responses <-chan Writer, w http.ResponseWriter)

func First(timeout time.Duration, responses <-chan Writer, w http.ResponseWriter) {
		select {
		case localWriter := <-responses:
			for key, values := range localWriter.Header() {
				for _, value := range values {
					w.Header().Add(key, value)
				}
			}
			w.Write(localWriter.Bytes())
			w.WriteHeader(localWriter.Code)
		case <-time.After(timeout):
			// TODO: ChildHandler should have a 'noResponseFn' that is called here
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(404)
			w.Write([]byte("Document not found\n"))
		}
	}
}

// Query self and all children, all responses that passes the PredicateFn test is
// written to the client through the response writer
func Aggregated(timeout time.Duration) ChannelFn {
	return func(responses <-chan Writer, w http.ResponseWriter) {
		var i int
		for localWriter := range responses {
			for key, values := range localWriter.Header() {
				for _, value := range values {
					w.Header().Add(key, value)
				}
			}
			w.Write(localWriter.Bytes())
			i += 1
		}
		if i <= 0 {
			// TODO: ChildHandler should have a 'noResponseFn' that is called here
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(404)
			w.Write([]byte("Document not found\n"))
		}
	}
}

// perItemFn and afterItemFn decided by First vs Aggregated

// Query self and all children, responses are sent into the custom chanFn using the responses channel parameter
// allowing the caller to specify how the channel should interperet and use the response(s)
func NewFirstHandler(children []url.URL, timeout time.Duration) http.HandlerFunc {
   // , perItemFn, afterItemFn
	return func (w http.ResponseWriter, r *http.Request) {
		responses := make(chan *Writer, len(children))

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		for _, childUrl := range children {
			req := r.Clone(ctx)
			req.URL = BuildChildURLString(&childUrl, req.URL)

			go func() {
				nw := NewWriter()
				upstream(nw, r)
				responses <- nw
			}()
		}
		first := <-responses
		// afterItemFn(cancel
		chanFn(responses, w)
	}
}

func upstream(w http.ResponseWriter, r *http.Request) {
	client := http.Client{
		Timeout: 10 * time.Second,
	}
	resp, err := client.Do(r)
	if err != nil {
		HandleError(w, fmt.Errorf("failed to query %s: %w", r.URL, err))
		return
	}
	err = resp.Write(w)
	if err != nil {
		HandleError(w, err)
	}
}

type ResponsePredicateFn func(*http.Response) bool
