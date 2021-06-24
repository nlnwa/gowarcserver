package handlers

import (
	"context"
	"net/http"
	"net/url"
	"time"
)

type ResponsePredicateFn func(*http.Response) bool

func PredicateFn(r *http.Response) bool {
	return r.StatusCode == 200 && r.ContentLength > 1
}

func Aggregated(children []url.URL, timeout time.Duration) http.Handler {
	writers := make(chan *Writer, len(children))

	iterFn := func(_ http.ResponseWriter, req *http.Request) {
		nw := NewWriter()
		Upstream{http.DefaultClient}.ServeHTTP(nw, req)
		writers <- nw
	}

	afterFn := func(w http.ResponseWriter, _ *http.Request) {
		var i int
		for nw := range writers {
			for key, values := range nw.Header() {
				for _, value := range values {
					w.Header().Add(key, value)
				}
			}
			w.Write(nw.Bytes())
			i += 1
		}
		if i <= 0 {
			// TODO: ChildHandler should have a 'noResponseFn' that is called here
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(404)
			w.Write([]byte("Document not found\n"))
		}
	}

	return ChildHandler(children, timeout, iterFn, afterFn)
}

func FirstHandler(children []url.URL, timeout time.Duration) http.Handler {
	writers := make(chan *Writer, len(children))

	iterFn := func(_ http.ResponseWriter, req *http.Request) {
		nw := NewWriter()
		Upstream{http.DefaultClient}.ServeHTTP(nw, req)
		writers <- nw
	}

	afterFn := func(w http.ResponseWriter, _ *http.Request) {
		for {
			select {
			case nw := <-writers:
				for key, values := range nw.Header() {
					for _, value := range values {
						w.Header().Add(key, value)
					}
				}
				w.Write(nw.Bytes())
				w.WriteHeader(nw.Code)
			case <-time.After(timeout):
				// TODO: ChildHandler should have a 'noResponseFn' that is called here
				w.Header().Set("Content-Type", "text/plain")
				w.WriteHeader(404)
				w.Write([]byte("Document not found\n"))
			}
		}
	}

	return ChildHandler(children, timeout, iterFn, afterFn)
}

func ChildHandler(children []url.URL, timeout time.Duration, iterFn http.HandlerFunc, afterFn http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		for _, childUrl := range children {
			req := r.Clone(ctx)
			req.URL = BuildChildURLString(&childUrl, req.URL)
			go iterFn(w, r)
		}
		afterFn(w, r)
	}
}
