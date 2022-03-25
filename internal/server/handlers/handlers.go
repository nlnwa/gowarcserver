package handlers

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

var client = &http.Client{
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

func AggregatedHandler(children []*url.URL, timeout time.Duration) http.Handler {
	responseHandler := func(w http.ResponseWriter, r *http.Request, responses <-chan *http.Response) {
		written := false
		for resp := range responses {
			if resp.StatusCode == http.StatusOK {
				_, err := io.Copy(w, resp.Body)
				if err != nil {
					log.Error().
						Err(err).
						Str("uri", resp.Request.RequestURI).
						Str("status", resp.Status).
						Msg("Failed to write response body")
				} else {
					written = true
				}
			}
			_ = resp.Body.Close()
		}
		if !written {
			http.Error(w, "", http.StatusOK)
		}
	}

	return ChildHandler(children, timeout, responseHandler)
}

func FirstHandler(children []*url.URL, timeout time.Duration) http.Handler {
	responseHandler := func(w http.ResponseWriter, r *http.Request, responses <-chan *http.Response) {
		written := false
		for response := range responses {
			resp := response
			if resp.StatusCode < 400 {
				if !written {
					// Write headers
					for key, values := range resp.Header {
						for i, value := range values {
							if i == 0 {
								w.Header().Set(key, value)
							} else {
								w.Header().Add(key, value)
							}
						}
					}
					w.WriteHeader(resp.StatusCode)

					if _, err := io.Copy(w, resp.Body); err != nil {
						log.Error().Err(err).
							Str("uri", resp.Request.RequestURI).
							Str("status", resp.Status).
							Msg("Failed to write response body")
					}
					written = true
				}
			}
			_ = resp.Body.Close()
		}
		if !written {
			http.Error(w, "Not found", http.StatusNotFound)
		}
	}
	return ChildHandler(children, timeout, responseHandler)
}

type ResponseHandler func(http.ResponseWriter, *http.Request, <-chan *http.Response)

func ChildHandler(children []*url.URL, timeout time.Duration, responseHandler ResponseHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		responses := make(chan *http.Response, len(children))
		wg := new(sync.WaitGroup)
		wg.Add(len(children))
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		go func() {
			wg.Wait()
			close(responses)
		}()

		for _, childUrl := range children {
			req := r.Clone(ctx)
			req.RequestURI = ""
			req.URL = buildChildURLString(childUrl, req.URL)
			log.Debug().Msgf("request to child url %s", req.URL.String())
			go func() {
				defer wg.Done()

				// The consumer of the http response is responsible for closing the response body
				//nolint:bodyclose
				resp, err := client.Do(req)
				if err != nil {
					log.Error().Msgf("request failed: %v: %v", req, err)
				} else {
					responses <- resp
				}
			}()
		}

		responseHandler(w, r, responses)
	}
}
