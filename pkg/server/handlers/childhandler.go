package handlers

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
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
					log.WithField("uri", resp.Request.RequestURI).WithField("status", resp.Status).Errorln(err)
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
					for k, vv := range resp.Header {
						for _, v := range vv {
							w.Header().Add(k, v)
						}
					}
					w.WriteHeader(resp.StatusCode)

					if _, err := io.Copy(w, resp.Body); err != nil {
						log.WithError(err).
							WithField("uri", resp.Request.RequestURI).
							WithField("status", resp.Status).
							Errorln("Failed to write response body")
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
			req.URL = BuildChildURLString(childUrl, req.URL)
			go func() {
				defer wg.Done()

				// The consumer of the http response is responsible for closing the response body
				//nolint:bodyclose
				resp, err := client.Do(req)
				if err != nil {
					log.Errorf("request failed: %v: %v", req, err)
				} else {
					responses <- resp
				}
			}()
		}

		responseHandler(w, r, responses)
	}
}
