package localhttp

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type Children struct {
	Urls    []url.URL
	Timeout time.Duration
}

type ResponsePredicateFn func(*http.Response) bool

type ChildQueryData struct {
	// Used to dictate if a response should be included in response channel
	// true means it should be included, false will cause an early return for a given child
	PredicateFn ResponsePredicateFn
	Wg          *sync.WaitGroup
	NodeUrl     *url.URL
	Children    *Children
	Response    chan<- Writer
}

func ChildQuery(queryData ChildQueryData) {
	var wgDoneFn func()
	if queryData.Wg != nil {
		wgDoneFn = func() {
			queryData.Wg.Done()
		}
	} else {
		wgDoneFn = func() {}
	}

	for _, childUrl := range queryData.Children.Urls {
		u := childUrl
		go func(u *url.URL) {
			defer wgDoneFn()

			client := http.Client{
				Timeout: queryData.Children.Timeout,
			}
			urlStr := BuildChildURLString(u, queryData.NodeUrl)
			ctx, cancel := context.WithTimeout(context.Background(), queryData.Children.Timeout)
			defer cancel()

			request, err := http.NewRequestWithContext(ctx, "GET", urlStr, nil)
			if err != nil {
				log.Errorf("Failed to create request: %v", err)
				return
			}

			resp, err := client.Do(request)
			if err != nil {
				log.Warnf("Query to %s failed: %v", u, err)
				return
			}
			defer resp.Body.Close()

			if !queryData.PredicateFn(resp) {
				return
			}

			bodyBytes, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Printf("Failed to read response from child url %s: %v", u, err)
				return
			}
			buffer := bytes.NewBuffer(bodyBytes)

			response := Writer{
				body:   buffer,
				header: &resp.Header,
				status: resp.StatusCode,
			}
			queryData.Response <- response
		}(&u)
	}
}
