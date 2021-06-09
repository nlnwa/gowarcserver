package localhttp

import (
	"bytes"
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

type Aggregator interface {
	Aggregate(handler *Children, resut chan<- Writer)
}

type ResponsePredicateFn func(*http.Response) bool

type QueryData struct {
	// Used to dictate if a response should be included in response channel
	// true means it should be included, false will cause an early return for a given child
	PredicateFn ResponsePredicateFn
	Wg          *sync.WaitGroup
	NodeUrl     *url.URL
	Children    *Children
	Response    chan<- Writer
}

func ChildQuery(queryData QueryData) {
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
			resp, err := client.Get(urlStr)
			if err != nil {
				log.Warnf("Query to %s resultet in error: %v", u, err)
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
