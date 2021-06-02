package childhandler

import (
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type UrlBuilderFn func(childUrl *url.URL) string
type ResponsePredicateFn func(*http.Response) bool

type QueryData struct {
	// Used to build url based on child url
	UrlBuilder UrlBuilderFn
	// Used to dictate if a response should be included in response channel
	// true means it should be included, false will cause an early return for a given child
	ResponsePredicate  ResponsePredicateFn
	ChildUrls          []url.URL
	Timeout            time.Duration
	WaitGroup          *sync.WaitGroup
	ChildQueryResponse chan<- []byte
}

func Query(qd QueryData) {
	for _, childUrl := range qd.ChildUrls {
		u := childUrl
		go func(u *url.URL) {
			defer qd.WaitGroup.Done()

			client := http.Client{
				Timeout: qd.Timeout,
			}
			urlStr := qd.UrlBuilder(u)
			resp, err := client.Get(urlStr)
			if err != nil {
				log.Warnf("Query to %s resultet in error: %v", u, err)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				log.Warnf("Query to %s got status code %d", u, resp.StatusCode)
				return
			}

			if !qd.ResponsePredicate(resp) {
				return
			}

			bodyBytes, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Printf("Failed to read response from child url %s: %v", u, err)
				return
			}

			qd.ChildQueryResponse <- bodyBytes
		}(&u)
	}
}
