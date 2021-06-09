package localhttp

import (
	"net/http"
	"sync"
)

// TODO: document this file (properly comment stuff)
// TODO: timeout a go rourine that has no results https://gobyexample.com/timeouts
// 		 this should also call a timoutFn
// 		 this should happen if the go channel times out in localhandler
// 		 w.Header().Set("Content-Type", "text/plain")
// 		 w.WriteHeader(404)
// 		 w.Write([]byte("Document not found\n"))

type LocalHandler interface {
	ServeLocalHTTP(wg *sync.WaitGroup, r *http.Request) (*Writer, error)
	PredicateFn(r *http.Response) bool
	Children() *Children
}

type ChannelFn func(responses <-chan Writer, w http.ResponseWriter)

func FirstQuery(lh LocalHandler, w http.ResponseWriter, r *http.Request) {
	chanFn := func(responses <-chan Writer, w http.ResponseWriter) {
		localWriter := <-responses
		for key, values := range localWriter.Header() {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.Write(localWriter.Bytes())
	}

	CustomQuery(lh, w, r, chanFn)
}

func AggregatedQuery(lh LocalHandler, w http.ResponseWriter, r *http.Request) {
	chanFn := func(responses <-chan Writer, w http.ResponseWriter) {
		for localWriter := range responses {
			for key, values := range localWriter.Header() {
				for _, value := range values {
					w.Header().Add(key, value)
				}
			}
			w.Write(localWriter.Bytes())
		}
	}

	CustomQuery(lh, w, r, chanFn)
}

func CustomQuery(lh LocalHandler, w http.ResponseWriter, r *http.Request, chanFn ChannelFn) {
	workCount := len(lh.Children().Urls) + 1
	responses := make(chan Writer, workCount)

	var waitGroup sync.WaitGroup
	waitGroup.Add(workCount)

	go func() {
		waitGroup.Wait()
		close(responses)
	}()

	go func() {
		defer waitGroup.Done()
		localWriter, err := lh.ServeLocalHTTP(&waitGroup, r)
		if err != nil {
			// TODO: error handling should only occur when response is empty
			// and we got an error of (some) severity in one or more of the nodes
			// w.Header().Set("Content-Type", "text/plain")
			// fmt.Fprint(w, err)

			// // if the error is from a malformed url being parsed, then the url is invalid
			// if _, ok := err.(*whatwg.UrlError); ok {
			// 	// 422: the url is unprocessanble.
			// 	w.WriteHeader(http.StatusUnprocessableEntity)
			// } else {
			// 	// 500: unexpected error recieved
			// 	w.WriteHeader(http.StatusInternalServerError)
			// }

			return
		}
		responses <- *localWriter
	}()

	queryData := QueryData{
		PredicateFn: lh.PredicateFn,
		Wg:          &waitGroup,
		NodeUrl:     r.URL,
		Children:    lh.Children(),
		Response:    responses,
	}
	ChildQuery(queryData)

	chanFn(responses, w)
}
