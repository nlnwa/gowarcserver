package localhttp

import (
	"net/http"
	"sync"
	"time"
)

type LocalHandler interface {
	// Function used to query self. It will return a Writer in the event of
	// no error and this will be sent to a repsonse channel if present
	ServeLocalHTTP(r *http.Request) (*Writer, error)
	// PredicateFn allows a handler to filter out responses from child processes
	// if they do not meet a certain criteria. True means that the response is
	// acceptable to be used, a false will mean that the response should be discarded
	PredicateFn(r *http.Response) bool
	// retrive the children struct from the interface instance
	Children() *Children
}

type ChannelFn func(responses <-chan Writer, w http.ResponseWriter)

// Query self and all children, the first response that passes the PredicateFn test is
// written to the client through the response writer
func FirstQuery(lh LocalHandler, w http.ResponseWriter, r *http.Request, timeAfter time.Duration) {
	chanFn := func(responses <-chan Writer, w http.ResponseWriter) {
		select {
		case localWriter := <-responses:
			for key, values := range localWriter.Header() {
				for _, value := range values {
					w.Header().Add(key, value)
				}
			}
			w.Write(localWriter.Bytes())
			w.WriteHeader(localWriter.status)
		case <-time.After(timeAfter):
			// TODO: LocalHandler should have a 'noResponseFn' that is called here
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(404)
			w.Write([]byte("Document not found\n"))
		}
	}

	CustomQuery(lh, w, r, chanFn)
}

// Query self and all children, all responses that passes the PredicateFn test is
// written to the client through the response writer
func AggregatedQuery(lh LocalHandler, w http.ResponseWriter, r *http.Request) {
	// TODO: write most common header in event of response
	chanFn := func(responses <-chan Writer, w http.ResponseWriter) {
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
			// TODO: LocalHandler should have a 'noResponseFn' that is called here
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(404)
			w.Write([]byte("Document not found\n"))
		}
	}

	CustomQuery(lh, w, r, chanFn)
}

// Query self and all children, responses are sent into the custom chanFn using the responses channel parameter
// allowing the caller to specify how the channel should interperet and use the response(s)
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
		localWriter, err := lh.ServeLocalHTTP(r)
		if err != nil {
			return
		}
		responses <- *localWriter
	}()

	queryData := ChildQueryData{
		PredicateFn: lh.PredicateFn,
		Wg:          &waitGroup,
		NodeUrl:     r.URL,
		Children:    lh.Children(),
		Response:    responses,
	}
	ChildQuery(queryData)

	chanFn(responses, w)
}
