package handlers

import (
	"fmt"
	"net/http"
)

type Upstream struct {
	*http.Client
}

// Upstream makes a request r upstream and writes the response to the http.ResonseWriter w.
// writes the response to w.
func (c Upstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	resp, err := c.Do(r)
	if err != nil {
		HandleError(w, fmt.Errorf("failed to query %s: %w", r.URL, err))
		return
	}
	err = resp.Write(w)
	if err != nil {
		http.Error(w, err.Error(), resp.StatusCode)
	}
}
