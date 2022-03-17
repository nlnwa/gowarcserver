package warcserver

import (
	"context"
	"errors"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"github.com/nlnwa/gowarc"
	"github.com/nlnwa/gowarcserver/internal/loader"
	"github.com/nlnwa/gowarcserver/internal/server/handlers"
	"github.com/nlnwa/gowarcserver/internal/surt"
	"github.com/nlnwa/gowarcserver/schema"
	"github.com/nlnwa/whatwg-url/url"
	"net/http"
	"strings"
	"time"
)

type ResourceHandler struct {
	DbCdxServer
	Loader loader.RecordLoader
}

func (rh ResourceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	params := httprouter.ParamsFromContext(r.Context())

	// closest parameter
	p0 := params[0].Value
	// remove trailing 'id_'
	closest := p0[:len(p0)-3]

	// url parameter
	p1 := params[1].Value
	// remove leading '/'
	uri := p1[1:]

	// we must add on any query parameters
	if q := r.URL.Query().Encode(); len(q) > 0 {
		uri += "?" + q
	}
	// and fragment
	if len(r.URL.Fragment) > 0 {
		// and fragment
		uri += "#" + r.URL.Fragment

	}

	key, err := surt.StringToSsurt(uri)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// fetch cdx to access warc record id
	cdx, err := rh.one(key, closest)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if cdx == nil {
		http.NotFound(w, r)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	// load warc record by id
	warcRecord, err := rh.Loader.Load(ctx, cdx.Rid)
	if err != nil {
		var errWarcProfile loader.ErrResolveRevisit
		if errors.As(err, &errWarcProfile) {
			http.Error(w, errWarcProfile.Error(), http.StatusNotImplemented)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer warcRecord.Close()

	block, ok := warcRecord.Block().(gowarc.HttpResponseBlock)
	if !ok {
		http.Error(w, fmt.Sprintf("Record not renderable: %s", warcRecord), http.StatusInternalServerError)
		return
	}

	s := block.HttpStatusCode()

	// Handle redirects
	if isRedirect(s) {
		location := block.HttpHeader().Get("Location")
		if location == "" {
			http.NotFound(w, r)
			return
		}
		// TODO check and handle relative location header paths
		// if !schemeRegExp.MatchString(location) {
		// }

		locUrl, err := url.Parse(location)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to parse location header as URL: %s: %s: %v", warcRecord, location, err), http.StatusInternalServerError)
			return
		}

		api := &CdxjServerApi{
			Urls:      []*url.Url{locUrl},
			MatchType: MatchTypeExact,
			Limit:     1,
			Sort:      SortClosest,
			Closest:   closest,
		}

		// Fields we need to rewrite the location header
		var sts string
		var uri string

		// Get timestamp and uri from cdx record
		perCdxFunc := func(record *schema.Cdx) error {
			sts = record.Sts
			uri = record.Uri
			return nil
		}

		if n, err := rh.search(api, perCdxFunc); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		} else if n == 0 || sts == "" || uri == "" {
			http.NotFound(w, r)
			return
		}

		prefix := r.URL.Path[:strings.Index(r.URL.Path, "id_")-14]
		path := prefix + sts + "id_/" + uri

		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		host := r.Host
		u, err := url.Parse(scheme + "://" + host)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		u.SetPathname(path)

		handlers.RenderRedirect(w, u.String())
	} else {
		err := handlers.RenderContent(w, block)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func isRedirect(code int) bool {
	return code == http.StatusMovedPermanently ||
		code == http.StatusFound ||
		code == http.StatusTemporaryRedirect ||
		code == http.StatusPermanentRedirect
}
