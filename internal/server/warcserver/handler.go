package warcserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nlnwa/gowarc"
	"github.com/nlnwa/gowarcserver/internal/loader"
	"github.com/nlnwa/gowarcserver/internal/server/api"
	"github.com/nlnwa/gowarcserver/internal/server/handlers"
	"github.com/nlnwa/gowarcserver/internal/surt"
	"github.com/nlnwa/gowarcserver/internal/timestamp"
	"github.com/nlnwa/gowarcserver/schema"
	"github.com/nlnwa/whatwg-url/url"
	"github.com/rs/zerolog/log"
	"net/http"
	"strings"
	"time"
)

type Handler struct {
	db api.DbAdapter
	loader loader.RecordLoader
}

func (h Handler) index(w http.ResponseWriter, r *http.Request) {
	coreAPI, err := api.Parse(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	start := time.Now()
	n, err := h.db.Search(coreAPI, renderCdx(w, coreAPI))
	if err != nil {
		api.HandleError(w, n, err)
	}
	if n > 0 {
		log.Debug().Msgf("Found %d items in %s", n, time.Since(start))
	}
}

func renderCdx(w http.ResponseWriter, coreAPI *api.CoreAPI) api.PerCdxFunc {
	return func(record *schema.Cdx) error {
		cdxj, err := json.Marshal(cdxToPywbJson(record))
		if err != nil {
			return err
		}
		switch coreAPI.Output {
		case api.OutputJson:
			_, err = fmt.Fprintln(w, cdxj)
		default:
			sts := timestamp.TimeTo14(record.Sts.AsTime())
			_, err = fmt.Fprintf(w, "%s %s %s\n", record.Ssu, sts, cdxj)
		}
		return err
	}
}

func (h Handler) resource(w http.ResponseWriter, r *http.Request) {
	uri, ts := parseWeb(r)

	key, err := surt.StringToSsurt(uri)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// fetch cdx to access warc record id
	cdx, err := h.db.Closest(key, ts)
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
	warcRecord, err := h.loader.Load(ctx, cdx.Rid)
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

		coreAPI := &api.CoreAPI{
			Urls:      []*url.Url{locUrl},
			MatchType: api.MatchTypeExact,
			Limit:     1,
			Sort:      api.SortClosest,
			Closest:   ts,
		}

		// the fields we need to rewrite the location header
		var sts string
		var uri string

		// Get timestamp and uri from cdx record
		perCdxFunc := func(record *schema.Cdx) error {
			sts = timestamp.TimeTo14(record.Sts.AsTime())
			uri = record.Uri
			return nil
		}

		if n, err := h.db.Search(coreAPI, perCdxFunc); err != nil {
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
