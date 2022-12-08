package warcserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/nlnwa/gowarc"
	"github.com/nlnwa/gowarcserver/index"
	"github.com/nlnwa/gowarcserver/loader"
	"github.com/nlnwa/gowarcserver/server/api"
	"github.com/nlnwa/gowarcserver/server/handlers"
	"github.com/nlnwa/gowarcserver/surt"
	"github.com/nlnwa/gowarcserver/timestamp"
	urlErrors "github.com/nlnwa/whatwg-url/errors"
	"github.com/nlnwa/whatwg-url/url"
	"github.com/rs/zerolog/log"
)

type Handler struct {
	index.CdxAPI
	index.FileAPI
	index.IdAPI
	loader.WarcLoader
}

func (h Handler) index(w http.ResponseWriter, r *http.Request) {
	coreAPI, err := api.Parse(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	start := time.Now()
	count := 0
	defer func() {
		log.Debug().Msgf("Found %d items in %s", count, time.Since(start))
	}()

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	response := make(chan index.CdxResponse)

	if err = h.CdxAPI.Search(ctx, api.SearchAPI{CoreAPI: coreAPI}, response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().Err(err).Msgf("Search failed: %+v", coreAPI)
		return
	}

	for res := range response {
		if res.Error != nil {
			log.Warn().Err(res.Error).Msg("failed result")
			continue
		}
		cdxj, err := json.Marshal(cdxToPywbJson(res.Cdx))
		if err != nil {
			log.Warn().Err(err).Msg("failed to marshal result")
			continue
		}
		switch coreAPI.Output {
		case api.OutputJson:
			_, err = fmt.Fprintln(w, cdxj)
		default:
			ssu := res.GetSsu()
			sts := timestamp.TimeTo14(res.GetSts().AsTime())
			_, err = fmt.Fprintf(w, "%s %s %s\n", ssu, sts, cdxj)
		}
		if err != nil {
			log.Warn().Err(err).Msg("failed to write result")
			return
		}
		count++
	}
}

func (h Handler) resource(w http.ResponseWriter, r *http.Request) {
	// parse API
	closestReq, err := parseResourceRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancelQuery := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancelQuery()

	// query API
	response := make(chan index.CdxResponse)
	err = h.CdxAPI.Closest(ctx, closestReq, response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().Err(err).Msgf("Failed search closest: %+v", closestReq)
		return
	}

	var res index.CdxResponse
	for res = range response {
		if res.Error != nil {
			log.Warn().Err(err).Msg("Failed cdx response")
			continue
		}
		if res.Cdx == nil {
			http.NotFound(w, r)
			return
		}
		break
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	// load warc record by storage ref
	warcRecord, err := h.LoadByStorageRef(ctx, res.GetRef())
	defer func() {
		if warcRecord != nil {
			_ = warcRecord.Close()
		}
	}()
	if err != nil {
		var errResolveRevisit loader.ErrResolveRevisit
		if errors.As(err, &errResolveRevisit) {
			http.Error(w, errResolveRevisit.Error(), http.StatusNotImplemented)
			log.Error().Err(errResolveRevisit).Msg("Failed to load record")
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().Err(err).Msg("Failed to load record")
		return
	}

	block, ok := warcRecord.Block().(gowarc.HttpResponseBlock)
	if !ok {
		err := fmt.Errorf("record not renderable: %s", warcRecord)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().Err(err).Msg("Failed to load resource")
		return
	}

	s := block.HttpStatusCode()

	if !isRedirect(s) {
		p, err := block.PayloadBytes()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			log.Error().Err(err).Msg("Failed to load resource")
			return
		}
		err = handlers.Render(w, *block.HttpHeader(), block.HttpStatusCode(), p)
		if err != nil {
			log.Warn().Err(err).Msg("Failed to load resource")
		}
		return
	}

	// handle redirect
	location := block.HttpHeader().Get("Location")
	if location == "" {
		err := errors.New("empty redirect location")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().Err(err).Msg("Failed to load resource")
		return
	}

	locUrl, err := url.Parse(location)
	if urlErrors.Code(err) == urlErrors.FailRelativeUrlWithNoBase {
		locUrl, err = url.ParseRef(closestReq.rawUrl, location)
		if err != nil {
			err = fmt.Errorf("failed to parse relative location header as URL: %s: %s: %w", warcRecord, location, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			log.Error().Err(err).Msg("Failed to load resource")
			return
		}
	}
	if err != nil {
		err = fmt.Errorf("failed to parse location header as URL: %s: %s: %w", warcRecord, location, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().Err(err).Msg("Failed to load resource")
		return
	}

	closestReq = closestRequest{
		key:     surt.UrlToSsurt(locUrl),
		closest: closestReq.closest,
		limit:   1,
	}
	response = make(chan index.CdxResponse)
	err = h.Closest(ctx, closestReq, response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().Err(err).Msg("Failed to find closest redirect")
		return
	}

	// fields needed to rewrite the location header
	var sts string
	var uri string

	for res := range response {
		if res.Error != nil {
			log.Warn().Err(err).Msg("failed result")
			continue
		}
		sts = timestamp.TimeTo14(res.GetSts().AsTime())
		uri = res.GetUri()
	}
	if uri == "" {
		http.NotFound(w, r)
		return
	}
	path := r.URL.Path[:strings.Index(r.URL.Path, "id_")-14] + sts + "id_/" + uri
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	host := r.Host
	u, err := url.Parse(scheme + "://" + host)
	if err != nil {
		err := fmt.Errorf("failed to construct redirect location: %s: %w", uri, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().Err(err).Msg("Failed load resource")
		return
	}
	u.SetPathname(path)

	handlers.RenderRedirect(w, u.String())
}

func isRedirect(code int) bool {
	return code == http.StatusMovedPermanently ||
		code == http.StatusFound ||
		code == http.StatusTemporaryRedirect ||
		code == http.StatusPermanentRedirect
}
