package warcserver

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nlnwa/gowarc"
	"github.com/nlnwa/gowarcserver/internal/loader"
	"github.com/nlnwa/gowarcserver/internal/server/handlers"
	"github.com/nlnwa/gowarcserver/schema"
	"github.com/rs/zerolog/log"
	"net/http"
)

type ResourceHandler struct {
	DbCdxServer
	loader loader.RecordLoader
}

func (rh ResourceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	api, err := ParseCdxjApi(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	api.Limit = 1

	n, err := rh.search(api, rh.render(w, api))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else if n == 0 {
		http.NotFound(w, r)
	}
}

func (rh ResourceHandler) render(w http.ResponseWriter, api *CdxjServerApi) RenderFunc {
	return func(record *schema.Cdx) error {
		warcId := record.Rid
		if len(warcId) > 0 && warcId[0] != '<' {
			warcId = "<" + warcId + ">"
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		warcRecord, err := rh.loader.Load(ctx, warcId)
		if err != nil {
			return err
		}
		defer func() {
			err := warcRecord.Close()
			if err != nil {
				log.Warn().Msgf("failed to close warc record: %s", err)
			}
		}()

		switch api.Output {
		case OutputContent:
			switch v := warcRecord.Block().(type) {
			case gowarc.HttpResponseBlock:
				// render as HTTP response
				return handlers.RenderContent(w, v)
			default:
				// unknown block type, render record as plain text
				w.Header().Set("Content-Type", "text/plain; charset=utf-8")
				return handlers.RenderRecord(w, warcRecord)
			}
		case OutputJson:
			cdx, _ := json.Marshal(cdxjToPywbJson(record))
			return renderWarcContent(w, warcRecord, api, fmt.Sprintf("%s\n", cdx))
		default:
			cdx, _ := json.Marshal(cdxjToPywbJson(record))
			return renderWarcContent(w, warcRecord, api, fmt.Sprintf("%s %s %s\n", record.Ssu, record.Sts, cdx))
		}
	}
}

func renderWarcContent(w http.ResponseWriter, warcRecord gowarc.WarcRecord, api *CdxjServerApi, cdx string) error {
	w.Header().Set("Warcserver-Cdx", cdx)
	w.Header().Set("Link", "<"+warcRecord.WarcHeader().Get(gowarc.WarcTargetURI)+">; rel=\"original\"")
	w.Header().Set("WARC-Target-URI", warcRecord.WarcHeader().Get(gowarc.WarcTargetURI))
	w.Header().Set("Warcserver-Source-Coll", api.Collection)
	w.Header().Set("Content-Type", "application/warc-record")
	w.Header().Set("Memento-Datetime", warcRecord.WarcHeader().Get(gowarc.WarcDate))
	w.Header().Set("Warcserver-Type", "warc")
	return handlers.RenderRecord(w, warcRecord)
}
