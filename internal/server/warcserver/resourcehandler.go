package warcserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/nlnwa/gowarcserver/internal/loader"

	"github.com/nlnwa/gowarc"
	cdx "github.com/nlnwa/gowarcserver/schema"
	log "github.com/sirupsen/logrus"
)

type ResourceHandler struct {
	DbCdxServer
	loader loader.RecordLoader
}

func (rh ResourceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	api, err := ParseCdxjApi(r)
	if err != nil {
		log.Warning(err)
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
	return func(cdxRecord *cdx.Cdx) error {
		warcId := cdxRecord.Rid
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
				log.Warnf("failed to close warc record: %s", err)
			}
		}()

		switch api.Output {
		case OutputContent:
			switch v := warcRecord.Block().(type) {
			case gowarc.HttpResponseBlock:
				_, err = warcRecord.WarcHeader().Write(w)
				if err != nil {
					return err
				}
				byteReader, err := v.PayloadBytes()
				if err != nil {
					return err
				}
				_, err = io.Copy(w, byteReader)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return err
				}
			default:
				w.Header().Set("Content-Type", "text/plain")
				_, err := warcRecord.WarcHeader().Write(w)
				if err != nil {
					return err
				}
				_, err = fmt.Fprintln(w)
				if err != nil {
					return err
				}
				rb, err := warcRecord.Block().RawBytes()
				if err != nil {
					return err
				}
				_, err = io.Copy(w, rb)
				if err != nil {
					return err
				}
				return nil
			}
		case OutputJson:
			cdxj, err := json.Marshal(cdxjToPywbJson(cdxRecord))
			if err != nil {
				return err
			}
			renderWarcContent(w, warcRecord, api, fmt.Sprintf("%s\n", cdxj))
		default:
			cdxj, err := json.Marshal(cdxjToPywbJson(cdxRecord))
			if err != nil {
				return err
			}
			renderWarcContent(w, warcRecord, api, fmt.Sprintf("%s %s %s\n", cdxRecord.Ssu, cdxRecord.Sts, cdxj))
		}

		return nil
	}
}

func renderWarcContent(w http.ResponseWriter, warcRecord gowarc.WarcRecord, api *CdxjServerApi, cdx string) {
	w.Header().Set("Warcserver-Cdx", cdx)
	w.Header().Set("Link", "<"+warcRecord.WarcHeader().Get(gowarc.WarcTargetURI)+">; rel=\"original\"")
	w.Header().Set("WARC-Target-URI", warcRecord.WarcHeader().Get(gowarc.WarcTargetURI))
	w.Header().Set("Warcserver-Source-Coll", api.Collection)
	w.Header().Set("Content-Type", "application/warc-record")
	w.Header().Set("Memento-Datetime", warcRecord.WarcHeader().Get(gowarc.WarcDate))
	w.Header().Set("Warcserver-Type", "warc")

	marshaler := gowarc.NewMarshaler()
	_, bytes, err := marshaler.Marshal(w, warcRecord, 0)
	if err != nil {
		log.Errorf("failed to render warc content: %s", err)
	}
	if bytes <= 0 {
		log.Warnf("rendered %d bytes from the warc content", bytes)
	}
}
