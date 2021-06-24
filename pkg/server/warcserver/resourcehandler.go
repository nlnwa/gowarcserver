package warcserver

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nlnwa/gowarc/warcoptions"
	"github.com/nlnwa/gowarc/warcwriter"
	"io"
	"net/http"

	"github.com/nlnwa/gowarc/warcrecord"
	"github.com/nlnwa/gowarcserver/pkg/loader"
	cdx "github.com/nlnwa/gowarcserver/proto"
)

type ResourceHandler struct {
	DbCdxServer
	loader loader.ResourceLoader
}

func (rh *ResourceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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

func (rh *ResourceHandler) render(w http.ResponseWriter, api *CdxjServerApi) RenderFunc {
	return func(record * cdx.Cdx) error{
		warcId := record.Rid
		if len(warcId) > 0 && warcId[0] != '<'{
			warcId = "<" + warcId + ">"
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		warcRecord, err := rh.loader.Get(ctx, warcId)
		if err != nil{
			return err
		}
		defer warcRecord.Close()

		switch api.Output{
		case "content":
			switch v := warcRecord.Block().(type){
			case warcrecord.HttpResponseBlock:
				r, err := v.Response()
				if err != nil{
					return err
				}
				return renderContent(w, r, v)
			default:
				return renderBlock(w, warcRecord)
			}
		case OutputJson:
			cdxj, err := json.Marshal(cdxjToPywbJson(record))
			if err != nil{
				return err
			}
			renderWarcContent(w, warcRecord, api, fmt.Sprintf("%s\n", cdxj))
		default:
			cdxj, err := json.Marshal(cdxjToPywbJson(record))
			if err != nil{
				return err
			}
			renderWarcContent(w, warcRecord, api, fmt.Sprintf("%s %s %s\n", record.Ssu, record.Sts, cdxj))
		}

		return nil
	}
}

func renderBlock(w http.ResponseWriter, record warcrecord.WarcRecord) error {
	w.Header().Set("Content-Type", "text/plain")

	_, err := record.WarcHeader().Write(w)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w)
	if err != nil {
		return err
	}
	rb, err := record.Block().RawBytes()
	if err != nil {
		return err
	}
	_, err = io.Copy(w, rb)
	if err != nil {
		return err
	}
	return nil
}

func renderWarcContent(w http.ResponseWriter, warcRecord warcrecord.WarcRecord, api *CdxjServerApi, cdx string) {
	w.Header().Set("Warcserver-Cdx", cdx)
	w.Header().Set("Link", "<"+warcRecord.WarcHeader().Get(warcrecord.WarcTargetURI)+">; rel=\"original\"")
	w.Header().Set("WARC-Target-URI", warcRecord.WarcHeader().Get(warcrecord.WarcTargetURI))
	w.Header().Set("Warcserver-Source-Coll", api.Collection)
	w.Header().Set("Content-Type", "application/warc-record")
	w.Header().Set("Memento-Datetime", warcRecord.WarcHeader().Get(warcrecord.WarcDate))
	w.Header().Set("Warcserver-Type", "warc")

	warcWriter := warcwriter.NewWriter(&warcoptions.WarcOptions{
		Strict:   false,
		Compress: false,
	})
	_, err := warcWriter.WriteRecord(w, warcRecord)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

func renderContent(w http.ResponseWriter, r *http.Response, v warcrecord.PayloadBlock) error {
	for k, vl := range r.Header {
		for _, v := range vl {
			w.Header().Set(k, v)
		}
	}
	w.WriteHeader(r.StatusCode)
	p, err := v.PayloadBytes()
	if err != nil {
		return fmt.Errorf("failed to retrieve payload bytes for request to %s", r.Request.URL)
	}

	_, err = io.Copy(w, p)
	if err != nil {
		return fmt.Errorf("failed to write content for request to %s", r.Request.URL)
	}
	return nil
}
