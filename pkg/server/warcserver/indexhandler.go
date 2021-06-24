package warcserver

import (
	"encoding/json"
	"fmt"
	"net/http"

	cdx "github.com/nlnwa/gowarcserver/proto"
)

type IndexHandler struct {
	DbCdxServer
}

func (ih *IndexHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	api, err := ParseCdxjApi(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	n, err := ih.search(api, ih.render(w, api))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else if n == 0 {
		http.NotFound(w, r)
	}
}

func (ih *IndexHandler) render(w http.ResponseWriter, api *CdxjServerApi) RenderFunc {
	return func(record *cdx.Cdx) error {
		cdxj, err := json.Marshal(cdxjToPywbJson(record))
		if err != nil {
			return err
		}
		switch api.Output {
		case OutputJson:
			_, err = fmt.Fprintf(w, "%s\n", cdxj)
		default:
			_, err = fmt.Fprintf(w, "%s %s %s\n", record.Ssu, record.Sts, cdxj)
		}
		return err
	}
}
