package api

import (
	"github.com/rs/zerolog/log"
	"net/http"
	"runtime"
)

func HandleError(w http.ResponseWriter, n int, err error) {
	// assume nothing has been written to client yet
	if n == 0 {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	// try to log error with caller information
	_, file, no, ok := runtime.Caller(1)
	if ok {
		log.Error().Err(err).Msgf("%s#%d\n", file, no)
	} else {
		log.Error().Err(err).Msg("")
	}
}
