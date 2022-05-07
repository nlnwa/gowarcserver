package api

import (
	"net/http"
	"runtime"

	"github.com/rs/zerolog/log"
)

func HandleError(w http.ResponseWriter, n int, err error) {
	// try to log error with caller information
	_, file, no, ok := runtime.Caller(1)
	if ok {
		log.Error().Err(err).Msgf("%s#%d\n", file, no)
	} else {
		log.Error().Err(err).Msg("")
	}
}
