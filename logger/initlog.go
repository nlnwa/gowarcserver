/*
 * Copyright 2021 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package logger

import (
	stdlog "log"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func InitLog(level string, format string, logCaller bool) {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	switch strings.ToLower(level) {
	case "panic":
		log.Logger = log.Level(zerolog.PanicLevel)
	case "fatal":
		log.Logger = log.Level(zerolog.FatalLevel)
	case "error":
		log.Logger = log.Level(zerolog.ErrorLevel)
	case "warn":
		log.Logger = log.Level(zerolog.WarnLevel)
	case "info":
		log.Logger = log.Level(zerolog.InfoLevel)
	case "debug":
		log.Logger = log.Level(zerolog.DebugLevel)
	case "trace":
		log.Logger = log.Level(zerolog.TraceLevel)
	default:
		log.Logger = log.Level(zerolog.Disabled)
	}

	if format == "logfmt" {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	}

	if logCaller {
		log.Logger = log.With().Caller().Logger()
	}

	stdlog.SetFlags(0)
	stdlog.SetOutput(log.Logger)
}
