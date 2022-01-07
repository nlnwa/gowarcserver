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

package proxy

import (
	"net/http"
	"os"
	"time"

	"github.com/nlnwa/gowarcserver/internal/server"
	"github.com/nlnwa/gowarcserver/internal/server/warcserver"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func NewCommand() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "proxy",
		Short: "Start proxy server",
		RunE:  proxyCmd,
	}

	// defaults
	port := 9998
	childUrls := []string{}
	childQueryTimeout := 300 * time.Millisecond

	cmd.Flags().IntP("port", "p", port, "Server port")
	cmd.Flags().StringSliceP("child-urls", "u", childUrls, "List of URLs to other gowarcserver instances, queries are propagated to these urls")
	cmd.Flags().DurationP("child-query-timeout", "t", childQueryTimeout, "Time before query to child node times out")
	if err := viper.BindPFlags(cmd.Flags()); err != nil {
		log.Fatalf("Failed to bind flags, err: %v", err)
	}

	return cmd
}

func proxyCmd(_ *cobra.Command, _ []string) error {
	childUrls := ParseUrls(viper.GetStringSlice("childUrls"))
	childQueryTimeout := viper.GetDuration("childQueryTimeout")
	port := viper.GetInt("port")
	r := mux.NewRouter()

	loggingMw := func(h http.Handler) http.Handler {
		return handlers.CombinedLoggingHandler(os.Stdout, h)
	}
	r.Use(loggingMw)

	warcserver.RegisterProxy(r, childUrls, childQueryTimeout)

	return server.Serve(port, r)
}
