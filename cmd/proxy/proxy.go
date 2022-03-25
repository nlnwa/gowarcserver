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
	"fmt"
	"net/http"
	"os"
	"time"

	gHandlers "github.com/gorilla/handlers"
	"github.com/julienschmidt/httprouter"
	"github.com/nlnwa/gowarcserver/internal/server"
	"github.com/nlnwa/gowarcserver/internal/server/handlers"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "proxy",
		Short: "Start proxy server",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if err := viper.BindPFlags(cmd.Flags()); err != nil {
				return fmt.Errorf("failed to bind flags, err: %w", err)
			}
			return nil
		},
		RunE: proxyCmd,
	}
	// defaults
	port := 9998
	childUrls := []string{}
	childQueryTimeout := 300 * time.Millisecond

	cmd.Flags().IntP("port", "p", port, "Server port")
	cmd.Flags().StringSliceP("child-urls", "u", childUrls, "List of URLs to other gowarcserver instances, queries are propagated to these urls")
	cmd.Flags().DurationP("child-query-timeout", "t", childQueryTimeout, "Time before query to child node times out")

	return cmd
}

func proxyCmd(_ *cobra.Command, _ []string) error {
	childUrls := ParseUrls(viper.GetStringSlice("child-urls"))
	childQueryTimeout := viper.GetDuration("child-query-timeout")
	port := viper.GetInt("port")
	r := httprouter.New()

	middleware := func(h http.Handler) http.Handler {
		return gHandlers.CombinedLoggingHandler(os.Stdout, h)
	}

	indexHandler := handlers.AggregatedHandler(childUrls, childQueryTimeout)
	resourceHandler := handlers.FirstHandler(childUrls, childQueryTimeout)
	contentHandler := handlers.FirstHandler(childUrls, childQueryTimeout)

	r.Handler("GET", "/warcserver/cdx", middleware(indexHandler))
	r.Handler("GET", "/warcserver/web", middleware(resourceHandler))
	r.Handler("GET", "/id/:id", middleware(contentHandler))

	return server.Serve(port, r)
}
