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

package serve

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"runtime"
	"time"

	"github.com/dgraph-io/badger/v3/options"
	"github.com/gorilla/handlers"
	"github.com/julienschmidt/httprouter"
	"github.com/nlnwa/gowarcserver/internal/index"
	"github.com/nlnwa/gowarcserver/internal/loader"
	"github.com/nlnwa/gowarcserver/internal/server"
	"github.com/nlnwa/gowarcserver/internal/server/coreserver"
	"github.com/nlnwa/gowarcserver/internal/server/warcserver"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start warc server",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			// Increase GOMAXPROCS as recommended by badger
			// https://github.com/dgraph-io/badger#are-there-any-go-specific-settings-that-i-should-use
			runtime.GOMAXPROCS(128)
		},
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if err := viper.BindPFlags(cmd.Flags()); err != nil {
				return fmt.Errorf("failed to bind flags, err: %w", err)
			}
			return nil
		},
		RunE: serveCmd,
	}
	// defaults
	port := 9999
	watch := false
	enableIndexing := true
	indexDbDir := "."
	indexDepth := 4
	indexWorkers := 8
	indexDbBatchMaxSize := 1000
	indexDbBatchMaxWait := 5 * time.Second
	compression := index.SnappyCompression
	logRequests := false
	readOnly := false
	pathPrefix := ""

	cmd.Flags().IntP("port", "p", port, "server port")
	cmd.Flags().String("proxy-url", "", "url to a gowarc server proxy that will be used to resolve records")
	cmd.Flags().String("path-prefix", pathPrefix, "prefix for all server endpoints")
	cmd.Flags().StringSlice("include", nil, "only include files matching these regular expressions")
	cmd.Flags().StringSlice("exclude", nil, "exclude files matching these regular expressions")
	cmd.Flags().BoolP("index", "a", enableIndexing, "enable indexing")
	cmd.Flags().IntP("max-depth", "w", indexDepth, "maximum directory recursion depth")
	cmd.Flags().Int("workers", indexWorkers, "number of index workers")
	cmd.Flags().StringSlice("dirs", nil, "directories to search for warc files in")
	cmd.Flags().Bool("watch", watch, "watch files for changes")
	cmd.Flags().String("db-dir", indexDbDir, "path to index database")
	cmd.Flags().Int("db-batch-max-size", indexDbBatchMaxSize, "max transaction batch size in badger")
	cmd.Flags().Bool("db-read-only", readOnly, "set database to read only")
	cmd.Flags().Duration("db-batch-max-wait", indexDbBatchMaxWait, "max wait time before flushing batched records")
	cmd.Flags().String("compression", compression, "database compression type: 'none', 'snappy' or 'zstd'")
	cmd.Flags().Bool("log-requests", logRequests, "log http requests")

	return cmd
}

func serveCmd(cmd *cobra.Command, args []string) error {
	// collect paths from args or flag
	var dirs []string
	if len(args) > 0 {
		dirs = append(dirs, args...)
	} else {
		dirs = viper.GetStringSlice("dirs")
	}
	// parse database compression type
	var c options.CompressionType
	if err := viper.UnmarshalKey("compression", &c, viper.DecodeHook(index.CompressionDecodeHookFunc())); err != nil {
		return err
	}
	// parse include patterns
	var includes []*regexp.Regexp
	for _, r := range viper.GetStringSlice("include") {
		if re, err := regexp.Compile(r); err != nil {
			return fmt.Errorf("%s: %w", r, err)
		} else {
			includes = append(includes, re)
		}
	}
	// parse exclude patterns
	var excludes []*regexp.Regexp
	for _, r := range viper.GetStringSlice("exclude") {
		if re, err := regexp.Compile(r); err != nil {
			return fmt.Errorf("%s: %w", r, err)
		} else {
			excludes = append(excludes, re)
		}
	}
	// parse proxy url
	var proxyUrl *url.URL
	proxyStr := viper.GetString("proxy-url")
	if proxyStr != "" {
		var err error
		proxyUrl, err = url.Parse(proxyStr)
		if err != nil {
			return err
		}
	}
	// create index database
	db, err := index.NewDB(
		index.WithCompression(c),
		index.WithDir(viper.GetString("db-dir")),
		index.WithBatchMaxSize(viper.GetInt("db-batch-max-size")),
		index.WithBatchMaxWait(viper.GetDuration("db-batch-max-wait")),
		index.WithReadOnly(viper.GetBool("db-read-only")),
	)
	if err != nil {
		return err
	}
	defer db.Close()
	// optionally start autoindexer
	if viper.GetBool("index") {
		log.Info().Msg("Starting auto indexer")

		indexWorker := index.Worker(db, viper.GetInt("workers"))
		defer indexWorker.Close()

		indexer, err := index.NewAutoIndexer(indexWorker,
			index.WithWatch(viper.GetBool("watch")),
			index.WithMaxDepth(viper.GetInt("max-depth")),
			index.WithIncludes(includes...),
			index.WithExcludes(excludes...),
		)
		if err != nil {
			return err
		}
		defer indexer.Close()

		for _, dir := range dirs {
			dir := dir
			go func() {
				if err := indexer.Index(dir); err != nil {
					log.Warn().Msgf(`Error indexing "%s": %v`, dir, err)
				}
			}()
		}
	}
	// create record loader
	l := &loader.Loader{
		StorageRefResolver: db,
		RecordLoader:       &loader.FileStorageLoader{FilePathResolver: db},
		NoUnpack:           false,
		ProxyUrl:           proxyUrl,
	}
	// middleware chain
	mw := func(h http.Handler) http.Handler {
		return h
	}
	// optionally add logging middleware
	if viper.GetBool("log-requests") {
		mw = func(h http.Handler) http.Handler {
			return handlers.CombinedLoggingHandler(os.Stdout, h)
		}
	}
	// create http router
	r := httprouter.New()
	// register routes
	pathPrefix := viper.GetString("path-prefix")
	warcserver.Register(r, mw, pathPrefix+"/warcserver", l, db)
	coreserver.Register(r, mw, pathPrefix, l, db)
	// start server
	if err := server.Serve(viper.GetInt("port"), r); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
