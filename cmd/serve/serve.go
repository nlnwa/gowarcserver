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
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"syscall"
	"time"

	"github.com/dgraph-io/badger/v3/options"
	"github.com/gorilla/handlers"
	"github.com/julienschmidt/httprouter"
	"github.com/nlnwa/gowarcserver/index"
	"github.com/nlnwa/gowarcserver/internal/badgeridx"
	"github.com/nlnwa/gowarcserver/internal/tikvidx"
	"github.com/nlnwa/gowarcserver/loader"
	"github.com/nlnwa/gowarcserver/server/coreserver"
	"github.com/nlnwa/gowarcserver/server/warcserver"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start warc server",
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
	storageEngine := "badger"
	autoIndex := true
	indexDepth := 4
	indexWorkers := 8
	badgerDir := "./warcdb"
	badgerCompression := "snappy"
	badgerDatabase := ""
	badgerBatchMaxSize := 1000
	badgerBatchMaxWait := 5 * time.Second
	badgerReadOnly := false
	logRequests := false
	tikvPdAddr := []string{}
	tikvBatchMaxSize := 255
	tikvBatchMaxWait := 5 * time.Second
	tikvDatabase := ""
	pathPrefix := ""

	cmd.Flags().IntP("port", "p", port, "server port")
	cmd.Flags().String("path-prefix", pathPrefix, "prefix for all server endpoints")
	cmd.Flags().StringSlice("include", nil, "only include files matching these regular expressions")
	cmd.Flags().StringSlice("exclude", nil, "exclude files matching these regular expressions")
	cmd.Flags().IntP("max-depth", "w", indexDepth, "maximum directory recursion depth")
	cmd.Flags().Int("workers", indexWorkers, "number of index workers")
	cmd.Flags().StringSlice("dirs", nil, "directories to search for warc files in")
	cmd.Flags().Bool("index", autoIndex, "run indexing")
	cmd.Flags().Bool("watch", watch, "watch files for changes")
	cmd.Flags().Bool("log-requests", logRequests, "log http requests")
	cmd.Flags().StringP("storage-engine", "b", storageEngine, `storage engine: "badger" or "tikv"`)
	cmd.Flags().String("badger-dir", badgerDir, "path to index database")
	cmd.Flags().String("badger-database", badgerDatabase, "name of badger database")
	cmd.Flags().String("badger-compression", badgerCompression, "compression algorithm")
	cmd.Flags().Int("badger-batch-max-size", badgerBatchMaxSize, "max transaction batch size in badger")
	cmd.Flags().Duration("badger-batch-max-wait", badgerBatchMaxWait, "max wait time before flushing batched records")
	cmd.Flags().Bool("badger-read-only", badgerReadOnly, "run badger read-only")
	cmd.Flags().StringSlice("tikv-pd-addr", tikvPdAddr, "host:port of TiKV placement driver")
	cmd.Flags().Int("tikv-batch-max-size", tikvBatchMaxSize, "max transaction batch size")
	cmd.Flags().Duration("tikv-batch-max-wait", tikvBatchMaxWait, "max wait time before flushing batched records regardless of max batch size")
	cmd.Flags().String("tikv-database", tikvDatabase, "name of tikv database")

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

	var indexer index.Indexer
	var fileApi index.FileAPI
	var cdxApi index.CdxAPI
	var idApi index.IdAPI
	var storageRefResolver loader.StorageRefResolver
	var filePathResolver loader.FilePathResolver

	switch viper.GetString("storage-engine") {
	case "badger":
		// Increase GOMAXPROCS as recommended by badger
		// https://github.com/dgraph-io/badger#are-there-any-go-specific-settings-that-i-should-use
		runtime.GOMAXPROCS(128)

		// parse badger compression type
		var c options.CompressionType
		if err := viper.UnmarshalKey("badger-compression", &c, viper.DecodeHook(badgeridx.CompressionDecodeHookFunc())); err != nil {
			return err
		}

		db, err := badgeridx.NewDB(
			badgeridx.WithCompression(c),
			badgeridx.WithDir(viper.GetString("badger-dir")),
			badgeridx.WithBatchMaxSize(viper.GetInt("badger-batch-max-size")),
			badgeridx.WithBatchMaxWait(viper.GetDuration("badger-batch-max-wait")),
			badgeridx.WithReadOnly(viper.GetBool("badger-read-only")),
			badgeridx.WithDatabase(viper.GetString("badger-database")),
		)
		if err != nil {
			return err
		}
		defer db.Close()
		indexer = db
		storageRefResolver = db
		filePathResolver = db
		cdxApi = db
		fileApi = db
		idApi = db
	case "tikv":
		db, err := tikvidx.NewDB(
			tikvidx.WithPDAddress(viper.GetStringSlice("tikv-pd-addr")),
			tikvidx.WithBatchMaxSize(viper.GetInt("tikv-batch-max-size")),
			tikvidx.WithBatchMaxWait(viper.GetDuration("tikv-batch-max-wait")),
			tikvidx.WithDatabase(viper.GetString("tikv-database")),
		)
		if err != nil {
			return err
		}
		defer db.Close()
		indexer = db
		storageRefResolver = db
		filePathResolver = db
		cdxApi = db
		fileApi = db
		idApi = db
	default:
		return fmt.Errorf("invalid storage engine")
	}

	// optionally start autoindexer
	if viper.GetBool("index") {
		log.Info().Msg("Starting auto indexer")

		indexWorker := index.NewWorker(indexer, viper.GetInt("workers"))
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
		StorageRefResolver: storageRefResolver,
		FileStorageLoader:  loader.FileStorageLoader{FilePathResolver: filePathResolver},
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
	handler := httprouter.New()
	// register routes
	pathPrefix := viper.GetString("path-prefix")

	// register warcserver API
	warcserver.Register(warcserver.Handler{
		CdxAPI:     cdxApi,
		FileAPI:    fileApi,
		IdAPI:      idApi,
		WarcLoader: l,
	}, handler, mw, pathPrefix+"/warcserver")

	// register core API
	coreserver.Register(coreserver.Handler{
		CdxAPI:             cdxApi,
		FileAPI:            fileApi,
		IdAPI:              idApi,
		StorageRefResolver: storageRefResolver,
		WarcLoader:         l,
	}, handler, mw, pathPrefix)

	port := viper.GetInt("port")
	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: handler,
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		log.Debug().Msgf("Received %s signal, shutting down server...", sig)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := httpServer.Shutdown(ctx)
		if err != nil {
			log.Error().Msgf("Failed to shut down server: %v", err)
		}
	}()

	log.Info().Msgf("Starting web server at :%v", port)

	err := httpServer.ListenAndServe()
	if !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
