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
	"github.com/dgraph-io/badger/v3/options"
	"github.com/gorilla/handlers"
	"github.com/julienschmidt/httprouter"
	"github.com/nlnwa/gowarcserver/internal/config"
	"github.com/nlnwa/gowarcserver/internal/database"
	"github.com/nlnwa/gowarcserver/internal/index"
	"github.com/nlnwa/gowarcserver/internal/loader"
	"github.com/nlnwa/gowarcserver/internal/server"
	"github.com/nlnwa/gowarcserver/internal/server/coreserver"
	"github.com/nlnwa/gowarcserver/internal/server/warcserver"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"net/http"
	"os"
	"regexp"
	"runtime"
	"time"
)

func NewCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Start warc server",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			// Increase GOMAXPROCS as recommended by badger
			// https://github.com/dgraph-io/badger#are-there-any-go-specific-settings-that-i-should-use
			runtime.GOMAXPROCS(128)
		},
		PreRunE: func(cmd *cobra.Command, args []string) error {
			// defaults
			port := 9999
			watch := false
			enableIndexing := true
			indexDbDir := "."
			indexDepth := 4
			indexWorkers := 8
			indexDbBatchMaxSize := 1000
			indexDbBatchMaxWait := 5 * time.Second
			compression := config.SnappyCompression
			logRequests := false

			cmd.Flags().IntP("port", "p", port, "server port")
			cmd.Flags().StringSlice("include", nil, "only include files matching these regular expressions")
			cmd.Flags().StringSlice("exclude", nil, "exclude files matching these regular expressions")
			cmd.Flags().BoolP("index", "a", enableIndexing, "enable indexing")
			cmd.Flags().IntP("max-depth", "w", indexDepth, "maximum directory recursion depth")
			cmd.Flags().Int("workers", indexWorkers, "number of index workers")
			cmd.Flags().StringSlice("dirs", nil, "directories to search for warc files in")
			cmd.Flags().Bool("watch", watch, "watch files for changes")
			cmd.Flags().String("db-dir", indexDbDir, "path to index database")
			cmd.Flags().Int("db-batch-max-size", indexDbBatchMaxSize, "max transaction batch size in badger")
			cmd.Flags().Duration("db-batch-max-wait", indexDbBatchMaxWait, "max transaction batch size in badger")
			cmd.Flags().String("compression", compression, "database compression type: 'none', 'snappy' or 'zstd'")
			cmd.Flags().Bool("log-requests", logRequests, "log http requests")

			if err := viper.BindPFlags(cmd.Flags()); err != nil {
				return fmt.Errorf("failed to bind flags, err: %v", err)
			}
			return nil
		},
		RunE: serveCmd,
	}
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
	if err := viper.UnmarshalKey("compression", &c, viper.DecodeHook(config.CompressionDecodeHookFunc())); err != nil {
		return err
	}
	// create database instance
	db, err := database.NewCdxIndexDb(
		database.WithCompression(c),
		database.WithDir(viper.GetString("db-dir")),
		database.WithBatchMaxSize(viper.GetInt("db-batch-max-size")),
		database.WithBatchMaxWait(viper.GetDuration("db-batch-max-wait")),
	)
	if err != nil {
		return err
	}
	defer db.Close()

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
	// optionally start autoindexer
	if viper.GetBool("index") {
		log.Info().Msg("Starting auto indexer")

		cdxDb := index.CdxDb{CdxDbIndex: db}

		indexWorker := index.Worker(cdxDb, viper.GetInt("workers"))
		defer indexWorker.Close()

		autoIndexer, err := index.NewAutoIndexer(indexWorker, dirs,
			index.WithWatch(viper.GetBool("watch")),
			index.WithMaxDepth(viper.GetInt("max-depth")),
			index.WithIncludes(includes...),
			index.WithExcludes(excludes...),
		)
		if err != nil {
			return err
		}
		defer autoIndexer.Close()
	}

	// create record loader
	l := &loader.Loader{
		Resolver: db,
		Loader: &loader.FileStorageLoader{FilePathResolver: func(fileName string) (filePath string, err error) {
			fileInfo, err := db.GetFileInfo(fileName)
			return fileInfo.Path, err
		}},
		NoUnpack: false,
	}

	// middleware chain
	var mw func(http.Handler) http.Handler

	// optionally add logging middleware
	if viper.GetBool("log-requests") {
		mw = func(h http.Handler) http.Handler {
			return handlers.CombinedLoggingHandler(os.Stdout, h)
		}
	}

	// create http router
	r := httprouter.New()

	// register
	warcserver.Register(r, mw, "/warcserver", l, db)
	coreserver.Register(r, mw, "", l, db)

	if err := server.Serve(viper.GetInt("port"), r); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
