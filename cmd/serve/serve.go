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
	"github.com/nlnwa/gowarcserver/internal/server/coreserver"
	"net/http"
	"os"

	"github.com/nlnwa/gowarcserver/internal/config"
	"github.com/nlnwa/gowarcserver/internal/database"
	"github.com/nlnwa/gowarcserver/internal/index"
	"github.com/nlnwa/gowarcserver/internal/loader"
	"github.com/nlnwa/gowarcserver/internal/server"
	"github.com/nlnwa/gowarcserver/internal/server/warcserver"

	"github.com/dgraph-io/badger/v3/options"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func NewCommand() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "serve",
		Short: "Start a warc server",
		RunE:  serveCmd,
	}

	// defaults
	port := 9999
	watch := false
	enableIndexing := false
	indexDbDir := "."
	indexDepth := 4
	indexWorkers := 8
	indexTargets := []string{"."}
	suffixes := []string{".warc", ".warc.gz"}
	compression := config.SnappyCompression
	logRequests := false

	cmd.Flags().IntP("port", "p", port, "server port")
	cmd.Flags().StringSlice("include", suffixes, "only include filenames matching these suffixes")
	cmd.Flags().BoolP("index", "a", enableIndexing, "enable indexing")
	cmd.Flags().IntP("max-depth", "w", indexDepth, "maximum directory recursion depth")
	cmd.Flags().Int("workers", indexWorkers, "number of index workers")
	cmd.Flags().StringSlice("dirs", indexTargets, "directories to search for warc files in")
	cmd.Flags().Bool("watch", watch, "watch files for changes")
	cmd.Flags().String("db-dir", indexDbDir, "path to index database")
	cmd.Flags().String("compression", compression, "database compression type: 'none', 'snappy' or 'zstd'")
	cmd.Flags().Bool("log-requests", logRequests, "log http requests")
	if err := viper.BindPFlags(cmd.Flags()); err != nil {
		log.Fatalf("Failed to bind serve flags, err: %v", err)
	}

	return cmd
}

func serveCmd(cmd *cobra.Command, args []string) error {
	// collect paths from args and --dirs flag
	dirs := viper.GetStringSlice("dirs")
	dirs = append(dirs, args...)

	var c options.CompressionType
	if err := viper.UnmarshalKey("compression", &c, viper.DecodeHook(config.CompressionDecodeHookFunc())); err != nil {
		return err
	}
	db, err := database.NewCdxIndexDb(
		database.WithCompression(c),
		database.WithDir(viper.GetString("db-dir")),
	)
	if err != nil {
		return err
	}
	defer db.Close()

	if viper.GetBool("index") {
		log.Infof("Starting auto indexer")

		cdxDb := &index.CdxDb{CdxDbIndex: db}

		indexWorker := index.NewIndexWorker(cdxDb, viper.GetInt("workers"))
		defer indexWorker.Close()

		autoIndexer, err := index.NewAutoIndexer(indexWorker.Accept, dirs,
			index.WithWatch(viper.GetBool("watch")),
			index.WithMaxDepth(viper.GetInt("max-depth")),
			index.WithSuffixes(viper.GetStringSlice("include")...))
		if err != nil {
			return err
		}
		defer autoIndexer.Close()
	}

	l := &loader.Loader{
		Resolver: db,
		Loader: &loader.FileStorageLoader{FilePathResolver: func(fileName string) (filePath string, err error) {
			fileInfo, err := db.GetFileInfo(fileName)
			return fileInfo.Path, err
		}},
		NoUnpack: false,
	}

	r := mux.NewRouter()

	if viper.GetBool("log-requests") {
		loggingMw := func(h http.Handler) http.Handler {
			return handlers.CombinedLoggingHandler(os.Stdout, h)
		}
		r.Use(loggingMw)
	}

	coreserver.Register(r, l, db)
	warcserver.Register(r.PathPrefix("/warcserver").Subrouter(), l, db)

	if err := server.Serve(viper.GetInt("port"), r); errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
