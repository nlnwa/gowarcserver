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
	"syscall"
	"time"

	"github.com/gorilla/handlers"
	"github.com/julienschmidt/httprouter"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/nlnwa/gowarcserver/index"
	"github.com/nlnwa/gowarcserver/internal/badgeridx"
	"github.com/nlnwa/gowarcserver/internal/tikvidx"
	"github.com/nlnwa/gowarcserver/loader"
	"github.com/nlnwa/gowarcserver/server/coreserver"
	"github.com/nlnwa/gowarcserver/server/warcserver"
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

	// server options
	cmd.Flags().IntP("port", "p", 9999, "server port")
	cmd.Flags().String("path-prefix", "", "path prefix for all server endpoints")
	cmd.Flags().Bool("log-requests", false, "log incoming http requests")

	// index options
	cmd.Flags().StringP("index-source", "s", "file", `index source: "file" or "kafka"`)
	cmd.Flags().StringP("index-format", "o", "badger", `index format: "badger", "tikv"`)
	cmd.Flags().StringSlice("index-include", nil, "only include files matching these regular expressions")
	cmd.Flags().StringSlice("index-exclude", nil, "exclude files matching these regular expressions")
	cmd.Flags().Int("index-workers", 8, "number of index workers")

	// auto indexer options
	cmd.Flags().StringSlice("file-paths", []string{"./testdata"}, "list of paths to warc files or directories containing warc files")
	cmd.Flags().Int("file-max-depth", 4, "maximum directory recursion depth")

	// kafka indexer options
	cmd.Flags().StringSlice("kafka-brokers", nil, "the list of broker addresses used to connect to the kafka cluster")
	cmd.Flags().String("kafka-group-id", "", "optional consumer group id")
	cmd.Flags().String("kafka-topic", "", "the topic to read messages from")
	cmd.Flags().Int("kafka-min-bytes", 0, "indicates to the broker the minimum batch size that the consumer will accept")
	cmd.Flags().Int("kafka-max-bytes", 0, "indicates to the broker the maximum batch size that the consumer will accept")
	cmd.Flags().Duration("kafka-max-wait", 0, "maximum amount of time to wait for new data to come when fetching batches of messages from kafka")

	// badger options
	cmd.Flags().String("badger-dir", "./warcdb", "path to index database")
	cmd.Flags().String("badger-database", "", "name of badger database")
	cmd.Flags().Int("badger-batch-max-size", 1000, "max transaction batch size in badger")
	cmd.Flags().Duration("badger-batch-max-wait", 5*time.Second, "max wait time before flushing batched records")
	cmd.Flags().String("badger-compression", badgeridx.SnappyCompression, "compression algorithm")

	// tikv options
	cmd.Flags().StringSlice("tikv-pd-addr", nil, "host:port of TiKV placement driver")
	cmd.Flags().Int("tikv-batch-max-size", 1000, "max transaction batch size")
	cmd.Flags().Duration("tikv-batch-max-wait", 5*time.Second, "max wait time before flushing batched records regardless of max batch size")
	cmd.Flags().String("tikv-database", "", "name of tikv database")

	return cmd
}

func serveCmd(_ *cobra.Command, _ []string) error {
	// parse include patterns
	var includes []*regexp.Regexp
	for _, r := range viper.GetStringSlice("index-include") {
		if re, err := regexp.Compile(r); err != nil {
			return fmt.Errorf("%s: %w", r, err)
		} else {
			includes = append(includes, re)
		}
	}
	// parse exclude patterns
	var excludes []*regexp.Regexp
	for _, r := range viper.GetStringSlice("index-exclude") {
		if re, err := regexp.Compile(r); err != nil {
			return fmt.Errorf("%s: %w", r, err)
		} else {
			excludes = append(excludes, re)
		}
	}

	var writer index.RecordWriter
	var fileApi index.FileAPI
	var cdxApi index.CdxAPI
	var idApi index.IdAPI
	var storageRefResolver loader.StorageRefResolver
	var filePathResolver loader.FilePathResolver

	indexFormat := viper.GetString("index-format")
	switch indexFormat {
	case "badger":
		db, err := badgeridx.NewDB(
			badgeridx.WithCompression(viper.GetString("badger-compression")),
			badgeridx.WithDir(viper.GetString("badger-dir")),
			badgeridx.WithBatchMaxSize(viper.GetInt("badger-batch-max-size")),
			badgeridx.WithBatchMaxWait(viper.GetDuration("badger-batch-max-wait")),
			badgeridx.WithReadOnly(viper.GetString("index-source") == ""),
			badgeridx.WithDatabase(viper.GetString("badger-database")),
		)
		if err != nil {
			return err
		}
		defer db.Close()
		writer = db
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
			tikvidx.WithReadOnly(viper.GetString("index-source") == ""),
		)
		if err != nil {
			return err
		}
		defer db.Close()
		writer = db
		storageRefResolver = db
		filePathResolver = db
		cdxApi = db
		fileApi = db
		idApi = db
	default:
		return fmt.Errorf("unknown index format: %s", indexFormat)
	}

	ctx, cancelIndexer := context.WithCancel(context.Background())
	defer cancelIndexer()

	indexSource := viper.GetString("index-source")
	if indexSource != "" {
		indexer := index.NewIndexer(writer,
			index.WithIncludes(includes...),
			index.WithExcludes(excludes...),
		)
		queue := index.NewWorkQueue(indexer,
			viper.GetInt("index-workers"),
		)
		defer queue.Close()

		var runner index.Runner
		switch indexSource {
		case "file":
			runner = index.NewAutoIndexer(queue,
				index.WithMaxDepth(viper.GetInt("file-max-depth")),
				index.WithPaths(viper.GetStringSlice("file-paths")),
			)
		case "kafka":
			runner = index.NewKafkaIndexer(queue,
				index.WithBrokers(viper.GetStringSlice("kafka-brokers")),
				index.WithGroupID(viper.GetString("kafka-group-id")),
				index.WithTopic(viper.GetString("kafka-topic")),
				index.WithMinBytes(viper.GetInt("kafka-min-bytes")),
				index.WithMaxBytes(viper.GetInt("kafka-max-bytes")),
				index.WithMaxWait(viper.GetDuration("kafka-max-wait")),
			)
		default:
			return fmt.Errorf("unknown index source: %s", indexSource)
		}
		go func() {
			log.Info().Msg("Starting indexer")
			err := runner.Run(ctx)
			if err != nil {
				log.Error().Err(err).Msg("Indexer has stopped")
			}
		}()

	}

	// create record loader
	l := &loader.Loader{
		StorageRefResolver: storageRefResolver,
		RecordLoader:       loader.FileStorageLoader{FilePathResolver: filePathResolver},
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

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigs
		log.Info().Msgf("Received %s signal, shutting down...", sig)

		cancelIndexer()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := httpServer.Shutdown(ctx)
		if err != nil {
			log.Error().Msgf("Failed to shut down server: %v", err)
		}
	}()

	log.Info().Msgf("Starting server at :%v", port)

	err := httpServer.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
