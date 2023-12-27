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

package index

import (
	"context"
	"fmt"
	"os/signal"
	"regexp"
	"syscall"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/nlnwa/gowarcserver/index"
	"github.com/nlnwa/gowarcserver/internal/badgeridx"
	"github.com/nlnwa/gowarcserver/internal/tikvidx"
)

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "index",
		Short: "Index warc file(s)",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if err := viper.BindPFlags(cmd.Flags()); err != nil {
				return fmt.Errorf("failed to bind flags: %w", err)
			}
			return nil
		},
		RunE: indexCmd,
	}
	// index options
	cmd.Flags().StringP("index-source", "s", "file", `index source: "file" or "kafka"`)
	cmd.Flags().StringP("index-format", "o", "cdxj", `index format: "cdxj", "cdxpb", "toc", badger", "tikv"`)
	cmd.Flags().StringSlice("index-include", nil, "only include files matching these regular expressions")
	cmd.Flags().StringSlice("index-exclude", nil, "exclude files matching these regular expressions")
	cmd.Flags().Int("index-workers", 8, "number of index workers")

	// auto indexer options
	cmd.Flags().StringSliceP("file-paths", "f", []string{"./testdata"}, "directories to search for warc files in")
	cmd.Flags().Int("file-max-depth", 4, "maximum directory recursion")

	// kafka indexer options
	cmd.Flags().StringSlice("kafka-brokers", nil, "the list of broker addresses used to connect to the kafka cluster")
	cmd.Flags().String("kafka-group-id", "", "optional consumer group id")
	cmd.Flags().String("kafka-topic", "", "the topic to read messages from")
	cmd.Flags().Int("kafka-min-bytes", 0, "indicates to the broker the minimum batch size that the consumer will accept")
	cmd.Flags().Int("kafka-max-bytes", 0, "indicates to the broker the maximum batch size that the consumer will accept")
	cmd.Flags().Duration("kafka-max-wait", 0, "maximum amount of time to wait for new data to come when fetching batches of messages from kafka")

	// toc indexer options
	cmd.Flags().Uint("toc-bloom-capacity", uint(1000), "estimated bloom filter capacity")
	cmd.Flags().Float64("toc-bloom-fp", 0.01, "estimated bloom filter false positive rate")

	// badger options
	cmd.Flags().String("badger-dir", "./warcdb", "path to index database")
	cmd.Flags().String("badger-database", "", "name of badger database")
	cmd.Flags().Int("badger-batch-max-size", 1000, "max transaction batch size in badger")
	cmd.Flags().Duration("badger-batch-max-wait", 5*time.Second, "max wait time before flushing batched records")
	cmd.Flags().String("badger-compression", badgeridx.SnappyCompression, "compression algorithm")
	cmd.Flags().Bool("badger-read-only", false, "run badger in read-only mode")

	// tikv options
	cmd.Flags().StringSlice("tikv-pd-addr", nil, "host:port of TiKV placement driver")
	cmd.Flags().Int("tikv-batch-max-size", 1000, "max transaction batch size")
	cmd.Flags().Duration("tikv-batch-max-wait", 5*time.Second, "max wait time before flushing batched records regardless of max batch size")
	cmd.Flags().String("tikv-database", "", "name of tikv database")

	return cmd
}

func indexCmd(_ *cobra.Command, _ []string) error {
	var w index.RecordWriter

	indexFormat := viper.GetString("index-format")
	switch indexFormat {
	case "cdxj":
		w = index.CdxJ{}
	case "cdxpb":
		w = index.CdxPb{}
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
		w = db
	case "badger":
		db, err := badgeridx.NewDB(
			badgeridx.WithCompression(viper.GetString("badger-compression")),
			badgeridx.WithDir(viper.GetString("badger-dir")),
			badgeridx.WithBatchMaxSize(viper.GetInt("badger-batch-max-size")),
			badgeridx.WithBatchMaxWait(viper.GetDuration("badger-batch-max-wait")),
			badgeridx.WithDatabase(viper.GetString("badger-database")),
		)
		if err != nil {
			return err
		}
		defer db.Close()
		w = db
	case "toc":
		w = &index.Toc{
			BloomFilter: bloom.NewWithEstimates(viper.GetUint("toc-bloom-capacity"), viper.GetFloat64("toc-bloom-fp")),
		}
	default:
		return fmt.Errorf("unknown index format: %s", indexFormat)
	}

	var includes []*regexp.Regexp
	for _, r := range viper.GetStringSlice("index-include") {
		if re, err := regexp.Compile(r); err != nil {
			return fmt.Errorf("%s: %w", r, err)
		} else {
			includes = append(includes, re)
		}
	}

	var excludes []*regexp.Regexp
	for _, r := range viper.GetStringSlice("index-exclude") {
		if re, err := regexp.Compile(r); err != nil {
			return fmt.Errorf("%s: %w", r, err)
		} else {
			excludes = append(excludes, re)
		}
	}

	indexer := index.NewIndexer(w,
		index.WithIncludes(includes...),
		index.WithExcludes(excludes...),
	)
	queue := index.NewWorkQueue(indexer,
		viper.GetInt("index-workers"),
	)
	defer queue.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var runner index.Runner

	indexSource := viper.GetString("index-source")
	switch indexSource {
	case "file":
		runner = index.NewAutoIndexer(queue,
			index.WithMaxDepth(viper.GetInt("file-max-depth")),
			index.WithPaths(viper.GetStringSlice("file-paths")),
			index.WithExcludeDirs(excludes...),
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

	log.Info().Msg("Starting indexer")

	return runner.Run(ctx)
}
