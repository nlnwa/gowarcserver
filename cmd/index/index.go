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
	"fmt"
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/dgraph-io/badger/v3/options"
	"github.com/nlnwa/gowarcserver/internal/config"
	"github.com/nlnwa/gowarcserver/internal/database"
	"github.com/nlnwa/gowarcserver/internal/index"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"regexp"
	"runtime"
	"time"
)

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "index [dir] ...",
		Short: "Index warc file(s)",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if err := viper.BindPFlags(cmd.Flags()); err != nil {
				return fmt.Errorf("failed to bind flags: %w", err)
			}
			return nil
		},
		RunE: indexCmd,
	}
	// defaults
	format := "cdxj"
	indexDbDir := "."
	compression := config.SnappyCompression
	indexDepth := 4
	indexWorkers := 8
	indexDbBatchMaxSize := 1000
	indexDbBatchMaxWait := 5 * time.Second
	bloomCapacity := uint(1000)
	bloomFp := 0.01

	cmd.Flags().StringP("format", "f", format, `index format: "cdxj", "cdxpb", "cdxdb" or "toc"`)
	cmd.Flags().StringSlice("include", nil, "only include files matching these regular expressions")
	cmd.Flags().StringSlice("exclude", nil, "exclude files matching these regular expressions")
	cmd.Flags().IntP("max-depth", "d", indexDepth, "maximum directory recursion")
	cmd.Flags().Int("workers", indexWorkers, "number of index workers")
	cmd.Flags().StringSlice("dirs", nil, "directories to search for warc files in")
	cmd.Flags().String("db-dir", indexDbDir, "path to index database")
	cmd.Flags().Int("db-batch-max-size", indexDbBatchMaxSize, "max transaction batch size in badger")
	cmd.Flags().Duration("db-batch-max-wait", indexDbBatchMaxWait, "max transaction batch size in badger")
	cmd.Flags().String("compression", compression, `badger compression type: "none", "snappy" or "zstd"`)
	cmd.Flags().Uint("bloom-capacity", bloomCapacity, "estimated bloom filter capacity")
	cmd.Flags().Float64("bloom-fp", bloomFp, "estimated bloom filter false positive rate")

	return cmd
}

func indexCmd(_ *cobra.Command, args []string) error {
	// collect paths from args or flag
	var dirs []string
	if len(args) > 0 {
		dirs = append(dirs, args...)
	} else {
		dirs = viper.GetStringSlice("dirs")
	}

	var w index.Indexer

	format := viper.GetString("format")
	switch format {
	case "cdxj":
		w = index.CdxJ{}
	case "cdxpb":
		w = index.CdxPb{}
	case "cdxdb":
		// Increase GOMAXPROCS as recommended by badger
		// https://github.com/dgraph-io/badger#are-there-any-go-specific-settings-that-i-should-use
		runtime.GOMAXPROCS(128)

		var c options.CompressionType
		if err := viper.UnmarshalKey("compression", &c, viper.DecodeHook(config.CompressionDecodeHookFunc())); err != nil {
			return err
		}
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
		w = index.CdxDb{
			CdxDbIndex: db,
		}
	case "toc":
		w = &index.Toc{
			BloomFilter: bloom.NewWithEstimates(viper.GetUint("bloom-capacity"), viper.GetFloat64("bloom-fp")),
		}
	default:
		return fmt.Errorf("unsupported format %s", format)
	}

	indexWorker := index.Worker(w, viper.GetInt("workers"))
	defer indexWorker.Close()

	var includes []*regexp.Regexp
	for _, r := range viper.GetStringSlice("include") {
		if re, err := regexp.Compile(r); err != nil {
			return fmt.Errorf("%s: %w", r, err)
		} else {
			includes = append(includes, re)
		}
	}

	var excludes []*regexp.Regexp
	for _, r := range viper.GetStringSlice("exclude") {
		if re, err := regexp.Compile(r); err != nil {
			return fmt.Errorf("%s: %w", r, err)
		} else {
			excludes = append(excludes, re)
		}
	}

	indexer, err := index.NewAutoIndexer(indexWorker, dirs,
		index.WithMaxDepth(viper.GetInt("max-depth")),
		index.WithIncludes(includes...),
		index.WithExcludes(excludes...),
	)
	if err != nil {
		return err
	}
	defer indexer.Close()

	return nil
}
