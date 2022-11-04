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
	"regexp"
	"runtime"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/dgraph-io/badger/v3/options"
	"github.com/nlnwa/gowarcserver/index"
	"github.com/nlnwa/gowarcserver/internal/badgeridx"
	"github.com/nlnwa/gowarcserver/internal/tikvidx"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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
	indexDepth := 4
	indexWorkers := 8
	badgerDir := "."
	badgerBatchMaxSize := 1000
	badgerBatchMaxWait := 5 * time.Second
	badgerCompression := "snappy"
	var tikvPdAddr []string
	tikvBatchMaxSize := 1000
	tikvBatchMaxWait := 5 * time.Second
	bloomCapacity := uint(1000)
	bloomFp := 0.01

	cmd.Flags().StringP("format", "f", format, `index format: "cdxj", "cdxpb", "badger", "tikv" or "toc"`)
	cmd.Flags().StringSlice("include", nil, "only include files matching these regular expressions")
	cmd.Flags().StringSlice("exclude", nil, "exclude files matching these regular expressions")
	cmd.Flags().IntP("max-depth", "d", indexDepth, "maximum directory recursion")
	cmd.Flags().Int("workers", indexWorkers, "number of index workers")
	cmd.Flags().StringSlice("dirs", nil, "directories to search for warc files in")
	cmd.Flags().Uint("toc-bloom-capacity", bloomCapacity, "estimated bloom filter capacity")
	cmd.Flags().Float64("toc-bloom-fp", bloomFp, "estimated bloom filter false positive rate")
	cmd.Flags().String("badger-dir", badgerDir, "path to index database")
	cmd.Flags().Int("badger-batch-max-size", badgerBatchMaxSize, "max transaction batch size in badger")
	cmd.Flags().Duration("badger-batch-max-wait", badgerBatchMaxWait, "max wait time before flushing batched records")
	cmd.Flags().String("badger-compression", badgerCompression, "compression algorithm")
	cmd.Flags().StringSlice("tikv-pd-addr", tikvPdAddr, "host:port of TiKV placement driver")
	cmd.Flags().Int("tikv-batch-max-size", tikvBatchMaxSize, "max transaction batch size")
	cmd.Flags().Duration("tikv-batch-max-wait", tikvBatchMaxWait, "max wait time before flushing batched records regardless of max batch size")
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
	case "tikv":
		db, err := tikvidx.NewDB(
			tikvidx.WithPDAddress(viper.GetStringSlice("tikv-pd-addr")),
			tikvidx.WithBatchMaxSize(viper.GetInt("tikv-batch-max-size")),
			tikvidx.WithBatchMaxWait(viper.GetDuration("tikv-batch-max-wait")))
		if err != nil {
			return err
		}
		defer db.Close()
		w = db
	case "badger":
		// Increase GOMAXPROCS as recommended by badger
		// https://github.com/dgraph-io/badger#are-there-any-go-specific-settings-that-i-should-use
		runtime.GOMAXPROCS(128)

		var c options.CompressionType
		if err := viper.UnmarshalKey("badger-compression", &c, viper.DecodeHook(badgeridx.CompressionDecodeHookFunc())); err != nil {
			return err
		}
		db, err := badgeridx.NewDB(
			badgeridx.WithCompression(c),
			badgeridx.WithDir(viper.GetString("badger-dir")),
			badgeridx.WithBatchMaxSize(viper.GetInt("badger-batch-max-size")),
			badgeridx.WithBatchMaxWait(viper.GetDuration("badger-batch-max-wait")),
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
		return fmt.Errorf("unsupported format %s", format)
	}

	indexWorker := index.NewWorker(w, viper.GetInt("workers"))
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

	indexer, err := index.NewAutoIndexer(indexWorker,
		index.WithMaxDepth(viper.GetInt("max-depth")),
		index.WithIncludes(includes...),
		index.WithExcludes(excludes...),
	)
	if err != nil {
		return err
	}
	defer indexer.Close()

	for _, dir := range dirs {
		err := indexer.Index(dir)
		if err != nil {
			log.Warn().Msgf(`Error indexing "%s": %v`, dir, err)
		}
	}

	return nil
}
