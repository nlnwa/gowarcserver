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
	"github.com/dgraph-io/badger/v3/options"
	"github.com/nlnwa/gowarcserver/internal/config"
	"github.com/nlnwa/gowarcserver/internal/database"
	"github.com/nlnwa/gowarcserver/internal/index"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func NewCommand() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "index [dir] ...",
		Short: "Index warc file(s)",
		RunE:  indexCmd,
	}

	// defaults
	format := "cdxj"
	indexDbDir := "."
	compression := config.SnappyCompression
	indexDepth := 4
	indexWorkers := 8
	indexTargets := []string{"."}
	suffixes := []string{""}
	useBloomFilter := true
	bloomCapacity := uint(1000)
	bloomFp := 0.01

	cmd.Flags().StringP("format", "f", format, `index format: "cdxj", "cdxpb", "cdxdb" or "toc"`)
	cmd.Flags().StringSlice("include", suffixes, "only include filenames matching these suffixes")
	cmd.Flags().IntP("max-depth", "d", indexDepth, "maximum directory recursion")
	cmd.Flags().Int("workers", indexWorkers, "number of index workers")
	cmd.Flags().StringSlice("dirs", indexTargets, "directories to search for warc files in")
	cmd.Flags().String("db-dir", indexDbDir, "path to index database")
	cmd.Flags().String("compression", compression, `badger compression type: "none", "snappy" or "zstd"`)
	cmd.Flags().Bool("bloom", useBloomFilter, "use a bloom filter when indexing toc")
	cmd.Flags().Uint("bloom-capacity", bloomCapacity, "estimated bloom filter capacity")
	cmd.Flags().Float64("bloom-fp", bloomFp, "estimated bloom filter false positive rate")

	if err := viper.BindPFlags(cmd.Flags()); err != nil {
		log.Fatalf("Failed to bind index flags, err: %v", err)
	}

	return cmd
}

func indexCmd(_ *cobra.Command, args []string) error {
	// collect paths from args and --dirs flag
	dirs := viper.GetStringSlice("dirs")
	dirs = append(dirs, args...)

	var w index.Indexer

	format := viper.GetString("format")
	switch format {
	case "cdxj":
		w = new(index.CdxJ)
	case "cdxpb":
		w = new(index.CdxPb)
	case "cdxdb":
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
		w = &index.CdxDb{CdxDbIndex: db}
	case "toc":
		toc := new(index.Toc)
		if viper.GetBool("bloom") {
			toc = index.NewTocWithBloom(viper.GetUint("bloom-capacity"), viper.GetFloat64("bloom-fp"))
		}
		w = toc
	default:
		return fmt.Errorf("unsupported format %s", format)
	}

	indexWorker := index.NewIndexWorker(w, viper.GetInt("workers"))
	defer indexWorker.Close()

	indexer, err := index.NewAutoIndexer(indexWorker.Accept, dirs,
		index.WithMaxDepth(viper.GetInt("max-depth")),
		index.WithSuffixes(viper.GetStringSlice("include")...))
	if err != nil {
		return err
	}
	defer indexer.Close()

	return nil
}
