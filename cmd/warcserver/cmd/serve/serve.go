/*
 * Copyright 2019 National Library of Norway.
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
	"net/url"
	"time"

	"github.com/nlnwa/gowarcserver/pkg/index"
	"github.com/nlnwa/gowarcserver/pkg/server"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func NewCommand() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "serve",
		Short: "Start the warc server to serve warc records",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			var warcDirs []string
			if len(args) > 0 {
				warcDirs = args
			} else {
				warcDirs = viper.GetStringSlice("warcDir")
			}
			return runE(warcDirs)
		},
	}

	// Stub to hold flags
	c := &struct {
		warcPort          int
		watchDepth        int
		autoIndex         bool
		noIdDB            bool
		noFileDB          bool
		noCdxDB           bool
		childUrls         []string
		childQueryTimeout time.Duration
	}{}
	cmd.Flags().IntVarP(&c.warcPort, "warcPort", "p", 9999, "Port that should be used to serve, will use config value otherwise")
	cmd.Flags().IntVarP(&c.watchDepth, "watchDepth", "w", 4, "Maximum depth when indexing warc")
	cmd.Flags().BoolVarP(&c.autoIndex, "autoIndex", "a", true, "Whether the server should index warc files automatically")
	cmd.Flags().BoolVarP(&c.noIdDB, "idDb", "i", false, "Turn off id db")
	cmd.Flags().BoolVarP(&c.noFileDB, "fileDb", "f", false, "Turn off file db")
	cmd.Flags().BoolVarP(&c.noCdxDB, "cdxDb", "x", false, "Turn off cdx db")
	cmd.Flags().StringSliceVarP(&c.childUrls, "childUrls", "u", []string{""}, "specify urls to other gowarcserver instances, queries are propagated to these urls")
	cmd.Flags().DurationVarP(&c.childQueryTimeout, "childQueryTimeout", "t", time.Millisecond*300, "Time before query to child node times out")
	if err := viper.BindPFlags(cmd.Flags()); err != nil {
		log.Fatalf("Failed to bind serve flags, err: %v", err)
	}

	return cmd
}

func runE(warcDirs []string) error {
	dir := viper.GetString("indexDir")
	mask := ConfigToDBMask(
		viper.GetBool("idDb"),
		viper.GetBool("fileDb"),
		viper.GetBool("cdxDb"),
	)
	compressionStr := viper.GetString("compression")
	dbConfig := index.NewDbConfig(dir, compressionStr, mask)
	db, err := index.NewIndexDb(dbConfig)
	if err != nil {
		return err
	}
	defer db.Close()

	if viper.GetBool("autoIndex") {
		log.Infof("Starting auto indexer")
		watchDepth := viper.GetInt("watchDepth")
		autoindexer := index.NewAutoIndexer(db, warcDirs, watchDepth)
		defer autoindexer.Shutdown()
	}

	childUrls := BuildUrlSlice(viper.GetStringSlice("childUrls"))
	childQueryTimeout := viper.GetDuration("childQueryTimeout")
	port := viper.GetInt("warcPort")
	log.Infof("Starting web server at http://localhost:%v", port)
	err = server.Serve(port, db, childUrls, childQueryTimeout)
	if err != nil {
		log.Warnf("%v", err)
	}
	return nil
}

func BuildUrlSlice(urlStrs []string) []*url.URL {
	var childUrls []*url.URL
	for _, urlstr := range urlStrs {
		if u, err := url.Parse(urlstr); err != nil {
			log.Warnf("Parsing config child url %s failed with error %v", urlstr, err)
		} else {
			childUrls = append(childUrls, u)
		}
	}
	return childUrls
}

func ConfigToDBMask(noIdDB bool, noFileDB bool, noCdxDB bool) int32 {
	masker := func(excludeDB bool, v int32) int32 {
		if !excludeDB {
			return v
		} else {
			return index.NONE_MASK
		}
	}

	cdxMask := masker(noCdxDB, index.CDX_DB_MASK)
	fileMask := masker(noFileDB, index.FILE_DB_MASK)
	idMask := masker(noIdDB, index.ID_DB_MASK)

	return cdxMask | fileMask | idMask
}
