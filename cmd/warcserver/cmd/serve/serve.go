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
		warcPort   int
		watchDepth int
		autoIndex  bool
	}{}
	cmd.Flags().IntVarP(&c.warcPort, "warcPort", "p", 9999, "Port that should be used to serve, will use config value otherwise")
	cmd.Flags().IntVarP(&c.watchDepth, "watchDepth", "w", 4, "Maximum depth when indexing warc")
	cmd.Flags().BoolVarP(&c.autoIndex, "autoIndex", "a", true, "Whether the server should index warc files automatically")
	if err := viper.BindPFlags(cmd.Flags()); err != nil {
		log.Fatalf("Failed to bind serve flags, err: %v", err)
	}

	return cmd
}

func runE(warcDirs []string) error {

	dbDir := viper.GetString("indexDir")
	db, err := index.NewIndexDb(dbDir)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if viper.GetBool("autoIndex") {
		log.Infof("Starting auto indexer")
		watchDepth := viper.GetInt("watchDepth")
		autoindexer := index.NewAutoIndexer(db, warcDirs, watchDepth)
		defer autoindexer.Shutdown()
	}

	port := viper.GetInt("warcPort")
	log.Infof("Starting web server at http://localhost:%v", port)
	err = server.Serve(db, port)
	if err != nil {
		log.Warnf("%v", err)
	}
	return nil
}
