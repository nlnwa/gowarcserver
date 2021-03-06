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

type conf struct {
	port       int
	warcDirs   []string
	watchDepth int
}

func NewCommand() *cobra.Command {
	c := &conf{}
	var cmd = &cobra.Command{
		Use:   "serve",
		Short: "Start the warc server to serve warc records",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				c.warcDirs = args
			} else {
				c.warcDirs = viper.GetStringSlice("warcdir")
			}
			return runE(c)
		},
	}

	cmd.Flags().IntVarP(&c.port, "port", "p", -1, "the port that should be used to serve, will use config value otherwise")
	cmd.Flags().IntVarP(&c.watchDepth, "watch-depth", "w", 4, "The maximum depth when indexing warc")

	return cmd
}

func runE(c *conf) error {
	if c.port < 0 {
		c.port = viper.GetInt("warcport")
	}

	db, err := index.DbFromViper()
	if err != nil {
		return err
	}
	defer db.Close()

	if viper.GetBool("autoindex") {
		log.Infof("Starting autoindexer")
		autoindexer := index.NewAutoIndexer(db, c.warcDirs, c.watchDepth)
		defer autoindexer.Shutdown()
	}

	log.Infof("Starting web server at http://localhost:%v", c.port)
	server.Serve(db, c.port)
	return nil
}
