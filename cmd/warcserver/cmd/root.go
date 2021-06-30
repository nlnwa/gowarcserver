/*
 * Copyright © 2019 National Library of Norway
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cmd

import (
	"errors"
	"fmt"
	"runtime"

	"github.com/fsnotify/fsnotify"
	"github.com/nlnwa/gowarcserver/cmd/warcserver/cmd/index"
	"github.com/nlnwa/gowarcserver/cmd/warcserver/cmd/serve"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// NewCommand returns a new cobra.Command implementing the root command for warc
func NewCommand() *cobra.Command {
	cobra.OnInitialize(func() { initConfig() })

	cmd := &cobra.Command{
		Use:   "warcserver",
		Short: "Server capable of indexing and serving warc files",
		Long:  ``,

		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			// Increase GOMAXPROCS as recommended by badger
			// https://github.com/dgraph-io/badger#are-there-any-go-specific-settings-that-i-should-use
			runtime.GOMAXPROCS(128)

			logLevel := viper.GetString("logLevel")
			level, err := log.ParseLevel(logLevel)
			if err != nil {
				return fmt.Errorf("'%s' is not part of the valid levels: 'panic', 'fatal', 'error', 'warn', 'warning', 'info', 'debug', 'trace'", logLevel)
			}
			log.SetLevel(level)

			return nil
		},
	}

	// Stub to store cobra variables
	c := &struct {
		cfgFile     string
		logLevel    string
		compression string
	}{}

	// Flags
	cmd.PersistentFlags().StringVarP(&c.compression, "compression", "c", "badgerdefault", "DB compression type: 'badgerdefault', 'none', 'snappy', 'zstd'.")
	cmd.PersistentFlags().StringVarP(&c.logLevel, "logLevel", "l", "info", "set the log level of gowarc, it will take precedence over config 'loglevel'")
	cmd.PersistentFlags().StringVar(&c.cfgFile, "config", "", "config file. If not set, /etc/warc/, $HOME/.warc/ and current working dir will be searched for file config.yaml")

	if err := viper.BindPFlags(cmd.PersistentFlags()); err != nil {
		log.Fatalf("Failed to bind root flags, err: %v", err)
	}

	// Subcommands
	cmd.AddCommand(serve.NewCommand())
	cmd.AddCommand(index.NewCommand())

	return cmd
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	viper.SetTypeByDefaultValue(true)
	viper.SetDefault("warcDir", []string{"."})
	viper.SetDefault("indexDir", ".")

	viper.AutomaticEnv() // read in environment variables that match

	if viper.IsSet("config") {
		// Use config file from the flag.
		viper.SetConfigFile(viper.GetString("config"))
	} else {
		// Search config in home directory with name ".warc" (without extension).
		viper.SetConfigName("config")      // name of config file (without extension)
		viper.SetConfigType("yaml")        // REQUIRED if the config file does not have the extension in the name
		viper.AddConfigPath("/etc/warc/")  // path to look for the config file in
		viper.AddConfigPath("$HOME/.warc") // call multiple times to add many search paths
		viper.AddConfigPath(".")           // optionally look for config in the working directory
	}

	viper.WatchConfig()
	viper.OnConfigChange(func(e fsnotify.Event) {
		fmt.Println("Config file changed:", e.Name)
	})

	if err := viper.ReadInConfig(); err != nil {
		if !errors.As(err, &viper.ConfigFileNotFoundError{}) {
			// Config file was found but another error was produced
			log.Fatalf("error reading config file: %v", err)
		}
	}

	// Config file found and successfully parsed
	fmt.Println("Using config file:", viper.ConfigFileUsed())
}
