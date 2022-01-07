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
	"github.com/nlnwa/gowarcserver/cmd/proxy"
	"runtime"
	"strings"

	"github.com/nlnwa/gowarcserver/cmd/index"
	"github.com/nlnwa/gowarcserver/cmd/serve"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// NewCommand returns a new cobra.Command implementing the root command for warc
func NewCommand() *cobra.Command {
	cobra.OnInitialize(func() { initConfig() })

	cmd := &cobra.Command{
		Use:   "gowarcserver",
		Short: "gowarcserver is a tool for indexing and serving WARC files",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			// Increase GOMAXPROCS as recommended by badger
			// https://github.com/dgraph-io/badger#are-there-any-go-specific-settings-that-i-should-use
			runtime.GOMAXPROCS(128)

			logLevel := viper.GetString("log-level")
			level, err := log.ParseLevel(logLevel)
			if err != nil {
				return fmt.Errorf("'%s' is not part of the valid levels: 'panic', 'fatal', 'error', 'warn', 'warning', 'info', 'debug', 'trace'", logLevel)
			}
			log.SetLevel(level)

			return nil
		},
	}

	// Global flags
	_ = cmd.PersistentFlags().StringP("config", "c", "", `path to config file, default paths is "./config.yaml", "$HOME/.gowarcserver/config.yaml" or "/etc/gowarcserver/config.yaml"`)
	_ = cmd.PersistentFlags().StringP("log-level", "l", "info", `set log level: "trace", "debug", "info", "warn" or "error"`)
	if err := viper.BindPFlags(cmd.PersistentFlags()); err != nil {
		log.Fatalf("Failed to bind root flags, err: %v", err)
	}

	// Subcommands
	cmd.AddCommand(serve.NewCommand())
	cmd.AddCommand(index.NewCommand())
	cmd.AddCommand(proxy.NewCommand())

	return cmd
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv() // read in environment variables that match flags

	if viper.IsSet("config") {
		viper.SetConfigFile(viper.GetString("config"))
	} else {
		viper.SetConfigName("config") // name of config file (without extension)
		viper.SetConfigType("yaml")   // REQUIRED if the config file does not have the extension in the name

		// look for config in:
		viper.AddConfigPath(".")                   // current working directory
		viper.AddConfigPath("$HOME/.gowarcserver") // home folder configuration directory
		viper.AddConfigPath("/etc/gowarcserver/")  // global configuration directory
	}

	if err := viper.ReadInConfig(); err != nil {
		if errors.As(err, new(viper.ConfigFileNotFoundError)) {
			return
		}
		log.Fatalf("Failed to read config file: %v", err)
	}
	log.Debugf("Using config file: %s", viper.ConfigFileUsed())
}
