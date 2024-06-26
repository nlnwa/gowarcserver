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
	"os"
	"strings"

	"github.com/nlnwa/gowarcserver/cmd/index"
	"github.com/nlnwa/gowarcserver/cmd/reset"
	"github.com/nlnwa/gowarcserver/cmd/serve"
	"github.com/nlnwa/gowarcserver/cmd/version"
	"github.com/nlnwa/gowarcserver/logger"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// NewCommand returns a new cobra.Command implementing the root command for warc
func NewCommand() *cobra.Command {
	cobra.OnInitialize(initConfig)

	cmd := &cobra.Command{
		Use:   "gowarcserver",
		Short: "gowarcserver is a tool for indexing and serving WARC files",
	}

	// Global flags
	_ = cmd.PersistentFlags().String("config", "", `path to config file, default paths are "./config.yaml", "$HOME/.gowarcserver/config.yaml" or "/etc/gowarcserver/config.yaml"`)
	_ = cmd.PersistentFlags().StringP("log-level", "l", "info", `set log level, available levels are "panic", "fatal", "error", "warn", "info", "debug" and "trace"`)
	_ = cmd.PersistentFlags().String("log-formatter", "logfmt", "log formatter, available values are logfmt and json")
	_ = cmd.PersistentFlags().Bool("log-method", false, "log method caller")

	if err := viper.BindPFlags(cmd.PersistentFlags()); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to bind global flags: %v", err)
		os.Exit(1)
	}

	// Subcommands
	cmd.AddCommand(serve.NewCommand())
	cmd.AddCommand(index.NewCommand())
	cmd.AddCommand(version.NewCommand())
	cmd.AddCommand(reset.NewCommand())
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

	defer func() {
		logger.InitLog(viper.GetString("log-level"), viper.GetString("log-formatter"), viper.GetBool("log-method"))
		log.Debug().Msgf("Using config file: %s", viper.ConfigFileUsed())
	}()

	err := viper.ReadInConfig()
	if err != nil && !errors.As(err, new(viper.ConfigFileNotFoundError)) {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to read config file: %v", err)
		os.Exit(1)
	}
}
