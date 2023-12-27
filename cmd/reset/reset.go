/*
 * Copyright 2023 National Library of Norway.
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

package reset

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/nlnwa/gowarcserver/index"
	"github.com/nlnwa/gowarcserver/internal/badgeridx"
	"github.com/nlnwa/gowarcserver/internal/tikvidx"
)

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reset",
		Short: "Delete all records in the database",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if err := viper.BindPFlags(cmd.Flags()); err != nil {
				return fmt.Errorf("failed to bind flags: %w", err)
			}
			return nil
		},
		RunE: deleteCmd,
	}
	// index options
	cmd.Flags().StringP("index-format", "o", "badger", `index format: "badger" or "tikv"`)

	// badger options
	cmd.Flags().String("badger-dir", "./warcdb", "path to index database")
	cmd.Flags().String("badger-database", "", "name of badger database")

	// tikv options
	cmd.Flags().StringSlice("tikv-pd-addr", nil, "host:port of TiKV placement driver")
	cmd.Flags().String("tikv-database", "", "name of tikv database")

	return cmd
}

func deleteCmd(_ *cobra.Command, _ []string) error {
	var deleter index.Deleter

	indexFormat := viper.GetString("index-format")
	switch indexFormat {
	case "badger":
		// Increase GOMAXPROCS as recommended by badger
		// https://github.com/dgraph-io/badger#are-there-any-go-specific-settings-that-i-should-use
		runtime.GOMAXPROCS(128)
		db, err := badgeridx.NewDB(
			badgeridx.WithDir(viper.GetString("badger-dir")),
			badgeridx.WithDatabase(viper.GetString("badger-database")),
		)
		if err != nil {
			return err
		}
		defer db.Close()
		deleter = db
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
		deleter = db
	default:
		return fmt.Errorf("unknown index format: %s", indexFormat)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigs
		log.Info().Msgf("Received %s signal, cancelling context...", sig)
		cancel()
	}()

	return deleter.Delete(ctx)
}
