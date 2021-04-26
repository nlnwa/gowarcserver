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
package index

import (
	"errors"
	"fmt"

	"github.com/nlnwa/gowarcserver/pkg/index"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func parseFormat(format string) (index.CdxWriter, error) {
	switch format {
	case "cdx":
		return &index.CdxLegacy{}, nil
	case "cdxj":
		return &index.CdxJ{}, nil
	case "cdxpb":
		return &index.CdxPb{}, nil
	case "db":
		return &index.CdxDb{}, nil
	}
	return nil, fmt.Errorf("unknwon format %v, valid formats are: 'cdx', 'cdxj', 'cdxpb', 'db'", format)
}

type conf struct {
	fileName     string
	writerFormat string
}

func NewCommand() *cobra.Command {
	c := &conf{}
	var cmd = &cobra.Command{
		Use:   "index",
		Short: "Index a given warc file",
		Long:  ``,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return errors.New("missing file name")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			c.fileName = args[0]

			writer, err := parseFormat(c.writerFormat)
			if err != nil {
				return err
			}

			return runE(c, writer)
		},
	}

	cmd.Flags().StringVarP(&c.writerFormat, "format", "f", "cdx", "set the index format type")

	return cmd
}

func runE(c *conf, writer index.CdxWriter) error {
	fmt.Printf("Format: %v\n", c.writerFormat)
	dir := viper.GetString("indexdir")
	compression := viper.GetString("compression")
	dbConfig := index.NewDbConfig(dir, compression, index.ALL_MASK)
	err := writer.Init(dbConfig)
	if err != nil {
		return err
	}
	defer writer.Close()

	ReadFile(c, writer)
	return nil
}
