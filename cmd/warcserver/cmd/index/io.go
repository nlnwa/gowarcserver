package index

import (
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/nlnwa/gowarc"
	"github.com/nlnwa/gowarcserver/pkg/index"
	log "github.com/sirupsen/logrus"
)

func ParseFormat(format string) (index.CdxWriter, error) {
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

/// reads a file using the supplied config and writes with a CdxWriter.
func ReadFile(c *conf, writer index.CdxWriter) error {
	opts := gowarc.WithNoValidation()
	wf, err := gowarc.NewWarcFileReader(c.fileName, 0, opts)
	if err != nil {
		return err
	}
	defer wf.Close()

	count := 0

	// print count even if an error occurs
	defer func() {
		log.Printf("Count: %d", count)
	}()

	for {
		record, currentOffset, validation, err := wf.Next()
		if !validation.Valid() {
			// return validation message to end user
			return errors.New(validation.String())

		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("failed iterating warc file: %w, rec num: %v, offset %v", err, strconv.Itoa(count), currentOffset)
		}
		count++

		err = writer.Write(record, c.fileName, currentOffset)
		if err != nil {
			log.Warnf("Failed to write to %s at offset %d: %v", c.fileName, currentOffset, err)
		}
	}
	return nil
}
