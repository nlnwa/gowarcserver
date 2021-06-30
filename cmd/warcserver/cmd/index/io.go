package index

import (
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/nlnwa/gowarc/warcoptions"
	"github.com/nlnwa/gowarc/warcreader"
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

func ReadFile(c *conf, writer index.CdxWriter) error {
	opts := &warcoptions.WarcOptions{Strict: false}
	wf, err := warcreader.NewWarcFilename(c.fileName, 0, opts)
	if err != nil {
		return err
	}
	defer wf.Close()

	count := 0

	// avoid defer copy value by using a anonymous function
	// At the end, print count even if an error occurs
	defer func() {
		log.Printf("Count: %d", count)
	}()

	for {
		wr, currentOffset, err := wf.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("failed iterating warc file: %w, rec num: %v, offset %v", err, strconv.Itoa(count), currentOffset)
		}
		count++

		err = writer.Write(wr, c.fileName, currentOffset)
		if err != nil {
			log.Warnf("Failed to write to %s at offset %d: %v", c.fileName, currentOffset, err)
		}
	}
	return nil
}
