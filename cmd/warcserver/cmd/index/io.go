package index

import (
	"fmt"
	"io"
	"strconv"

	"github.com/nlnwa/gowarc/warcoptions"
	"github.com/nlnwa/gowarc/warcreader"
	"github.com/nlnwa/gowarcserver/pkg/index"
	logrus "github.com/sirupsen/logrus"
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

func ReadFile(c *conf) error {
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
		logrus.Printf("Count: %d", count)
	}()

	for {
		wr, currentOffset, err := wf.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("Error: %v, rec num: %v, Offset %v\n", err.Error(), strconv.Itoa(count), currentOffset)
		}
		count++

		c.writer.Write(wr, c.fileName, currentOffset)
	}
	return nil
}
