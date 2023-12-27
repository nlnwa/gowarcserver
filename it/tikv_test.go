//go:build tikv

package it

import (
	"context"
	"testing"

	"github.com/nlnwa/gowarcserver/index"
	"github.com/nlnwa/gowarcserver/internal/tikvidx"
	"github.com/nlnwa/gowarcserver/server/api"
)

func TestTIKV(t *testing.T) {
	db, err := tikvidx.NewDB(
		tikvidx.WithPDAddress([]string{"localhost:2379"}),
		tikvidx.WithDatabase("test"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = writeRecords(db)
	if err != nil {
		t.Error(err)
	}
	db.FlushBatch()

	runIntegrationTest(t, db)

	// delete all records
	err = db.Delete(context.Background())
	if err != nil {
		t.Error(err)
	}

	res := make(chan index.CdxResponse)
	err = db.Search(context.Background(), api.SearchRequest{}, res)
	if err != nil {
		t.Error(err)
	}
	for r := range res {
		t.Error("expected no records, got:", r)
	}
}
