package it

import (
	"context"
	"testing"

	"github.com/nlnwa/gowarcserver/index"
	"github.com/nlnwa/gowarcserver/internal/badgeridx"
	"github.com/nlnwa/gowarcserver/server/api"
)

func TestBadger(t *testing.T) {
	db, err := badgeridx.NewDB(badgeridx.WithDir(t.TempDir()), badgeridx.WithoutBadgerLogging())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = writeRecords(db)
	if err != nil {
		t.Fatal(err)
	}
	db.FlushBatch()

	runIntegrationTest(t, db)

	err = db.Delete(context.Background())
	if err != nil {
		t.Error(err)
	}

	res := make(chan index.CdxResponse)
	req := new(api.SearchRequest)
	err = db.Search(context.Background(), req, res)
	if err != nil {
		t.Error(err)
	}
	for r := range res {
		t.Error("expected no records, got:", r)
	}
}
