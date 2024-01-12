package badgeridx

import (
	"context"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/nlnwa/gowarcserver/index"
	"github.com/nlnwa/gowarcserver/internal/keyvalue"
	"github.com/nlnwa/gowarcserver/schema"
	"google.golang.org/protobuf/proto"
)

// Assert that DB implements index.ReportGenerator
var _ index.ReportGenerator = (*DB)(nil)

func (db *DB) AddTask(id string, cancel context.CancelFunc) {
	db.wg.Add(1)
	db.tasks[id] = func() {
		cancel()
	}
}

func (db *DB) DeleteTask(id string) {
	delete(db.tasks, id)
}

func (db *DB) SaveReport(ctx context.Context, report *schema.Report) error {
	key, value, err := keyvalue.MarshalReport(report, "")
	if err != nil {
		return err
	}

	return db.ReportIndex.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (db *DB) CreateReport(ctx context.Context, req index.Request) (*schema.Report, error) {
	r, err := keyvalue.NewReportGenerator(db)
	if err != nil {
		return nil, err
	}
	return r.Generate(ctx, req)
}

func (db *DB) CancelReport(ctx context.Context, id string) error {
	cancel, ok := db.tasks[id]
	if !ok {
		return fmt.Errorf("no report with id '%s'", id)
	}
	cancel()
	return nil
}

func (db *DB) DeleteReport(ctx context.Context, id string) error {
	report, err := db.GetReport(ctx, id)
	if err != nil {
		return err
	}
	if report == nil {
		return fmt.Errorf("no report with id '%s'", id)
	}
	if report.Status == schema.Report_RUNNING {
		return fmt.Errorf("report with id '%s' is running", id)
	}
	return db.ReportIndex.Update(func(txn *badger.Txn) error {
		return txn.Delete(keyvalue.Key(id))
	})
}

func (db *DB) GetReport(ctx context.Context, id string) (*schema.Report, error) {
	key := keyvalue.Key(id)
	val := new(schema.Report)
	err := db.ReportIndex.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		err = item.Value(func(v []byte) error {
			return proto.Unmarshal(v, val)
		})
		return err
	})
	return val, err
}

func (db *DB) ListReports(ctx context.Context, req index.Request, results chan<- index.ReportResponse) error {
	go func() {
		limit := req.Limit()
		_ = db.ReportIndex.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = limit
			iter := txn.NewIterator(opts)

			defer iter.Close()
			defer close(results)

			count := 0
			for iter.Seek(nil); iter.Valid(); iter.Next() {
				select {
				case <-ctx.Done():
					results <- keyvalue.ReportResponse{Error: ctx.Err()}
					return nil
				default:
				}

				if limit > 0 && count >= limit {
					return nil
				}
				count++

				report := new(schema.Report)
				err := iter.Item().Value(func(value []byte) error {
					return proto.Unmarshal(value, report)
				})
				if err != nil {
					results <- keyvalue.ReportResponse{Error: err}
					return nil
				} else {
					results <- keyvalue.ReportResponse{Value: report}
				}
			}
			return nil
		})
	}()
	return nil
}
