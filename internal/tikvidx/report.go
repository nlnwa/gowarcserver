package tikvidx

import (
	"context"
	"fmt"

	"github.com/nlnwa/gowarcserver/index"
	"github.com/nlnwa/gowarcserver/internal/keyvalue"
	"github.com/nlnwa/gowarcserver/schema"
	"google.golang.org/protobuf/proto"
)

func (db *DB) saveReport(ctx context.Context, key []byte, report *schema.Report) error {
	value, err := proto.Marshal(report)
	if err != nil {
		return err
	}
	return db.client.Put(ctx, key, value)
}

func (db *DB) AddTask(id string, cancel context.CancelFunc) {
	db.wg.Add(1)
	db.tasks[id] = cancel
}

func (db *DB) DeleteTask(id string) {
	defer db.wg.Done()
	if cancel, ok := db.tasks[id]; ok {
		delete(db.tasks, id)
		cancel()
	}
}

func (db *DB) SaveReport(ctx context.Context, report *schema.Report) error {
	key := keyvalue.KeyWithPrefix(report.Id, reportPrefix)
	return db.saveReport(ctx, key, report)
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
	return db.client.Delete(ctx, keyvalue.KeyWithPrefix(id, reportPrefix))
}

func (db *DB) GetReport(ctx context.Context, id string) (*schema.Report, error) {
	value, err := db.client.Get(ctx, keyvalue.KeyWithPrefix(id, reportPrefix))
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	report := new(schema.Report)
	err = proto.Unmarshal(value, report)
	if err != nil {
		return nil, err
	}
	return report, nil
}

func (db *DB) ListReports(ctx context.Context, req index.Request, res chan<- index.ReportResponse) error {
	key := keyvalue.KeyWithPrefix("", reportPrefix)
	it, err := newIter(ctx, key, db.client, req, reportPrefix)
	if err != nil {
		return err
	}
	if it == nil {
		close(res)
		return nil
	}
	go func() {
		defer close(res)
		defer it.Close()

		count := 0

		for it.Valid() {
			var response keyvalue.ReportResponse
			report := new(schema.Report)
			err := proto.Unmarshal(it.Value(), report)
			if err != nil {
				response.Error = err
			} else {
				response.Value = report
			}
			select {
			case <-ctx.Done():
				return
			case res <- response:
				if response.Error == nil {
					count++
				}
			}
			if req.Limit() > 0 && count >= req.Limit() {
				return
			}
			if err = it.Next(); err != nil {
				res <- keyvalue.ReportResponse{Error: err}
				return
			}
		}
	}()

	return nil
}
