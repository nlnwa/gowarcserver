package tikvidx

import (
	"context"
	"strings"

	"github.com/nlnwa/gowarcserver/index"
	"github.com/nlnwa/gowarcserver/schema"
	"google.golang.org/protobuf/proto"
)

// Closest returns the first closest cdx value(s).
func (db *DB) Closest(ctx context.Context, req index.ClosestRequest, res chan<- index.CdxResponse) error {
	ts, err := db.client.CurrentTimestamp("global")
	if err != nil {
		return err
	}
	snapshot := db.client.GetSnapshot(ts)
	it, err := NewIterClosest(ctx, snapshot, req.Key(), req.Closest())
	if err != nil {
		return err
	}

	go func() {
		defer close(res)
		defer it.Close()

		for it.Valid() {
			select {
			case <-ctx.Done():
				return
			default:
			}

			cdx, err := cdxFromValue(it.Value())
			if err != nil {
				res <- index.CdxResponse{Error: err}
			} else {
				res <- index.CdxResponse{Cdx: cdx}
			}

			if err := it.Next(); err != nil {
				res <- index.CdxResponse{Error: err}
			}
		}
	}()

	return nil
}

func cdxFromValue(value []byte) (*schema.Cdx, error) {
	result := new(schema.Cdx)
	if err := proto.Unmarshal(value, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (db *DB) Search(ctx context.Context, req index.SearchRequest, res chan<- index.CdxResponse) error {
	ts, err := db.client.CurrentTimestamp("global")
	if err != nil {
		return err
	}
	snapshot := db.client.GetSnapshot(ts)

	it, err := newIter(ctx, snapshot, req)
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

		limit := req.Limit()

		for it.Valid() && limit > 0 {
			select {
			case <-ctx.Done():
				res <- index.CdxResponse{Error: ctx.Err()}
				return
			default:
				limit--
			}

			func() {
				inDateRange, err := req.DateRange().ContainsStr(cdxKey(it.Key()).ts())
				if err != nil {
					res <- index.CdxResponse{Error: err}
					return
				}
				if !inDateRange {
					return
				}
				cdx := new(schema.Cdx)
				if err := proto.Unmarshal(it.Value(), cdx); err != nil {
					res <- index.CdxResponse{Error: err}
					return
				}
				if req.Filter().Eval(cdx) {
					res <- index.CdxResponse{Cdx: cdx}
				}
			}()
			if err := it.Next(); err != nil {
				res <- index.CdxResponse{Error: err}
				break
			}
		}
	}()
	return nil
}

func (db *DB) List(ctx context.Context, limit int, res chan<- index.CdxResponse) error {
	ts, err := db.client.CurrentTimestamp("global")
	if err != nil {
		return err
	}
	snapshot := db.client.GetSnapshot(ts)

	it, err := snapshot.Iter([]byte(cdxPrefix), []byte(cdxEOF))
	if err != nil {
		return err
	}

	go func() {
		defer it.Close()
		defer close(res)

		for it.Valid() && limit > 0 {
			select {
			case <-ctx.Done():
				res <- index.CdxResponse{Error: ctx.Err()}
				return
			default:
			}
			limit--
			cdx := new(schema.Cdx)
			err := proto.Unmarshal(it.Value(), cdx)
			if err != nil {
				res <- index.CdxResponse{Error: err}
			} else {
				res <- index.CdxResponse{Cdx: cdx}
			}
			err = it.Next()
			if err != nil {
				res <- index.CdxResponse{Error: err}
				break
			}
		}
	}()

	return nil
}

func (db *DB) GetFileInfo(_ context.Context, filename string) (*schema.Fileinfo, error) {
	return db.getFileInfo(filename)
}

func (db *DB) ListFileInfo(_ context.Context, limit int, res chan<- index.FileResponse) error {
	ts, err := db.client.CurrentTimestamp("global")
	if err != nil {
		return err
	}
	snapshot := db.client.GetSnapshot(ts)

	it, err := snapshot.Iter([]byte(filePrefix), []byte(fileEOF))
	if err != nil {
		return err
	}

	go func() {
		defer it.Close() // close iterator
		defer close(res) // close response channel

		for it.Valid() && limit > 0 {
			limit--
			fileInfo := new(schema.Fileinfo)
			err := proto.Unmarshal(it.Value(), fileInfo)
			if err != nil {
				res <- index.FileResponse{Error: err}
			} else {
				res <- index.FileResponse{Fileinfo: fileInfo}
			}
			err = it.Next()
			if err != nil {
				res <- index.FileResponse{Error: err}
				break
			}
		}
	}()

	return nil
}

func (db *DB) GetStorageRef(ctx context.Context, id string) (string, error) {
	ts, err := db.client.CurrentTimestamp("global")
	if err != nil {
		return "", err
	}
	snapshot := db.client.GetSnapshot(ts)

	b, err := snapshot.Get(ctx, []byte(id))
	return string(b), err
}

func (db *DB) ListStorageRef(_ context.Context, limit int, res chan<- index.IdResponse) error {
	ts, err := db.client.CurrentTimestamp("global")
	if err != nil {
		return err
	}
	snapshot := db.client.GetSnapshot(ts)

	it, err := snapshot.Iter([]byte(idPrefix), []byte(idEOF))
	if err != nil {
		return err
	}

	go func() {
		defer it.Close()
		defer close(res)

		for it.Valid() && limit > 0 {
			limit--
			if err != nil {
				res <- index.IdResponse{Error: err}
			} else {
				k := strings.TrimPrefix(string(it.Key()), idPrefix)
				res <- index.IdResponse{Key: k, Value: string(it.Value())}
			}
			err = it.Next()
			if err != nil {
				res <- index.IdResponse{Error: err}
				break
			}
		}
	}()

	return nil
}

// Resolve looks up warcId in the id index of the database and returns corresponding storageRef, or an error if not found.
func (db *DB) Resolve(ctx context.Context, warcId string) (string, error) {
	key := []byte(idPrefix + warcId)

	kv, err := db.get(ctx, key)
	if err != nil {
		return "", err
	}
	return string(kv.V), nil
}

// ResolvePath looks up filename in file index and returns the path field.
func (db *DB) ResolvePath(filename string) (filePath string, err error) {
	fileInfo, err := db.getFileInfo(filename)
	return fileInfo.Path, err
}
