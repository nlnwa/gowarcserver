package tikvidx

import (
	"context"
	"errors"

	"github.com/nlnwa/gowarcserver/index"
	"github.com/nlnwa/gowarcserver/schema"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"google.golang.org/protobuf/proto"
)

// Closest returns the first closest cdx value(s).
func (db *DB) Closest(ctx context.Context, req index.ClosestRequest, res chan<- index.CdxResponse) error {
	// begin transaction
	tx, err := db.client.Begin()
	if err != nil {
		return err
	}

	it, err := NewIterClosest(ctx, tx, req.Key(), req.Closest())
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
	var err error
	var tx *transaction.KVTxn
	var it iterator

	if len(req.Keys()) == 0 {
		return errors.New("search request has no keys")
	}
	tx, err = db.client.Begin()
	if err != nil {
		return err
	}

	it, err = newIter(ctx, tx, req)
	if err != nil {
		return err
	}

	go func() {
		defer close(res)
		defer it.Close()

		limit := req.Limit()
		if limit == 0 {
			limit = 100
		}

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
	tx, err := db.client.Begin()
	if err != nil {
		return err
	}
	it, err := tx.Iter([]byte(cdxPrefix), []byte(cdxEOF))
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
	tx, err := db.client.Begin()
	if err != nil {
		return err
	}
	it, err := tx.Iter([]byte(filePrefix), []byte(fileEOF))
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
	tx, err := db.client.Begin()
	if err != nil {
		return "", err
	}

	b, err := tx.Get(ctx, []byte(id))
	return string(b), err
}

func (db *DB) ListStorageRef(_ context.Context, limit int, res chan<- index.IdResponse) error {
	tx, err := db.client.Begin()
	if err != nil {
		return err
	}
	it, err := tx.Iter([]byte(idPrefix), []byte(idEOF))
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
				res <- index.IdResponse{Key: string(it.Key()[1:]), Value: string(it.Value())}
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
func (db *DB) Resolve(warcId string) (storageRef string, err error) {
	key := []byte(idPrefix + warcId)

	var kv KV
	kv, err = db.get(key)
	if err != nil {
		return
	}
	storageRef = string(kv.V)
	return
}

// ResolvePath looks up filename in file index and returns the path field.
func (db *DB) ResolvePath(filename string) (filePath string, err error) {
	fileInfo, err := db.getFileInfo(filename)
	return fileInfo.Path, err
}
