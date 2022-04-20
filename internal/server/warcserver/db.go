package warcserver

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/nlnwa/gowarcserver/internal/database"
	"github.com/nlnwa/gowarcserver/internal/surt"
	"github.com/nlnwa/gowarcserver/internal/timestamp"
	"github.com/nlnwa/gowarcserver/schema"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
)

type Key string

func (k Key) ts() string {
	return strings.Split(string(k), " ")[1]
}

// DbCdxServer implements searching the index database via a CDXJ API
type DbCdxServer struct {
	*database.CdxDbIndex
}

func unmarshal(item *badger.Item) (cdx *schema.Cdx, err error) {
	err = item.Value(func(val []byte) error {
		result := new(schema.Cdx)
		if err := proto.Unmarshal(val, result); err != nil {
			return err
		}
		cdx = result
		return nil
	})
	return
}

func (c DbCdxServer) one(key string, closest string) (cdx *schema.Cdx, err error) {
	// forward seek key
	fk := []byte(key + " " + closest)
	// prefix
	prefix := []byte(key)

	err = c.Walk(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 1
		opts.Prefix = prefix

		forward := txn.NewIterator(opts)
		defer forward.Close()
		forward.Seek(fk)

		// first check if we got literal match on forward seek key (fast path)
		if forward.ValidForPrefix(fk) {
			var err error
			cdx, err = unmarshal(forward.Item())
			if err != nil {
				return err
			}
			return nil
		}

		// no literal match; iterate forward and backward to get next closest (slow path)

		// iterate forward
		forward.Next()
		// and backward
		opts.Reverse = true
		backward := txn.NewIterator(opts)
		defer backward.Close()
		bk := []byte(key + " " + closest + string(rune(0xff)))
		backward.Seek(bk)

		var ft int64
		var bt int64
		t, _ := timestamp.Parse(closest)
		cl := t.Unix()

		// get forward ts
		if forward.ValidForPrefix(prefix) {
			t, _ = timestamp.Parse(Key(forward.Item().Key()).ts())
			ft = t.Unix()
		}
		// get backward ts
		backward.Seek(bk)
		if backward.ValidForPrefix(prefix) {
			t, _ = timestamp.Parse(Key(backward.Item().Key()).ts())
			bt = t.Unix()
		}

		var item *badger.Item

		if ft != 0 && bt != 0 {
			// find closest of forward and backward
			isForward := timestamp.AbsInt64(cl-ft) < timestamp.AbsInt64(cl-bt)
			if isForward {
				item = forward.Item()
			} else {
				item = backward.Item()
			}
		} else if ft != 0 {
			item = forward.Item()
		} else if bt != 0 {
			item = backward.Item()
		} else {
			// found nothing
			return nil
		}

		var err error
		cdx, err = unmarshal(item)
		return err
	})
	return
}

func (c DbCdxServer) search(api *CdxjServerApi, perCdxFunc PerCdxFunc) (int, error) {
	if len(api.Urls) > 1 {
		if api.Sort == "" {
			return c.unsortedSerialSearch(api, perCdxFunc)
		}
		return c.sortedParallelSearch(api, perCdxFunc)
	} else {
		if api.Sort == SortClosest && api.MatchType == MatchTypeExact {
			return c.closestUniSearch(api, perCdxFunc)
		}
		return c.uniSearch(api, perCdxFunc)
	}
}

// unsortedParallelSearch searches the index database, sorts the results and processes each result with perCdxFunc.
func (c DbCdxServer) sortedParallelSearch(api *CdxjServerApi, perCdxFunc PerCdxFunc) (int, error) {
	var searchKeys []string
	for _, u := range api.Urls {
		key := parseKey(surt.UrlToSsurt(u), api.MatchType)
		searchKeys = append(searchKeys, key)
	}

	filter := parseFilter(api.Filter)

	t, _ := timestamp.Parse(api.Closest)
	closest := t.Unix()
	s := &sorter{
		reverse: api.Sort == SortReverse,
		closest: closest,
	}

	count := 0

	perItemFn := func(item *badger.Item) error {
		err := item.Value(func(val []byte) error {
			result := new(schema.Cdx)
			err := proto.Unmarshal(val, result)
			if err != nil {
				return err
			}

			// filter (exact, contains, regexp)
			if filter.eval(result) {
				count++
				return perCdxFunc(result)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("%s: %w", item.KeyCopy(nil), err)
		}
		return nil
	}

	err := c.Walk(func(txn *badger.Txn) error {
		items := make(chan []byte, len(searchKeys))

		done := make(chan struct{})

		go func() {
			for key := range items {
				s.add(key)
			}
			done <- struct{}{}
		}()

		// wg is used to synchronize multiple transaction iterators operating simultaneously.
		var wg sync.WaitGroup

		for _, key := range searchKeys {
			wg.Add(1)
			key := key

			go func() {
				defer wg.Done()
				opts := badger.DefaultIteratorOptions
				opts.PrefetchValues = false
				opts.Prefix = []byte(key)

				it := txn.NewIterator(opts)
				defer it.Close()

				for it.Seek([]byte(key)); it.ValidForPrefix([]byte(key)); it.Next() {
					k := it.Item().KeyCopy(nil)

					// filter from/to
					inDateRange, _ := api.FromTo.containsStr(Key(k).ts())
					if inDateRange {
						items <- k
					}
				}
			}()
		}
		wg.Wait()
		close(items)

		<-done
		s.sort()

		return s.walk(txn, func(item *badger.Item) (stopIteration bool) {
			if err := perItemFn(item); err != nil {
				return true
			}
			return false
		})
	})
	return count, err
}

func (c DbCdxServer) unsortedSerialSearch(api *CdxjServerApi, perCdxFunc PerCdxFunc) (int, error) {
	keys := make([]string, len(api.Urls))
	for i, url := range api.Urls {
		key := parseKey(surt.UrlToSsurt(url), api.MatchType)
		keys[i] = key
	}

	count := 0
	err := c.Walk(func(txn *badger.Txn) error {
		// initalize badger iterators
		iterators := make([]*badger.Iterator, len(keys))
		prefixes := make([][]byte, len(keys))
		for i, key := range keys {
			prefixes[i] = []byte(key)
			opts := badger.DefaultIteratorOptions
			opts.Prefix = prefixes[i]

			iterators[i] = txn.NewIterator(opts)
			defer iterators[i].Close()

			iterators[i].Seek(prefixes[i])
		}

	OUTER:
		for len(iterators) > 0 {
			// set timestamp to aprox max time.Time value
			earliestTimestamp := time.Unix(1<<62, 1<<62)
			earliestIndex := -1
			// find the earliest timestamp
			for i, iter := range iterators {
				// if iter is no longer valid, close it, remove it from slice and restart search
				if !iter.ValidForPrefix(prefixes[i]) {
					iteratorsLen := len(iterators)
					iterators[i].Close()
					// remove iterator from list
					iterators[i] = iterators[iteratorsLen-1]
					iterators = iterators[0 : iteratorsLen-1]
					continue OUTER
				}

				item := iter.Item()
				// in the event of parse error we get a zero timestamp
				ts, err := time.Parse(timeLayout, Key(item.Key()).ts())
				if err != nil {
					// current value seems invalid, iterate to next
					iter.Next()
					log.Warn().Msgf("error parsing item key '%s': %s", string(item.Key()), err)
					// do not return error as there might be more valid values to handle
					continue
				}

				inRange, _ := api.FromTo.containsTime(ts)
				if !inRange {
					// invalid value, go to next itme
					iter.Next()
					continue
				}

				if ts.Before(earliestTimestamp) {
					earliestTimestamp = ts
					earliestIndex = i
				}
			}
			if earliestIndex == -1 {
				break
			}
			iter := iterators[earliestIndex]

			cdx, err := unmarshal(iter.Item())
			if err != nil {
				return err
			}

			iter.Next()

			err = perCdxFunc(cdx)
			if err != nil {
				return err
			}
			count++

			if api.Limit > 0 && count >= api.Limit {
				break
			}
		}
		return nil
	})
	return count, err
}

func (c DbCdxServer) closestUniSearch(api *CdxjServerApi, perCdxFunc PerCdxFunc) (int, error) {
	u := api.Urls[0]
	s := surt.UrlToSsurt(u)
	key := parseKey(s, api.MatchType)
	closest := api.Closest
	t, _ := timestamp.Parse(api.Closest)
	cl := t.Unix()

	seek := key + closest

	isClosest := func(a int64, b int64) bool {
		return timestamp.AbsInt64(cl-a) <= timestamp.AbsInt64(cl-b)
	}

	count := 0

	err := c.Walk(func(txn *badger.Txn) error {
		prefix := []byte(key)

		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix

		forward := txn.NewIterator(opts)
		defer forward.Close()

		opts.Reverse = true
		backward := txn.NewIterator(opts)
		defer backward.Close()

		fk := []byte(seek)
		forward.Seek(fk)

		bk := []byte(seek + string(rune(0xff)))
		backward.Seek(bk)
		if forward.ValidForPrefix(prefix) && backward.ValidForPrefix(prefix) && bytes.Equal(forward.Item().Key(), backward.Item().Key()) {
			// if forward and backward iterator point to same item we advance the backward iterator
			backward.Next()
		}

		var ft int64
		var bt int64

		f := true
		b := true

		for {
			if f && forward.ValidForPrefix(prefix) {
				t, _ := timestamp.Parse(Key(forward.Item().Key()).ts())
				ft = t.Unix()
			} else if f {
				f = false
				ft = 0
			}

			if b && backward.ValidForPrefix(prefix) {
				t, _ := timestamp.Parse(Key(backward.Item().Key()).ts())
				bt = t.Unix()
			} else if b {
				b = false
				bt = 0
			}

			var it *badger.Iterator
			if f && isClosest(ft, bt) {
				it = forward
			} else if b {
				it = backward
			} else {
				return nil
			}

			cdx, err := unmarshal(it.Item())
			if err != nil {
				return err
			}

			it.Next()

			err = perCdxFunc(cdx)
			if err != nil {
				return err
			}
			count++

			if api.Limit > 0 && count >= api.Limit {
				break
			}
		}
		return nil
	})
	return count, err
}

// uniSearch the index database and render each item with the provided renderFunc.
func (c DbCdxServer) uniSearch(api *CdxjServerApi, perCdxFunc PerCdxFunc) (int, error) {
	u := api.Urls[0]
	s := surt.UrlToSsurt(u)

	key := parseKey(s, api.MatchType)
	filter := parseFilter(api.Filter)
	reverse := api.Sort == SortReverse
	count := 0

	err := c.Walk(func(txn *badger.Txn) error {
		prefix := []byte(key)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = prefix
		opts.Reverse = reverse

		it := txn.NewIterator(opts)
		defer it.Close()

		// see https://github.com/dgraph-io/badger/issues/436 for details regarding reverse seeking
		seekKey := key
		if reverse {
			seekKey += string(rune(0xff))
		}

		for it.Seek([]byte(seekKey)); it.ValidForPrefix(prefix); it.Next() {
			contains, _ := api.FromTo.containsStr(Key(it.Item().Key()).ts())
			if !contains {
				continue
			}

			err := it.Item().Value(func(v []byte) error {
				result := new(schema.Cdx)
				if err := proto.Unmarshal(v, result); err != nil {
					return err
				}

				if filter.eval(result) {
					if err := perCdxFunc(result); err != nil {
						return err
					}
					count++
				}
				return nil
			})
			if err != nil {
				return fmt.Errorf("failed to process item value")
			}

			if api.Limit > 0 && count > api.Limit {
				break
			}
		}
		return nil
	})
	return count, err
}
