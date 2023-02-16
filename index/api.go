package index

import (
	"context"
	"time"

	"github.com/nlnwa/gowarcserver/schema"
)

type Runner interface {
	Run(context.Context) error
}

type indexError string

const AlreadyIndexedError indexError = "already indexed"

func (a indexError) Error() string {
	return string(a)
}

type DateRange interface {
	ContainsTime(time.Time) (bool, error)
	ContainsStr(string) (bool, error)
	Contains(int64) bool
}

type Filter interface {
	Eval(*schema.Cdx) bool
}

type Sort int

const (
	SortNone Sort = iota
	SortDesc
	SortAsc
	SortClosest
)

type Request interface {
	Key() string
	Keys() []string
	Sort() Sort
	DateRange() DateRange
	Filter() Filter
	Limit() int
	Closest() string
	MatchType() string
}

type FileAPI interface {
	GetFileInfo(ctx context.Context, filename string) (*schema.Fileinfo, error)
	ListFileInfo(context.Context, int, chan<- FileInfoResponse) error
}

type IdAPI interface {
	GetStorageRef(ctx context.Context, warcId string) (string, error)
	ListStorageRef(context.Context, int, chan<- IdResponse) error
}

type CdxAPI interface {
	List(context.Context, int, chan<- CdxResponse) error
	Search(context.Context, Request, chan<- CdxResponse) error
	Closest(context.Context, Request, chan<- CdxResponse) error
}

type FileInfoResponse struct {
	*schema.Fileinfo
	Error error
}

type CdxResponse struct {
	*schema.Cdx
	Error error
}

type IdResponse struct {
	Key   string
	Value string
	Error error
}
