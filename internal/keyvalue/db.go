package keyvalue

import (
	"bytes"
	"github.com/nlnwa/gowarcserver/index"
	"github.com/nlnwa/gowarcserver/schema"
	"github.com/nlnwa/gowarcserver/timestamp"
	"google.golang.org/protobuf/proto"
)

// MarshalCdx takes a record and returns a key-value pair for the cdx index.
func MarshalCdx(r index.Record, prefix string) (key []byte, value []byte, err error) {
	ts := timestamp.TimeTo14(r.GetSts().AsTime())
	key = []byte(prefix + r.GetSsu() + " " + ts + " " + r.GetSrt())
	value, err = r.Marshal()
	return
}

// MarshalId takes a record and returns a key-value pair for the id index.
func MarshalId(r index.Record, prefix string) (key []byte, value []byte, err error) {
	key = []byte(prefix + r.GetRid())
	value = []byte(r.GetRef())
	return
}

// MarshalFileInfo takes a fileinfo and returns a key-value pair for the file index.
func MarshalFileInfo(fileInfo *schema.Fileinfo, prefix string) (key []byte, value []byte, err error) {
	key = []byte(prefix + fileInfo.Name)
	value, err = proto.Marshal(fileInfo)
	return
}

