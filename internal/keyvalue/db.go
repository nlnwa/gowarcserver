package keyvalue

import (
	"bytes"
	"time"

	"github.com/nlnwa/gowarcserver/index"
	"github.com/nlnwa/gowarcserver/schema"
	"github.com/nlnwa/gowarcserver/timestamp"
	"google.golang.org/protobuf/proto"
)

func ClosestWithPrefix(req index.Request, prefix string) ([]byte, []byte) {
	ssurt := scope(req.Ssurt(), index.MatchTypeExact)
	host, _, path := SplitSSURT(ssurt)
	closest := req.Closest()
	p := prefix + host + path
	return []byte(p), []byte(p + closest)
}

func Closest(req index.Request) ([]byte, []byte) {
	return ClosestWithPrefix(req, "")
}

func SearchKeyWithPrefix(req index.Request, prefix string) []byte {
	host, _, path := SplitSSURT(scope(req.Ssurt(), req.MatchType()))
	return []byte(prefix + host + path)
}

func SearchKey(req index.Request) []byte {
	return SearchKeyWithPrefix(req, "")
}

func KeyWithPrefix(key string, prefix string) []byte {
	return []byte(prefix + key)
}

func Key(key string) []byte {
	return KeyWithPrefix(key, "")
}

// MarshalCdxWithPrefix takes a record and returns a key-value pair for the cdx index.
func MarshalCdxWithPrefix(r index.Record, prefix string) (key []byte, value []byte, err error) {
	ts := timestamp.TimeTo14(r.GetSts().AsTime())
	ssurt := r.GetSsu()
	host, schemeAndUserinfo, path := SplitSSURT(ssurt)
	key = []byte(prefix + host + path + " " + ts + " " + schemeAndUserinfo + " " + r.GetSrt())
	value, err = r.Marshal()
	return
}

func MarshalCdx(r index.Record) (key []byte, value []byte, err error) {
	return MarshalCdxWithPrefix(r, "")
}

// MarshalId takes a record and returns a key-value pair for the id index.
func MarshalId(r index.Record, prefix string) (key []byte, value []byte, err error) {
	key = []byte(prefix + r.GetRid())
	value = []byte(r.GetRef())
	return
}

// MarshalFileInfo takes a fileinfo and returns a key-value pair for the file index.
func MarshalFileInfo(fileInfo *schema.FileInfo, prefix string) (key []byte, value []byte, err error) {
	key = KeyWithPrefix(fileInfo.Name, prefix)
	value, err = proto.Marshal(fileInfo)
	return
}

// MarshalReport takes a report and returns a key-value pair for the report index.
func MarshalReport(report *schema.Report, prefix string) (key []byte, value []byte, err error) {
	key = KeyWithPrefix(report.Id, prefix)
	value, err = proto.Marshal(report)
	return
}

// CdxKey is a wrapper around the key used in the cdx index
type CdxKey []byte

var spaceCharacter = []byte{32}
var colonCharacter = []byte{58}
var slashCharacter = []byte{47}

func (ck CdxKey) String() string {
	return string(ck)
}

func (ck CdxKey) DomainAndPath() []byte {
	return bytes.Split(ck, spaceCharacter)[0]
}

func (ck CdxKey) Path() string {
	b := ck.DomainAndPath()
	i := bytes.Index(b, slashCharacter)
	if i == -1 {
		return ""
	}
	return string(b[i:])
}

func (ck CdxKey) Domain() string {
	b := ck.DomainAndPath()
	return string(bytes.Split(b, slashCharacter)[0])
}

func (ck CdxKey) Time() time.Time {
	b := bytes.Split(ck, spaceCharacter)[1]
	t, _ := timestamp.Parse(string(b))
	return t
}

// Unix returns the time part of the key as unix time.
func (ck CdxKey) Unix() int64 {
	return ck.Time().Unix()
}

func (ck CdxKey) SchemeAndUserInfo() string {
	b := bytes.Split(ck, spaceCharacter)[2]
	return string(b)
}

func (ck CdxKey) Port() string {
	b := bytes.Split(ck, spaceCharacter)[2]
	return string(bytes.Split(b, colonCharacter)[0])
}

func (ck CdxKey) Scheme() string {
	b := bytes.Split(ck, spaceCharacter)[2]
	return string(bytes.Split(b, colonCharacter)[1])
}

func (ck CdxKey) UserInfo() string {
	b := bytes.Split(ck, spaceCharacter)[2]
	return string(bytes.Split(b, colonCharacter)[2])
}
