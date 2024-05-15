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
// The key consists of the following parts separated by a space character:
// 1. surt domain and path (<surt domain>/<path>)
// 2. timestamp (14 digits)
// 3. port, scheme and userinfo (port:scheme@userinfo:)
// 4. response type (response)
//
// Example:
//
//	test,example,/path 20200101000000 8080:http@user:password: response
type CdxKey []byte

// byte constants used in the key
var (
	spaceCharacter = []byte{32} // ' '
	colonCharacter = []byte{58} // ':'
	slashCharacter = []byte{47} // '/'
	atCharacter    = []byte{64} // '@'
)

func (ck CdxKey) domainPath() []byte {
	parts := bytes.Split(ck, spaceCharacter)
	return parts[0]
}

func (ck CdxKey) timestamp() []byte {
	parts := bytes.Split(ck, spaceCharacter)
	return parts[1]
}

func (ck CdxKey) portSchemeUserInfo() []byte {
	parts := bytes.Split(ck, spaceCharacter)
	if len(parts) < 3 {
		return nil
	}
	return parts[2]
}

func (ck CdxKey) responseType() []byte {
	parts := bytes.Split(ck, spaceCharacter)
	if len(parts) < 4 {
		return nil
	}
	return parts[3]
}

func (ck CdxKey) port() []byte {
	return bytes.Split(ck.portSchemeUserInfo(), colonCharacter)[0]
}

func (ck CdxKey) schemeUserInfo() []byte {
	portSchemeUserInfo := ck.portSchemeUserInfo()
	if portSchemeUserInfo == nil {
		return nil
	}

	portAndSchemeAndUserInfo := bytes.SplitN(ck.portSchemeUserInfo(), colonCharacter, 2)
	if len(portAndSchemeAndUserInfo) < 2 {
		return nil
	}
	schemeUserInfo := portAndSchemeAndUserInfo[1]
	return bytes.TrimRight(schemeUserInfo, ":")
}

func (ck CdxKey) String() string {
	return string(ck)
}

func (ck CdxKey) Domain() string {
	b := ck.domainPath()
	return string(bytes.Split(b, slashCharacter)[0])
}

func (ck CdxKey) Path() string {
	b := ck.domainPath()
	i := bytes.Index(b, slashCharacter)
	if i == -1 {
		return ""
	}
	return string(b[i:])
}

func (ck CdxKey) Time() time.Time {
	t, _ := timestamp.Parse(string(ck.timestamp()))
	return t
}

// Unix returns the time part of the key as unix time.
func (ck CdxKey) Unix() int64 {
	return ck.Time().Unix()
}

func (ck CdxKey) Port() string {
	return string(ck.port())
}

func (ck CdxKey) PortSchemeUserInfo() string {
	portSchemeUserInfo := ck.portSchemeUserInfo()
	if portSchemeUserInfo == nil {
		return ""
	}
	return string(portSchemeUserInfo)
}

func (ck CdxKey) Scheme() string {
	schemeUserInfo := ck.schemeUserInfo()
	if schemeUserInfo == nil {
		return ""
	}
	scheme := bytes.Split(schemeUserInfo, atCharacter)[0]
	return string(scheme)
}

func (ck CdxKey) UserInfo() string {
	schemeUserInfo := ck.schemeUserInfo()
	if schemeUserInfo == nil {
		return ""
	}
	schemeAndUserInfo := bytes.Split(schemeUserInfo, atCharacter)
	if len(schemeAndUserInfo) < 2 {
		return ""
	}
	userInfo := schemeAndUserInfo[1]
	return string(userInfo)
}

func (ck CdxKey) ResponseType() string {
	return string(ck.responseType())
}