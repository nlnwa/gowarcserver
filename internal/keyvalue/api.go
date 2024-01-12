package keyvalue

// This file contains implementations of some of
// the interfaces from the index package.

import (
	"github.com/nlnwa/gowarcserver/index"
	"github.com/nlnwa/gowarcserver/schema"
)

// IdResponse implements the index.IdResponse interface.
type IdResponse struct {
	Key   string
	Value string
	Error error
}

func (ir IdResponse) GetId() string {
	return ir.Key
}

func (ir IdResponse) GetValue() string {
	return ir.Value
}

func (ir IdResponse) GetError() error {
	return ir.Error
}

// Assert IdResponse implements the index.IdResponse interface.
var _ index.IdResponse = IdResponse{}

// CdxResponse implements the index.CdxResponse interface.
type CdxResponse struct {
	Key   CdxKey
	Value *schema.Cdx
	Error error
}

func (cr CdxResponse) GetCdx() *schema.Cdx {
	return cr.Value
}

func (cr CdxResponse) GetError() error {
	return cr.Error
}

// Assert CdxResponse implements the index.CdxResponse interface.
var _ index.CdxResponse = CdxResponse{}

// FileInfoResponse implements the index.FileInfoResponse interface.
type FileInfoResponse struct {
	FileInfo *schema.FileInfo
	Error    error
}

func (fir FileInfoResponse) GetFileInfo() *schema.FileInfo {
	return fir.FileInfo
}

func (fir FileInfoResponse) GetError() error {
	return fir.Error
}

// Assert FileInfoResponse implements the index.FileInfoResponse interface.
var _ index.FileInfoResponse = FileInfoResponse{}

// ReportResponse implements the index.ReportResponse interface.
type ReportResponse struct {
	Value *schema.Report
	Error error
}

func (rr ReportResponse) GetReport() *schema.Report {
	return rr.Value
}

func (rr ReportResponse) GetError() error {
	return rr.Error
}

// Assert ReportResponse implements the index.ReportResponse interface.
var _ index.ReportResponse = ReportResponse{}
