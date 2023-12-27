/*
 * Copyright 2021 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package index

import (
	"context"

	"github.com/nlnwa/gowarcserver/schema"
)

type Deleter interface {
	Delete(context.Context) error
}

type Runner interface {
	Run(context.Context) error
}

type indexError string

const AlreadyIndexedError indexError = "already indexed"

func (a indexError) Error() string {
	return string(a)
}

type DateRange interface {
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
