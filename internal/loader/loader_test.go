/*
 * Copyright 2020 National Library of Norway.
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

package loader

// func TestLoader_Get(t *testing.T) {
// 	loader := &Loader{
// 		StorageRefResolver: &mockStorageRefResolver{},
// 		RecordLoader:       &FileStorageLoader{},
// 	}

// 	type args struct {
// 		warcId string
// 	}

// 	opts := gowarc.WithVersion(gowarc.V1_0)

// 	infoBuilder := gowarc.NewRecordBuilder(gowarc.Warcinfo, opts)
// 	v1InfoRecord, _, err := infoBuilder.Build()
// 	if err != nil {
// 		t.Fatalf("Warcinfo WarcRecord creation failed: %s", err)
// 	}

// 	v1ResponseRecord, _, err := gowarc.NewRecordBuilder(gowarc.Response, opts).Build()
// 	if err != nil {
// 		t.Fatalf("Warcinfo WarcRecord creation failed: %s", err)
// 	}

// 	v1RequestRecord, _, err := gowarc.NewRecordBuilder(gowarc.Request, opts).Build()
// 	if err != nil {
// 		t.Fatalf("Warcinfo WarcRecord creation failed: %s", err)
// 	}
// 	tests := []struct {
// 		name       string
// 		args       args
// 		wantRecord gowarc.WarcRecord
// 	}{
// 		{
// 			"base1",
// 			args{"urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008"},
// 			v1InfoRecord,
// 		},
// 		{
// 			"base2",
// 			args{"urn:uuid:a9c51e3e-0221-11e7-bf66-0242ac120005"},
// 			v1ResponseRecord,
// 		},
// 		{
// 			"base3",
// 			args{"urn:uuid:e9a0ee48-0221-11e7-adb1-0242ac120008"},
// 			v1InfoRecord,
// 		},
// 		{
// 			"base4",
// 			args{"urn:uuid:a9c5c23a-0221-11e7-8fe3-0242ac120007"},
// 			v1RequestRecord,
// 		},
// 		{
// 			"base5",
// 			args{"urn:uuid:e6e41fea-0221-11e7-8fe3-0242ac120007"},
// 			v1RequestRecord,
// 		},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			ctx, cancel := context.WithCancel(context.Background())
// 			defer cancel()

// 			gotRecord, err := loader.Load(ctx, tt.args.warcId)
// 			if err != nil {
// 				t.Errorf("Loader.Load() error = %v", err)
// 				return
// 			}

// 			if reflect.DeepEqual(gotRecord, tt.wantRecord) {
// 				t.Errorf("\nLoader.Load() = \n%v\nWant = \n%v", gotRecord, tt.wantRecord)
// 			}
// 		})
// 	}
// }

type mockStorageRefResolver struct{}

func (m mockStorageRefResolver) Resolve(warcId string) (storageRef string, err error) {
	switch warcId {
	case "urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008":
		storageRef = "warcfile:../../testdata/example.warc#0"
	case "urn:uuid:e9a0ee48-0221-11e7-adb1-0242ac120008":
		storageRef = "warcfile:../../testdata/example.warc#488"
	case "urn:uuid:a9c51e3e-0221-11e7-bf66-0242ac120005":
		storageRef = "warcfile:../../testdata/example.warc#1197"
	case "urn:uuid:a9c5c23a-0221-11e7-8fe3-0242ac120007":
		storageRef = "warcfile:../../testdata/example.warc#3078"
	case "urn:uuid:e6e395ca-0221-11e7-a18d-0242ac120005":
		storageRef = "warcfile:../../testdata/example.warc#3370"
	case "urn:uuid:e6e41fea-0221-11e7-8fe3-0242ac120007":
		storageRef = "warcfile:../../testdata/example.warc#4828"
	}
	return
}
