// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.20.1
// 	protoc        v3.11.4
// source: fileinfo.proto

package gowarcpb

import (
	proto "github.com/golang/protobuf/proto"
	timestamp "github.com/golang/protobuf/ptypes/timestamp"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// This is a compile-time assertion that a sufficiently up-to-date version
// of the legacy proto package is being used.
const _ = proto.ProtoPackageIsVersion4

type Fileinfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Filename
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// Full path
	Path string `protobuf:"bytes,2,opt,name=path,proto3" json:"path,omitempty"`
	// Last modified
	LastModified *timestamp.Timestamp `protobuf:"bytes,3,opt,name=last_modified,json=lastModified,proto3" json:"last_modified,omitempty"`
	// File size
	Size int64 `protobuf:"varint,4,opt,name=size,proto3" json:"size,omitempty"`
}

func (x *Fileinfo) Reset() {
	*x = Fileinfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_fileinfo_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Fileinfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Fileinfo) ProtoMessage() {}

func (x *Fileinfo) ProtoReflect() protoreflect.Message {
	mi := &file_fileinfo_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Fileinfo.ProtoReflect.Descriptor instead.
func (*Fileinfo) Descriptor() ([]byte, []int) {
	return file_fileinfo_proto_rawDescGZIP(), []int{0}
}

func (x *Fileinfo) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Fileinfo) GetPath() string {
	if x != nil {
		return x.Path
	}
	return ""
}

func (x *Fileinfo) GetLastModified() *timestamp.Timestamp {
	if x != nil {
		return x.LastModified
	}
	return nil
}

func (x *Fileinfo) GetSize() int64 {
	if x != nil {
		return x.Size
	}
	return 0
}

var File_fileinfo_proto protoreflect.FileDescriptor

var file_fileinfo_proto_rawDesc = []byte{
	0x0a, 0x0e, 0x66, 0x69, 0x6c, 0x65, 0x69, 0x6e, 0x66, 0x6f, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x12, 0x0f, 0x67, 0x6f, 0x77, 0x61, 0x72, 0x63, 0x2e, 0x66, 0x69, 0x6c, 0x65, 0x69, 0x6e, 0x66,
	0x6f, 0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62,
	0x75, 0x66, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x22, 0x87, 0x01, 0x0a, 0x08, 0x46, 0x69, 0x6c, 0x65, 0x69, 0x6e, 0x66, 0x6f, 0x12,
	0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e,
	0x61, 0x6d, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x70, 0x61, 0x74, 0x68, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x04, 0x70, 0x61, 0x74, 0x68, 0x12, 0x3f, 0x0a, 0x0d, 0x6c, 0x61, 0x73, 0x74, 0x5f,
	0x6d, 0x6f, 0x64, 0x69, 0x66, 0x69, 0x65, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a,
	0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66,
	0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x0c, 0x6c, 0x61, 0x73, 0x74,
	0x4d, 0x6f, 0x64, 0x69, 0x66, 0x69, 0x65, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x73, 0x69, 0x7a, 0x65,
	0x18, 0x04, 0x20, 0x01, 0x28, 0x03, 0x52, 0x04, 0x73, 0x69, 0x7a, 0x65, 0x42, 0x22, 0x5a, 0x20,
	0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x6e, 0x6c, 0x6e, 0x77, 0x61,
	0x2f, 0x67, 0x6f, 0x77, 0x61, 0x72, 0x63, 0x2f, 0x67, 0x6f, 0x77, 0x61, 0x72, 0x63, 0x70, 0x62,
	0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_fileinfo_proto_rawDescOnce sync.Once
	file_fileinfo_proto_rawDescData = file_fileinfo_proto_rawDesc
)

func file_fileinfo_proto_rawDescGZIP() []byte {
	file_fileinfo_proto_rawDescOnce.Do(func() {
		file_fileinfo_proto_rawDescData = protoimpl.X.CompressGZIP(file_fileinfo_proto_rawDescData)
	})
	return file_fileinfo_proto_rawDescData
}

var file_fileinfo_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_fileinfo_proto_goTypes = []interface{}{
	(*Fileinfo)(nil),            // 0: gowarc.fileinfo.Fileinfo
	(*timestamp.Timestamp)(nil), // 1: google.protobuf.Timestamp
}
var file_fileinfo_proto_depIdxs = []int32{
	1, // 0: gowarc.fileinfo.Fileinfo.last_modified:type_name -> google.protobuf.Timestamp
	1, // [1:1] is the sub-list for method output_type
	1, // [1:1] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_fileinfo_proto_init() }
func file_fileinfo_proto_init() {
	if File_fileinfo_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_fileinfo_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Fileinfo); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_fileinfo_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_fileinfo_proto_goTypes,
		DependencyIndexes: file_fileinfo_proto_depIdxs,
		MessageInfos:      file_fileinfo_proto_msgTypes,
	}.Build()
	File_fileinfo_proto = out.File
	file_fileinfo_proto_rawDesc = nil
	file_fileinfo_proto_goTypes = nil
	file_fileinfo_proto_depIdxs = nil
}