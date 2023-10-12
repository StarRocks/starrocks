// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "storage/protobuf_file.h"

#include <fmt/format.h>
#include <google/protobuf/message.h>

#include "fs/fs.h"
#include "storage/olap_define.h"
#include "storage/utils.h"
#include "testutil/sync_point.h"
#include "util/raw_container.h"

namespace starrocks {

typedef struct _FixedFileHeader {
    uint64_t magic_number;
    uint32_t version;
    // file's total length
    uint64_t file_length;
    // checksum for content except FileHeader
    uint32_t checksum;
    // length for protobuf content
    uint64_t protobuf_length;
    // checksum for protobuf
    uint32_t protobuf_checksum;
} __attribute__((packed)) FixedFileHeader;

Status ProtobufFileWithHeader::save(const ::google::protobuf::Message& message, bool sync) {
    uint32_t unused_flag = 0;
    FixedFileHeader header;
    std::string serialized_message;
    bool r = message.SerializeToString(&serialized_message);
    TEST_SYNC_POINT_CALLBACK("ProtobufFileWithHeader::save:serialize", &r);
    if (UNLIKELY(!r)) {
        return Status::InternalError(
                fmt::format("failed to serialize protobuf to string, maybe the protobuf is too large. path={}", _path));
    }
    header.protobuf_checksum = olap_adler32(ADLER32_INIT, serialized_message.c_str(), serialized_message.size());
    header.checksum = 0;
    header.protobuf_length = serialized_message.size();
    header.file_length = sizeof(header) + sizeof(unused_flag) + serialized_message.size();
    header.version = OLAP_DATA_VERSION_APPLIED;
    header.magic_number = OLAP_FIX_HEADER_MAGIC_NUMBER;

    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(_path));
    WritableFileOptions opts{.sync_on_close = sync, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_RETURN(auto output_file, fs->new_writable_file(opts, _path));
    RETURN_IF_ERROR(output_file->append(Slice((const char*)(&header), sizeof(header))));
    RETURN_IF_ERROR(output_file->append(Slice((const char*)(&unused_flag), sizeof(unused_flag))));
    RETURN_IF_ERROR(output_file->append(serialized_message));
    RETURN_IF_ERROR(output_file->close());
    return Status::OK();
}

Status ProtobufFileWithHeader::load(::google::protobuf::Message* message, bool fill_cache) {
    SequentialFileOptions opts{.skip_fill_local_cache = !fill_cache};
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(_path));
    ASSIGN_OR_RETURN(auto input_file, fs->new_sequential_file(opts, _path));

    FixedFileHeader header;
    ASSIGN_OR_RETURN(auto nread, input_file->read(&header, sizeof(header)));
    if (nread != sizeof(header)) {
        return Status::Corruption(fmt::format("failed to read header of protobuf file {}", _path));
    }
    if (header.magic_number != OLAP_FIX_HEADER_MAGIC_NUMBER) {
        return Status::Corruption(fmt::format("invalid magic number of protobuf file {}", _path));
    }

    uint32_t unused_flag; // unused, read for compatibility
    ASSIGN_OR_RETURN(nread, input_file->read(&unused_flag, sizeof(unused_flag)));
    if (UNLIKELY(nread != sizeof(unused_flag))) {
        return Status::Corruption(fmt::format("fail to read flag of protobuf file {}", _path));
    }

    std::string str;
    raw::stl_string_resize_uninitialized(&str, header.protobuf_length + 1);
    ASSIGN_OR_RETURN(nread, input_file->read(str.data(), str.size()));
    str.resize(nread);
    if (str.size() != header.protobuf_length) {
        return Status::Corruption(fmt::format("mismatched message size of protobuf file {}. real={} expect={}", _path,
                                              nread, (int64_t)header.protobuf_length));
    }
    if (olap_adler32(ADLER32_INIT, str.data(), str.size()) != header.protobuf_checksum) {
        return Status::Corruption(fmt::format("mismatched checksum of protobuf file {}", _path));
    }
    if (!message->ParseFromString(str)) {
        return Status::Corruption(fmt::format("failed to parse protobuf file {}", _path));
    }
    return Status::OK();
}

Status ProtobufFile::save(const ::google::protobuf::Message& message, bool sync) {
    std::string serialized_message;
    bool r = message.SerializeToString(&serialized_message);
    TEST_SYNC_POINT_CALLBACK("ProtobufFile::save:serialize", &r);
    if (UNLIKELY(!r)) {
        return Status::InternalError(
                fmt::format("failed to serialize protobuf to string, maybe the protobuf is too large. path={}", _path));
    }
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(_path));
    WritableFileOptions opts{.sync_on_close = sync, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_RETURN(auto output_file, fs->new_writable_file(opts, _path));
    RETURN_IF_ERROR(output_file->append(serialized_message));
    RETURN_IF_ERROR(output_file->close());
    return Status::OK();
}

Status ProtobufFile::load(::google::protobuf::Message* message, bool fill_cache) {
    RandomAccessFileOptions opts{.skip_fill_local_cache = !fill_cache};
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(_path));
    ASSIGN_OR_RETURN(auto input_file, fs->new_random_access_file(opts, _path));
    ASSIGN_OR_RETURN(auto file_size, input_file->get_size());
    TEST_SYNC_POINT_CALLBACK("ProtobufFile::load:get_size", &file_size);
    if (UNLIKELY(file_size > std::numeric_limits<int>::max())) {
        return Status::Corruption(fmt::format("protobuf file too large: {}", file_size));
    }
    std::string serialized_string;
    raw::stl_string_resize_uninitialized(&serialized_string, file_size);
    RETURN_IF_ERROR(input_file->read_at_fully(0, serialized_string.data(), file_size));
    TEST_SYNC_POINT_CALLBACK("ProtobufFile::load:2", &serialized_string);
    bool parsed = message->ParseFromArray(serialized_string.data(), static_cast<int>(file_size));
    if (!parsed) {
        return Status::Corruption(fmt::format("failed to parse protobuf file {}", _path));
    }
    return Status::OK();
}

} // namespace starrocks
