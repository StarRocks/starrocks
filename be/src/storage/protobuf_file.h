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

#pragma once

#include <string>
#include <utility>

#include "common/status.h"
#include "common/storage_define.h"
#include "gutil/macros.h"

namespace google::protobuf {
class Message;
}

namespace starrocks {

class FileSystem;
class Slice;

class ProtobufFile {
public:
    explicit ProtobufFile(std::string path) : _path(std::move(path)) {}

    explicit ProtobufFile(std::string path, std::shared_ptr<FileSystem> fs)
            : _path(std::move(path)), _fs(std::move(fs)) {}

    DISALLOW_COPY_AND_MOVE(ProtobufFile);

    Status save(const ::google::protobuf::Message& message, bool sync = true);

    Status load(::google::protobuf::Message* message, bool fill_cache = true);

private:
    std::string _path;
    std::shared_ptr<FileSystem> _fs;
};

// Reads/writes a protobuf message prefixed with a FixedFileHeader that carries an Adler-32
// checksum over the serialized message.
//
// |magic| identifies the on-disk format. The shared-nothing storage engine uses the default
// OLAP_FIX_HEADER_MAGIC_NUMBER; shared-data (lake) metadata/txn-log files use
// LAKE_META_HEADER_MAGIC_NUMBER. When |allow_plain_protobuf_fallback| is true, load() will,
// upon failing to find the expected magic, fall back to parsing the bytes as a legacy
// headerless protobuf message. This keeps readers able to consume files written before the
// checksum format was introduced (the magic's first on-disk byte is chosen so legacy
// protobuf can never be misdetected as the checksummed format).
class ProtobufFileWithHeader {
public:
    explicit ProtobufFileWithHeader(std::string path, uint64_t magic = OLAP_FIX_HEADER_MAGIC_NUMBER,
                                    bool allow_plain_protobuf_fallback = false)
            : _path(std::move(path)), _magic(magic), _allow_plain_protobuf_fallback(allow_plain_protobuf_fallback) {}

    explicit ProtobufFileWithHeader(std::string path, std::shared_ptr<FileSystem> fs,
                                    uint64_t magic = OLAP_FIX_HEADER_MAGIC_NUMBER,
                                    bool allow_plain_protobuf_fallback = false)
            : _path(std::move(path)),
              _fs(std::move(fs)),
              _magic(magic),
              _allow_plain_protobuf_fallback(allow_plain_protobuf_fallback) {}

    DISALLOW_COPY_AND_MOVE(ProtobufFileWithHeader);

    Status save(const ::google::protobuf::Message& message, bool sync = true);

    Status load(::google::protobuf::Message* message, bool fill_cache = true);

    static Status load_from_buffer(::google::protobuf::Message* message, std::string_view data,
                                   uint64_t magic = OLAP_FIX_HEADER_MAGIC_NUMBER,
                                   bool allow_plain_protobuf_fallback = false);

private:
    std::string _path;
    std::shared_ptr<FileSystem> _fs;
    uint64_t _magic{OLAP_FIX_HEADER_MAGIC_NUMBER};
    bool _allow_plain_protobuf_fallback{false};
};

} // namespace starrocks
