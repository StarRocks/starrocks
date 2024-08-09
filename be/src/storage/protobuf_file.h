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

#include "common/status.h"
#include "gutil/macros.h"

namespace google::protobuf {
class Message;
}

namespace starrocks {

class FileSystem;

class ProtobufFile {
public:
    explicit ProtobufFile(std::string path) : _path(std::move(path)) {}

    explicit ProtobufFile(std::string path, std::shared_ptr<FileSystem> fs) : _path(std::move(path)), _fs(fs) {}

    DISALLOW_COPY_AND_MOVE(ProtobufFile);

    Status save(const ::google::protobuf::Message& message, bool sync = true);

    Status load(::google::protobuf::Message* message, bool fill_cache = true);

private:
    std::string _path;
    std::shared_ptr<FileSystem> _fs;
};

class ProtobufFileWithHeader {
public:
    explicit ProtobufFileWithHeader(std::string path) : _path(std::move(path)) {}

    explicit ProtobufFileWithHeader(std::string path, std::shared_ptr<FileSystem> fs)
            : _path(std::move(path)), _fs(fs) {}

    DISALLOW_COPY_AND_MOVE(ProtobufFileWithHeader);

    Status save(const ::google::protobuf::Message& message, bool sync = true);

    Status load(::google::protobuf::Message* message, bool fill_cache = true);

    static Status load(::google::protobuf::Message* message, std::string_view data);

private:
    std::string _path;
    std::shared_ptr<FileSystem> _fs;
};

} // namespace starrocks
