// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <string>

#include "common/status.h"

namespace google::protobuf {
class Message;
}

namespace starrocks {

class FileSystem;

class ProtobufFile {
public:
    ProtobufFile(std::string path, FileSystem* fs = nullptr);
    ~ProtobufFile() = default;

    Status save(const ::google::protobuf::Message& message, bool sync);

    Status load(::google::protobuf::Message* message);

private:
    std::string _path;
    FileSystem* _fs;
};

} // namespace starrocks
