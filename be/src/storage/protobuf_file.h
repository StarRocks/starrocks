// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <string>

#include "common/status.h"

namespace google::protobuf {
class Message;
}

namespace starrocks {

class Env;

class ProtobufFile {
public:
    ProtobufFile(std::string path, Env* env = nullptr);
    ~ProtobufFile() = default;

    Status save(const ::google::protobuf::Message& message, bool sync);

    Status load(::google::protobuf::Message* message);

private:
    std::string _path;
    Env* _env;
};

} // namespace starrocks
