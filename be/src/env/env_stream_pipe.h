// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "env/env.h"

namespace starrocks {
class StreamLoadPipe;

// Since the StreamPipe does not have the standard
// SequentialFile interface. The StreamLoadPipeInputStream
// is used to wrap the memory to provide the sequential API.
// TODO: remove this class
class StreamLoadPipeInputStream : public io::InputStream {
public:
    explicit StreamLoadPipeInputStream(std::shared_ptr<StreamLoadPipe> file);
    ~StreamLoadPipeInputStream() override;

    StatusOr<int64_t> read(void* data, int64_t size) override;

    Status read_one_message(std::unique_ptr<uint8_t[]>* buf, size_t* length, size_t padding = 0);

    Status skip(int64_t n) override;

private:
    std::shared_ptr<StreamLoadPipe> _file;
};

} // namespace starrocks
