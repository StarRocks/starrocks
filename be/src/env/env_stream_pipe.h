// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "env/env.h"

namespace starrocks {
class StreamLoadPipe;

// Since the StreamPipe does not have the standard
// SequentialFile interface. The StreamPipeSequentialFile
// is used to wrap the memory to provide the sequential API.
class StreamPipeSequentialFile : public SequentialFile {
public:
    explicit StreamPipeSequentialFile(std::shared_ptr<StreamLoadPipe> file);
    ~StreamPipeSequentialFile() override;

    StatusOr<int64_t> read(void* data, int64_t size) override;

    Status read_one_message(std::unique_ptr<uint8_t[]>* buf, size_t* buf_cap, size_t* buf_sz, size_t padding = 0);

    Status skip(uint64_t n) override;
    const std::string& filename() const override { return _filename; }

private:
    std::shared_ptr<StreamLoadPipe> _file;
    std::string _filename = "StreamPipeSequentialFile";
};

} // namespace starrocks
