// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

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

    Status read(Slice* result) override;
    Status read_one_message(std::unique_ptr<uint8_t[]>* buf, size_t* length);

    Status skip(uint64_t n) override;
    const std::string& filename() const override { return _filename; }

private:
    std::shared_ptr<StreamLoadPipe> _file;
    std::string _filename = "StreamPipeSequentialFile";
};

} // namespace starrocks
