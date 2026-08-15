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

#include <cstdint>
#include <memory>
#include <string>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "common/statusor.h"
#include "formats/file_commit_result.h"

namespace starrocks::formats {

class AsyncFlushOutputStream;

struct FileWriterOptions {
    virtual ~FileWriterOptions() = default;
};

class FileWriter {
public:
    virtual ~FileWriter() = default;
    virtual Status init() = 0;
    virtual int64_t get_written_bytes() = 0;
    virtual int64_t get_allocated_bytes() = 0;
    virtual int64_t get_flush_batch_size() = 0;
    virtual Status write(Chunk* chunk) = 0;
    virtual FileCommitResult close() = 0;
};

struct WriterAndStream {
    std::unique_ptr<AsyncFlushOutputStream> stream;
    std::unique_ptr<FileWriter> writer;
};

class FileWriterFactory {
public:
    virtual ~FileWriterFactory() = default;

    virtual Status init() = 0;

    virtual StatusOr<WriterAndStream> create(const std::string& path) const = 0;
};

class UnknownFileWriterFactory : public FileWriterFactory {
public:
    UnknownFileWriterFactory(std::string format);

    Status init() override;

    StatusOr<WriterAndStream> create(const std::string& path) const override;

private:
    std::string _format;
};

} // namespace starrocks::formats
