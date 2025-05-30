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

#include <future>

#include "column/chunk.h"
#include "common/status.h"
#include "formats/column_evaluator.h"
#include "fs/fs.h"
#include "io/async_flush_output_stream.h"
#include "runtime/runtime_state.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks::formats {

struct FileWriterOptions {
    virtual ~FileWriterOptions() = default;
};

class FileWriter {
public:
    struct FileStatistics {
        int64_t record_count;
        int64_t file_size;
        std::optional<std::vector<int64_t>> split_offsets;
        std::optional<std::map<int32_t, int64_t>> column_sizes;
        std::optional<std::map<int32_t, int64_t>> value_counts;
        std::optional<std::map<int32_t, int64_t>> null_value_counts;
        std::optional<std::map<int32_t, std::string>> lower_bounds;
        std::optional<std::map<int32_t, std::string>> upper_bounds;
    };

    struct CommitResult {
        Status io_status;
        std::string format;
        FileStatistics file_statistics;
        std::string location;
        std::function<void()> rollback_action;
        std::string extra_data;
        CommitResult& set_extra_data(std::string extra_data) {
            this->extra_data = std::move(extra_data);
            return *this;
        }
    };

    virtual ~FileWriter() = default;
    virtual Status init() = 0;
    virtual int64_t get_written_bytes() = 0;
    virtual int64_t get_allocated_bytes() = 0;
    virtual Status write(Chunk* chunk) = 0;
    virtual CommitResult commit() = 0;
};

struct WriterAndStream {
    std::unique_ptr<FileWriter> writer;
    std::unique_ptr<io::AsyncFlushOutputStream> stream;
};

class FileWriterFactory {
public:
    virtual ~FileWriterFactory() = default;

    virtual Status init() = 0;

    virtual StatusOr<WriterAndStream> create(const std::string& path) const = 0;
};

class UnknownFileWriterFactory : public FileWriterFactory {
public:
    UnknownFileWriterFactory(std::string format) : _format(std::move(format)) {}

    Status init() override { return Status::NotSupported(fmt::format("got unsupported file format: {}", _format)); }

    StatusOr<WriterAndStream> create(const std::string& path) const override {
        return Status::NotSupported(fmt::format("got unsupported file format: {}", _format));
    }

private:
    std::string _format;
};

} // namespace starrocks::formats
