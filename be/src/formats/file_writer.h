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
#include "column_evaluator.h"
#include "common/status.h"
#include "fs/fs.h"
#include "runtime/runtime_state.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks::formats {

struct FileWriterOptions {
    virtual ~FileWriterOptions() = default;
};

class FileWriter {
public:
    struct FileMetrics {
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
        FileMetrics file_metrics;
        std::string location;
        std::function<void()> rollback_action;
    };

    virtual ~FileWriter() = default;
    virtual Status init() = 0;
    virtual int64_t get_written_bytes() = 0;
    virtual std::future<Status> write(ChunkPtr chunk) = 0;
    virtual std::future<CommitResult> commit() = 0;
};

class FileWriterFactory {
public:
    virtual ~FileWriterFactory() = default;

    virtual StatusOr<std::shared_ptr<FileWriter>> create(const std::string& path) = 0;
};

} // namespace starrocks::formats
