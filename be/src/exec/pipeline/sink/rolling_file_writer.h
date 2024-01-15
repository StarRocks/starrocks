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
#include <common/status.h>
#include <column/chunk.h>
#include <runtime/runtime_state.h>

namespace starrocks::pipeline {

struct FileMetrics {
    std::string file_location;
    std::string partition_location;
    int64_t record_count;
    int64_t file_size;
    std::optional<std::vector<int64_t>> split_offsets;
    // field id to statitics
    std::optional<std::map<int32_t, int64_t>> column_sizes;
    std::optional<std::map<int32_t, int64_t>> value_counts;
    std::optional<std::map<int32_t, int64_t>> null_value_counts;
    std::optional<std::map<int32_t, std::string>> lower_bounds;
    std::optional<std::map<int32_t, std::string>> upper_bounds;
};

class FileWriter {
public:
    virtual ~FileWriter() = default;
    virtual int64_t getWrittenBytes() = 0;
    virtual std::future<Status> write(ChunkPtr chunk) = 0;
    virtual std::future<StatusOr<FileMetrics>> commit() = 0;
    virtual void commitAsync(std::function<void(StatusOr<FileMetrics>)> callback) = 0;
    virtual void rollback() = 0;
    virtual void close() = 0;
    virtual FileMetrics metrics() = 0;
};

// TODO(me): do we need this intermediate class?
class RollingFileWriter {
public:
    std::future<Status> write(ChunkPtr chunk) {
        if (_file_writer->getWrittenBytes() < 100) {
            _file_writer->commit();
        }
    }
private:
    std::unique_ptr<FileWriter> _file_writer;
};

} // namespace starrocks::pipeline

