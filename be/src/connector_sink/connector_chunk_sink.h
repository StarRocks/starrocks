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

#include <fmt/format.h>

#include <boost/thread/future.hpp>
#include <future>

#include "column/chunk.h"
#include "common/status.h"
#include "formats/file_writer.h"
#include "fs/fs.h"
#include "runtime/runtime_state.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks::connector {

// Location provider provides file location for every output file. The name format depends on if the write is partitioned or not.
class LocationProvider {
public:
    // file_name_prefix = {query_id}_{be_number}_{driver_id}
    LocationProvider(const std::string& base_path, const std::string& query_id, int be_number, int driver_id,
                     const std::string& file_suffix)
            : _base_path(base_path),
              _file_name_prefix(fmt::format("{}_{}_{}", query_id, be_number, driver_id)),
              _file_name_suffix(file_suffix) {}

    // location = base_path/partition/{query_id}_{be_number}_{driver_id}_index.file_suffix
    std::string get(const std::string& partition) {
        return fmt::format("{}/{}/{}_{}.{}", _base_path, partition, _file_name_prefix, _partition2index[partition]++,
                           _file_name_suffix);
    }

    // location = base_path/{query_id}_{be_number}_{driver_id}_index.file_suffix
    std::string get() { return fmt::format("{}/{}_{}.{}", _base_path, _file_name_prefix, _index++, _file_name_suffix); }

private:
    const std::string _base_path;
    const std::string _file_name_prefix;
    const std::string _file_name_suffix;
    int _index = 0;
    std::map<std::string, int> _partition2index;
};

class ConnectorChunkSink {
public:
    struct Futures {
        std::vector<std::future<Status>> add_chunk_future;
        std::vector<std::future<formats::FileWriter::CommitResult>> commit_file_future;
        // std::vector<std::unique_ptr<FileWriter>> file_writers;
    };

    virtual ~ConnectorChunkSink() = default;
    virtual StatusOr<Futures> add(ChunkPtr chunk) = 0;
    virtual Futures finish() = 0;
    // virtual std::function<void(formats::FileWriter::CommitResult)> callback_on_success() = 0;
};

class FileChunkSink : public ConnectorChunkSink {
public:
    FileChunkSink(const std::vector<std::string>& partition_columns, const std::vector<ExprContext*>& partition_exprs,
                  std::unique_ptr<LocationProvider> location_provider,
                  std::unique_ptr<formats::FileWriterFactory> file_writer_factory, int64_t max_file_size);

    ~FileChunkSink() override = default;

    StatusOr<Futures> add(ChunkPtr chunk) override;
    Futures finish() override;
    // std::function<void(formats::FileWriter::CommitResult)> callback_on_success() override;

private:
    std::vector<ExprContext*> _partition_exprs;
    std::vector<std::string> _partition_column_names;

    std::unique_ptr<LocationProvider> _location_provider;
    std::unique_ptr<formats::FileWriterFactory> _file_writer_factory;
    std::map<std::string, std::shared_ptr<formats::FileWriter>> _partition_writers;
    int64_t _max_file_size;

    inline static std::string DEFAULT_PARTITION = "__DEFAULT_PARTITION__";
};

class HiveUtils {
public:
    static StatusOr<std::string> make_partition_name(const std::vector<std::string>& column_names,
                                                     const std::vector<ExprContext*>& exprs, ChunkPtr chunk);

private:
    static StatusOr<std::string> column_value(const TypeDescriptor& type_desc, const ColumnPtr& column);
};

} // namespace starrocks::connector
