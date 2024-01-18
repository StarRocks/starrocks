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
#include <boost/thread/future.hpp>

#include <fmt/format.h>
#include <util/priority_thread_pool.hpp>

#include "rolling_file_writer.h"
#include "fs/fs.h"

namespace starrocks::pipeline {

// TODO: comments
class LocationProvider {
public:
    // TODO: handle suffix
    LocationProvider(std::string base_path, std::string query_id, int be_number, int driver_id, std::string file_suffix) : _base_path(base_path) {
        _file_name_prefix = fmt::format("{}_{}_{}_{}", query_id, be_number, driver_id);
        _file_name_suffix = file_suffix;
    }

    std::string get(std::string partition) {
        return fmt::format("{}/{}/{}_{}.{}", _base_path, partition, _file_name_prefix, _partition2index[partition]++, file_suffix);
    }

    std::string get() {
        return fmt::format("{}/{}/{}_{}.{}", _base_path, partition, _file_name_prefix, _partition2index[partition]++, file_suffix);
    }

private:
    std::string _base_path;
    std::string _file_name_prefix;
    std::string _file_name_suffix;
    std::map<std::string, int> _partition2index;
};

class FileWriterFactory {
public:
    // TODO: how to handle file options of different formats
    FileWriterFactory(std::unique_ptr<FileSystem> fs, FileWriter::FileFormat format, std::shared_ptr<FileWriter::FileWriterOptions> options, const std::vector<std::string>& column_names,
                  const std::vector<ExprContext*>& output_exprs, PriorityThreadPool* executors = nullptr);

    StatusOr<std::unique_ptr<FileWriter>> create(std::string path) const;

private:
    std::shared_ptr<FileWriter::FileWriterOptions> _options;
    std::unique_ptr<FileSystem> _fs;
    FileWriter::FileFormat _format;
    std::vector<std::string> _column_names;
    std::vector<ExprContext*> _output_exprs;
    PriorityThreadPool* _executors;
};


class ConnectorChunkSink {
public:
    struct Futures {
        std::vector<std::future<Status>> add_chunk_future;
        std::vector<std::future<FileWriter::CommitResult>> commit_file_future;
        std::vector<std::unique_ptr<FileWriter>> file_writers;
    };

    virtual ~ConnectorChunkSink() = 0;
    virtual StatusOr<Futures> add(ChunkPtr chunk) = 0;
    virtual Futures finish() = 0;
    virtual std::function<void(FileWriter::CommitResult)> callbackOnCommitSuccess() = 0; // TODO: rename
};

class FilesChunkSink : public ConnectorChunkSink {
public:
    FilesChunkSink(const std::vector<std::string>& partition_columns,
        const std::vector<ExprContext*>& partition_exprs, std::unique_ptr<LocationProvider> location_provider, std::unique_ptr<FileWriterFactory> file_writer_factory, int64_t max_file_size);

    StatusOr<Futures> add(ChunkPtr chunk) override;
    Futures finish() override;

private:
    std::vector<ExprContext*> _partition_exprs;
    std::vector<std::string> _partition_column_names;

    std::unique_ptr<LocationProvider> _location_provider;
    std::unique_ptr<FileWriterFactory> _file_writer_factory;
    std::map<std::string, std::unique_ptr<FileWriter>> _partition_writers;
    int64_t _max_file_size;

    inline static std::string DEFAULT_PARTITION = "__DEFAULT_PARTITION__";
};

class HiveUtils {
public:
    static StatusOr<std::string> make_partition_name(const std::vector<std::string>& column_names, const std::vector<ExprContext*>& exprs, ChunkPtr chunk);

private:
    static StatusOr<std::string> column_value(const TypeDescriptor& type_desc, const ColumnPtr& column);
};

}



