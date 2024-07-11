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
#include "connector/connector.h"
#include "connector_chunk_sink.h"
#include "formats/column_evaluator.h"
#include "formats/file_writer.h"
#include "fs/fs.h"
#include "runtime/runtime_state.h"
#include "util/priority_thread_pool.hpp"
#include "utils.h"

namespace starrocks::connector {

class FileChunkSink : public ConnectorChunkSink {
public:
    FileChunkSink(std::vector<std::string> partition_columns,
                  std::vector<std::unique_ptr<ColumnEvaluator>>&& partition_column_evaluators,
                  std::unique_ptr<LocationProvider> location_provider,
                  std::unique_ptr<formats::FileWriterFactory> file_writer_factory, int64_t max_file_size,
                  RuntimeState* state);

    ~FileChunkSink() override = default;

    void callback_on_commit(const formats::FileWriter::CommitResult& result) override;
};

struct FileChunkSinkContext : public ConnectorChunkSinkContext {
    ~FileChunkSinkContext() override = default;

    std::string path;
    std::vector<std::string> column_names;
    std::vector<std::unique_ptr<ColumnEvaluator>> column_evaluators;
    std::vector<int32_t> partition_column_indices;
    int64_t max_file_size = 128L * 1024 * 1024;
    std::string format;
    TCompressionType::type compression_type = TCompressionType::UNKNOWN_COMPRESSION;
    std::map<std::string, std::string> options;
    PriorityThreadPool* executor = nullptr;
    TCloudConfiguration cloud_conf;
    pipeline::FragmentContext* fragment_context = nullptr;
};

class FileChunkSinkProvider : public ConnectorChunkSinkProvider {
public:
    ~FileChunkSinkProvider() override = default;

    StatusOr<std::unique_ptr<ConnectorChunkSink>> create_chunk_sink(std::shared_ptr<ConnectorChunkSinkContext> context,
                                                                    int32_t driver_id) override;
};

} // namespace starrocks::connector
