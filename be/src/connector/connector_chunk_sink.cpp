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

#include "connector_chunk_sink.h"

#include "column/chunk.h"
#include "common/status.h"
#include "connector/sink_memory_manager.h"
#include "formats/file_writer.h"
#include "runtime/runtime_state.h"

namespace starrocks::connector {

ConnectorChunkSink::ConnectorChunkSink(std::vector<std::string> partition_columns,
                                       std::vector<std::unique_ptr<ColumnEvaluator>>&& partition_column_evaluators,
                                       std::unique_ptr<PartitionChunkWriterFactory> partition_chunk_writer_factory,
                                       RuntimeState* state, bool support_null_partition)
        : _partition_column_names(std::move(partition_columns)),
          _partition_column_evaluators(std::move(partition_column_evaluators)),
          _partition_chunk_writer_factory(std::move(partition_chunk_writer_factory)),
          _state(state),
          _support_null_partition(support_null_partition) {}

Status ConnectorChunkSink::init() {
    RETURN_IF_ERROR(ColumnEvaluator::init(_partition_column_evaluators));
    RETURN_IF_ERROR(_partition_chunk_writer_factory->init());
    _op_mem_mgr->init(&_partition_chunk_writers, _io_poller,
                      [this](const CommitResult& r) { this->callback_on_commit(r); });
    return Status::OK();
}

Status ConnectorChunkSink::write_partition_chunk(const std::string& partition,
                                                 const std::vector<int8_t>& partition_field_null_list, Chunk* chunk) {
    // partition_field_null_list is used to distinguish with the secenario like NULL and string "null"
    // They are under the same dir path, but should not in the same data file.
    // We should record them in different files so that each data file could has its own meta info.
    // otherwise, the scanFileTask may filter data incorrectly.
    PartitionKey partition_key = std::make_pair(partition, partition_field_null_list);
    auto it = _partition_chunk_writers.find(partition_key);
    if (it != _partition_chunk_writers.end()) {
        return it->second->write(chunk);
    } else {
        auto writer = _partition_chunk_writer_factory->create(partition, partition_field_null_list);
        auto commit_callback = [this](const CommitResult& r) { this->callback_on_commit(r); };
        auto error_handler = [this](const Status& s) { this->set_status(s); };
        writer->set_commit_callback(commit_callback);
        writer->set_error_handler(error_handler);
        writer->set_io_poller(_io_poller);
        RETURN_IF_ERROR(writer->init());
        RETURN_IF_ERROR(writer->write(chunk));
        _partition_chunk_writers[partition_key] = writer;
    }
    return Status::OK();
}

Status ConnectorChunkSink::add(Chunk* chunk) {
    std::string partition = DEFAULT_PARTITION;
    bool partitioned = !_partition_column_names.empty();
    if (partitioned) {
        ASSIGN_OR_RETURN(partition,
                         HiveUtils::make_partition_name(_partition_column_names, _partition_column_evaluators, chunk,
                                                        _support_null_partition));
    }

    RETURN_IF_ERROR(
            write_partition_chunk(partition, std::vector<int8_t>(_partition_column_evaluators.size(), 0), chunk));
    return Status::OK();
}

Status ConnectorChunkSink::finish() {
    for (auto& [partition_key, writer] : _partition_chunk_writers) {
        RETURN_IF_ERROR(writer->finish());
    }
    return Status::OK();
}

void ConnectorChunkSink::push_rollback_action(const std::function<void()>& action) {
    // Not a very frequent operation, so use unique_lock here is ok.
    std::unique_lock<std::shared_mutex> wlck(_mutex);
    _rollback_actions.push_back(std::move(action));
}

void ConnectorChunkSink::rollback() {
    std::shared_lock<std::shared_mutex> rlck(_mutex);
    for (auto& action : _rollback_actions) {
        action();
    }
}

void ConnectorChunkSink::set_status(const Status& status) {
    std::unique_lock<std::shared_mutex> wlck(_mutex);
    _status = status;
}

Status ConnectorChunkSink::status() {
    std::shared_lock<std::shared_mutex> rlck(_mutex);
    return _status;
}

bool ConnectorChunkSink::is_finished() {
    for (auto& [partition_key, writer] : _partition_chunk_writers) {
        if (!writer->is_finished()) {
            return false;
        }
    }
    return true;
}

} // namespace starrocks::connector
