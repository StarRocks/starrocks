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
#include "connector/iceberg_chunk_sink.h"
#include "connector/sink_memory_manager.h"
#include "formats/file_writer.h"
#include "runtime/runtime_state.h"

namespace starrocks::connector {

ConnectorChunkSink::ConnectorChunkSink(std::vector<std::string> partition_columns,
                                       std::vector<std::unique_ptr<ColumnEvaluator>>&& partition_column_evaluators,
                                       std::unique_ptr<LocationProvider> location_provider,
                                       std::unique_ptr<formats::FileWriterFactory> file_writer_factory,
                                       int64_t max_file_size, RuntimeState* state, bool support_null_partition)
        : _partition_column_names(std::move(partition_columns)),
          _partition_column_evaluators(std::move(partition_column_evaluators)),
          _location_provider(std::move(location_provider)),
          _file_writer_factory(std::move(file_writer_factory)),
          _max_file_size(max_file_size),
          _state(state),
          _support_null_partition(support_null_partition) {}

Status ConnectorChunkSink::init() {
    RETURN_IF_ERROR(ColumnEvaluator::init(_partition_column_evaluators));
    RETURN_IF_ERROR(_file_writer_factory->init());
    _op_mem_mgr->init(&_writer_stream_pairs, _io_poller,
                      [this](const CommitResult& r) { this->callback_on_commit(r); });
    return Status::OK();
}

Status ConnectorChunkSink::add(Chunk* chunk) {
    std::string partition = DEFAULT_PARTITION;
    bool partitioned = !_partition_column_names.empty();
    std::vector<bool> partition_field_null_list;
    if (partitioned) {
        if (typeid(*this) == typeid(IcebergChunkSink)) {
            ASSIGN_OR_RETURN(partition, HiveUtils::iceberg_make_partition_name(
                                                _partition_column_names, _partition_column_evaluators,
                                                dynamic_cast<IcebergChunkSink*>(this)->transform_expr(), chunk,
                                                _support_null_partition, partition_field_null_list));
        } else {
            ASSIGN_OR_RETURN(partition,
                             HiveUtils::make_partition_name(_partition_column_names, _partition_column_evaluators,
                                                            chunk, _support_null_partition));
        }
    }

    std::string null_fingerprint = "";
    for (bool b : partition_field_null_list) {
        null_fingerprint += (b ? '1' : '0');
    }

// null_fingerprint is used to distinguish with the secenario like NULL and string "null"
// They are under the same dir path, but should not in the same data file.
// We should record them in different files so that each data file could has its own meta info.
// otherwise, the scanFileTask may filter data incorrectly.
#define PartitionKey(partition, finger) partition + "&" + finger
    auto it = _writer_stream_pairs.find(PartitionKey(partition, null_fingerprint));
    if (it != _writer_stream_pairs.end()) {
        Writer* writer = it->second.first.get();
        if (writer->get_written_bytes() >= _max_file_size) {
            callback_on_commit(writer->commit().set_extra_data(null_fingerprint));
            _writer_stream_pairs.erase(it);
            auto path = partitioned ? _location_provider->get(partition) : _location_provider->get();
            ASSIGN_OR_RETURN(auto new_writer_and_stream, _file_writer_factory->create(path));
            std::unique_ptr<Writer> new_writer = std::move(new_writer_and_stream.writer);
            std::unique_ptr<Stream> new_stream = std::move(new_writer_and_stream.stream);
            RETURN_IF_ERROR(new_writer->init());
            RETURN_IF_ERROR(new_writer->write(chunk));
            _writer_stream_pairs[PartitionKey(partition, null_fingerprint)] =
                    std::make_pair(std::move(new_writer), new_stream.get());
            _io_poller->enqueue(std::move(new_stream));
        } else {
            RETURN_IF_ERROR(writer->write(chunk));
        }
    } else {
        auto path = partitioned ? _location_provider->get(partition) : _location_provider->get();
        ASSIGN_OR_RETURN(auto new_writer_and_stream, _file_writer_factory->create(path));
        std::unique_ptr<Writer> new_writer = std::move(new_writer_and_stream.writer);
        std::unique_ptr<Stream> new_stream = std::move(new_writer_and_stream.stream);
        RETURN_IF_ERROR(new_writer->init());
        RETURN_IF_ERROR(new_writer->write(chunk));
        _writer_stream_pairs[PartitionKey(partition, null_fingerprint)] =
                std::make_pair(std::move(new_writer), new_stream.get());
        _io_poller->enqueue(std::move(new_stream));
    }
#undef PartitionKey
    return Status::OK();
}

Status ConnectorChunkSink::finish() {
    for (auto& [partition_key, writer_and_stream] : _writer_stream_pairs) {
        string extra_data = partition_key.substr(partition_key.rfind('&') + 1);
        callback_on_commit(writer_and_stream.first->commit().set_extra_data(extra_data));
    }
    return Status::OK();
}

void ConnectorChunkSink::rollback() {
    for (auto& action : _rollback_actions) {
        action();
    }
}

} // namespace starrocks::connector
