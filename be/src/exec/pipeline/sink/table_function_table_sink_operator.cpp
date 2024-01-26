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

#include "table_function_table_sink_operator.h"

#include <boost/thread/future.hpp>

#include "formats/parquet/file_writer.h"
#include "glog/logging.h"
#include "util/url_coding.h"

namespace starrocks::pipeline {

Status TableFunctionTableSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    return Status::OK();
}

void TableFunctionTableSinkOperator::close(RuntimeState* state) {
    for (const auto& writer : _partition_writers) {
        if (!writer.second->closed()) {
            WARN_IF_ERROR(writer.second->close(state), "close writer failed");
        }
    }
    Operator::close(state);
}

template <typename R>
bool is_ready(std::future<R> const& f) {
    return f.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
}

bool TableFunctionTableSinkOperator::need_input() const {
    if (_is_finished) {
        return false;
    }

    // LOG(INFO) << "need input";
    while (!_blocking_futures.empty()) {
        // return if any future is not ready, check in order of FIFO
        if (!is_ready(_blocking_futures.front())) {
            return false;
        }
        if (auto st = _blocking_futures.front().get(); !st.ok()) {
            LOG(WARNING) << "cancel fragment: " << st;
            _fragment_ctx->cancel(st);
            return false;
        }
        _blocking_futures.pop();
    }

    return true;

    /*
    for (const auto& writer : _partition_writers) {
        if (!writer.second->writable()) {
            return false;
        }
    }

    return true;
    */
}

bool TableFunctionTableSinkOperator::is_finished() const {
    // LOG(INFO) << "is finished";
    if (!_is_finished) {
        return false;
    }
    return _next_id == _rollback_actions.size();
}

Status TableFunctionTableSinkOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
    LOG(INFO) << "set finishing";
    if (_file_writer != nullptr) {
        _file_writer->commitAsync([&, fragment_ctx = _fragment_ctx, state = state](FileWriter::CommitResult result) {
            if (!result.io_status.ok()) {
                LOG(WARNING) << "cancel fragment instance " << state->fragment_instance_id() << ": "
                             << result.io_status;
                fragment_ctx->cancel(result.io_status);
                return;
            }
            state->update_num_rows_load_sink(result.file_metrics.record_count);
            _rollback_actions.push(result.rollback_action);
        });
    }
    return Status::OK();
}

bool TableFunctionTableSinkOperator::pending_finish() const {
    return !is_finished();
}

Status TableFunctionTableSinkOperator::set_cancelled(RuntimeState* state) {
    LOG(INFO) << "set cancelled";
    return Status::OK();
}

StatusOr<ChunkPtr> TableFunctionTableSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from table function table sink operator");
}

Status TableFunctionTableSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    auto future_handler = [&](auto&& future) {
        if (is_ready(future)) {
            RETURN_IF_ERROR(future.get());
        } else {
            _blocking_futures.push(std::move(future));
        }
        return Status::OK();
    };

    // TODO: file writer factory
    if (_file_writer == nullptr) {
        string location = _path + "_" +
                          fmt::format("{}_{}_{}_{}.parquet", print_id(state->query_id()), state->be_number(),
                                      _driver_sequence, _next_id++);
        ASSIGN_OR_RETURN(auto fs, FileSystem::CreateUniqueFromString(_path, FSOptions(&_cloud_conf)));
        WritableFileOptions options{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        ASSIGN_OR_RETURN(auto writable_file, fs->new_writable_file(options, location));
        _file_writer = std::make_unique<parquet::ParquetFileWriter>(std::move(writable_file),
                                                                    ::parquet::default_writer_properties(),
                                                                    _parquet_file_schema, _output_exprs, 1 << 30);
        RETURN_IF_ERROR(_file_writer->init());
    }

    // poc: unpartitoned writer
    // commit writer if exceeds target file size
    if (_file_writer->get_written_bytes() > 1 << 30) {
        _file_writer->commitAsync([&, fragment_ctx = _fragment_ctx, state = state](FileWriter::CommitResult result) {
            if (!result.io_status.ok()) {
                LOG(WARNING) << "cancel fragment instance " << state->fragment_instance_id() << ": "
                             << result.io_status;
                fragment_ctx->cancel(result.io_status);
                return;
            }
            state->update_num_rows_load_sink(result.file_metrics.record_count);
            _rollback_actions.push(result.rollback_action);
        });

        string location = _path + "_" +
                          fmt::format("{}_{}_{}_{}.parquet", print_id(state->query_id()), state->be_number(),
                                      _driver_sequence, _next_id++);
        ASSIGN_OR_RETURN(auto fs, FileSystem::CreateUniqueFromString(location, FSOptions(&_cloud_conf)));
        WritableFileOptions options{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        ASSIGN_OR_RETURN(auto writable_file, fs->new_writable_file(options, location));
        _file_writer = std::make_unique<parquet::ParquetFileWriter>(std::move(writable_file),
                                                                    ::parquet::default_writer_properties(),
                                                                    _parquet_file_schema, _output_exprs, 1 << 30);
        RETURN_IF_ERROR(_file_writer->init());
    }

    RETURN_IF_ERROR(future_handler(_file_writer->write(chunk)));
    return Status::OK();
}

TableInfo TableFunctionTableSinkOperator::_make_table_info(const string& partition_location) const {
    TableInfo tableInfo;
    tableInfo.partition_location = partition_location;
    tableInfo.schema = _parquet_file_schema;
    tableInfo.compress_type = _compression_type;
    tableInfo.cloud_conf = _cloud_conf;
    if (_write_single_file) {
        tableInfo.max_file_size = -1;
    }
    return tableInfo;
}

TableFunctionTableSinkOperatorFactory::TableFunctionTableSinkOperatorFactory(
        const int32_t id, std::shared_ptr<connector::FileChunkSinkContext> context, FragmentContext* fragment_ctx)
        : OperatorFactory(id, "table_function_table_sink", Operator::s_pseudo_plan_node_id_for_final_sink),
          _fragment_context(fragment_ctx) {
    auto connector = connector::ConnectorManager::default_instance()->get(connector::Connector::FILE);
    auto sink_provider = connector->create_data_sink_provider();
    _inner = std::make_unique<ConnectorSinkOperatorFactory>(std::move(sink_provider), context, fragment_ctx);
}

Status TableFunctionTableSinkOperatorFactory::prepare(RuntimeState* state) {
    LOG(INFO) << "prepare";
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    return Status::OK();
}

OperatorPtr TableFunctionTableSinkOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return _inner->create(degree_of_parallelism, driver_sequence);
}

void TableFunctionTableSinkOperatorFactory::close(RuntimeState* state) {
    Expr::close(_partition_exprs, state);
    Expr::close(_output_exprs, state);
    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
