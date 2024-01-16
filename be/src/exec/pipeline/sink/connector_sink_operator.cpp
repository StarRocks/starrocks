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

#include "connector_sink_operator.h"

#include "formats/parquet/file_writer.h"
#include "glog/logging.h"
#include "util/url_coding.h"

namespace starrocks::pipeline {

// TODO(letian-jiang): optimize
StatusOr<std::string> column_to_string(const TypeDescriptor& type_desc, const ColumnPtr& column) {
    auto datum = column->get(0);
    if (datum.is_null()) {
        return "null";
    }

    switch (type_desc.type) {
    case TYPE_BOOLEAN: {
        return datum.get_uint8() ? "true" : "false";
    }
    case TYPE_TINYINT: {
        return std::to_string(datum.get_int8());
    }
    case TYPE_SMALLINT: {
        return std::to_string(datum.get_int16());
    }
    case TYPE_INT: {
        return std::to_string(datum.get_int32());
    }
    case TYPE_BIGINT: {
        return std::to_string(datum.get_int64());
    }
    case TYPE_DATE: {
        return datum.get_date().to_string();
    }
    case TYPE_DATETIME: {
        return url_encode(datum.get_timestamp().to_string());
    }
    case TYPE_CHAR: {
        std::string origin_str = datum.get_slice().to_string();
        if (origin_str.length() < type_desc.len) {
            origin_str.append(type_desc.len - origin_str.length(), ' ');
        }
        return url_encode(origin_str);
    }
    case TYPE_VARCHAR: {
        return url_encode(datum.get_slice().to_string());
    }
    default: {
        return Status::InvalidArgument("unsupported partition column type" + type_desc.debug_string());
    }
    }
}

inline std::string get_partition_location(const string& path, const std::vector<std::string>& names,
                                          const std::vector<std::string>& values) {
    std::string partition_location = path;
    for (size_t i = 0; i < names.size(); i++) {
        partition_location += names[i] + "=" + values[i] + "/";
    }
    partition_location += "data_";
    return partition_location;
}

Status ConnectorSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    return Status::OK();
}

void ConnectorSinkOperator::close(RuntimeState* state) {
    for (const auto& writer : _partition_writers) {
        if (!writer.second->closed()) {
            WARN_IF_ERROR(writer.second->close(state), "close writer failed");
        }
    }
    Operator::close(state);
}

template<typename R>
bool is_ready(std::future<R> const& f) {
    return f.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
}

bool ConnectorSinkOperator::need_input() const {
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

bool ConnectorSinkOperator::is_finished() const {
    // LOG(INFO) << "is finished";
    if (!_is_finished) {
        return false;
    }
    // TODO:
    return _next_id == _rollback_actions.size();
}

Status ConnectorSinkOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
    LOG(INFO) << "set finishing";
    if (_file_writer != nullptr) {
        _file_writer->commitAsync([&, fragment_ctx = _fragment_ctx, state = state](FileWriter::CommitResult result) {
            if (!result.io_status.ok()) {
                LOG(WARNING) << "cancel fragment instance " << state->fragment_instance_id() << ": " << result.io_status;
                fragment_ctx->cancel(result.io_status);
                return;
            }
            state->update_num_rows_load_sink(result.file_metrics.record_count);
            _rollback_actions.push(result.rollback_action);
        });
    }
    return Status::OK();
}

bool ConnectorSinkOperator::pending_finish() const {
    return !is_finished();
}

Status ConnectorSinkOperator::set_cancelled(RuntimeState* state) {
    // TODO: cancel
    return Status::OK();
}

StatusOr<ChunkPtr> ConnectorSinkOperator::pull_chunk(RuntimeState* state) {
    CHECK(false) << "not allowed to pull chunk from ConnectorSinkOperator";
    __builtin_unreachable();
}

Status ConnectorSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    auto future_handler = [&](auto&& future) {
        if (is_ready(future)) {
            RETURN_IF_ERROR(future.get());
        } else {
            _blocking_futures.push(std::move(future));
        }
        return Status::OK();
    };

    auto future = _connector_chunk_sink->add(chunk);
    return future_handler(future);
}

ConnectorSinkOperatorFactory::ConnectorSinkOperatorFactory(
        const int32_t id, const string& path, const string& file_format, const TCompressionType::type& compression_type,
        const std::vector<ExprContext*>& output_exprs, const std::vector<ExprContext*>& partition_exprs,
        const std::vector<std::string>& column_names, const std::vector<std::string>& partition_column_names,
        bool write_single_file, const TCloudConfiguration& cloud_conf, FragmentContext* fragment_ctx)
        : OperatorFactory(id, "table_function_table_sink", Operator::s_pseudo_plan_node_id_for_final_sink),
          _path(path),
          _file_format(file_format),
          _compression_type(compression_type),
          _output_exprs(output_exprs),
          _partition_exprs(partition_exprs),
          _column_names(column_names),
          _partition_column_names(partition_column_names),
          _write_single_file(write_single_file),
          _cloud_conf(cloud_conf),
          _fragment_ctx(fragment_ctx) {}

Status ConnectorSinkOperatorFactory::prepare(RuntimeState* state) {
    LOG(INFO) << "prepare";
    RETURN_IF_ERROR(OperatorFactory::prepare(state));

    RETURN_IF_ERROR(Expr::prepare(_output_exprs, state));
    RETURN_IF_ERROR(Expr::open(_output_exprs, state));

    RETURN_IF_ERROR(Expr::prepare(_partition_exprs, state));
    RETURN_IF_ERROR(Expr::open(_partition_exprs, state));

    if (_file_format == "parquet") {
        auto result = parquet::ParquetBuildHelper::make_schema(
                _column_names, _output_exprs, std::vector<parquet::FileColumnId>(_output_exprs.size()));
        if (!result.ok()) {
            return Status::NotSupported(result.status().message());
        }
        _parquet_file_schema = result.ValueOrDie();
    } else {
        return Status::InternalError("unsupported file format" + _file_format);
    }

    return Status::OK();
}

OperatorPtr ConnectorSinkOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<ConnectorSinkOperator>(
            this, _id, _plan_node_id, driver_sequence, _path, _file_format, _compression_type, _output_exprs,
            _partition_exprs, _partition_column_names, _write_single_file, _cloud_conf, _fragment_ctx,
            _parquet_file_schema);
}

void ConnectorSinkOperatorFactory::close(RuntimeState* state) {
    Expr::close(_partition_exprs, state);
    Expr::close(_output_exprs, state);
    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
