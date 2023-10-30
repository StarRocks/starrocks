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

#include "hive_table_sink_operator.h"

#include <utility>

#include "exec/parquet_builder.h"
#include "table_function_table_sink_operator.h"
#include "util/path_util.h"

namespace starrocks::pipeline {

static const std::string HIVE_UNPARTITIONED_TABLE_LOCATION = "hive_unpartitioned_table_fake_location";

Status HiveTableSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    return Status::OK();
}

void HiveTableSinkOperator::close(RuntimeState* state) {
    for (const auto& writer : _partition_writers) {
        if (!writer.second->closed()) {
            WARN_IF_ERROR(writer.second->close(state), "close writer failed");
        }
    }
    Operator::close(state);
}

bool HiveTableSinkOperator::need_input() const {
    for (const auto& writer : _partition_writers) {
        if (!writer.second->writable()) {
            return false;
        }
    }

    return true;
}

bool HiveTableSinkOperator::is_finished() const {
    if (_partition_writers.size() == 0) {
        return _is_finished.load();
    }
    for (const auto& writer : _partition_writers) {
        if (!writer.second->closed()) {
            return false;
        }
    }

    return true;
}

Status HiveTableSinkOperator::set_finishing(RuntimeState* state) {
    for (const auto& writer : _partition_writers) {
        if (!writer.second->closed()) {
            WARN_IF_ERROR(writer.second->close(state), "close writer failed");
        }
    }

    if (_partition_writers.size() == 0) {
        _is_finished = true;
    }
    return Status::OK();
}

bool HiveTableSinkOperator::pending_finish() const {
    return !is_finished();
}

Status HiveTableSinkOperator::set_cancelled(RuntimeState* state) {
    return Status::OK();
}

StatusOr<ChunkPtr> HiveTableSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from hive table sink operator");
}

Status HiveTableSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    TableInfo tableInfo;
    tableInfo.schema = _parquet_file_schema;
    tableInfo.compress_type = _compression_codec;
    tableInfo.cloud_conf = _cloud_conf;

    if (_partition_column_names.size() == 0) {
        if (_partition_writers.empty()) {
            tableInfo.partition_location = _location;
            auto writer = std::make_unique<RollingAsyncParquetWriter>(tableInfo, _output_expr, _common_metrics.get(),
                                                                      add_hive_commit_info, state, _driver_sequence);
            RETURN_IF_ERROR(writer->init());
            _partition_writers.insert({HIVE_UNPARTITIONED_TABLE_LOCATION, std::move(writer)});
        }

        RETURN_IF_ERROR(_partition_writers[HIVE_UNPARTITIONED_TABLE_LOCATION]->append_chunk(chunk.get(), state));
    } else if (_is_static_partition_insert && !_partition_writers.empty()) {
        RETURN_IF_ERROR(_partition_writers.begin()->second->append_chunk(chunk.get(), state));
    } else {
        std::vector<std::string> partition_column_values(_partition_expr.size());
        for (int i = 0; i < _partition_expr.size(); ++i) {
            ASSIGN_OR_RETURN(ColumnPtr column, _partition_expr[i]->evaluate(chunk.get()));
            DCHECK(column != nullptr);
            if (column->has_null()) {
                return Status::NotSupported("Partition value can't be null.");
            }
            ASSIGN_OR_RETURN(partition_column_values[i], column_to_string(_partition_expr[i]->root()->type(), column))
        }

        DCHECK(_partition_column_names.size() == partition_column_values.size());

        string partition_location = _get_partition_location(partition_column_values);

        auto partition_writer = _partition_writers.find(partition_location);
        if (partition_writer == _partition_writers.end()) {
            tableInfo.partition_location = partition_location;
            std::vector<ExprContext*> data_col_exprs(_output_expr.begin(),
                                                     _output_expr.begin() + _data_column_names.size());
            auto writer = std::make_unique<RollingAsyncParquetWriter>(tableInfo, data_col_exprs, _common_metrics.get(),
                                                                      add_hive_commit_info, state, _driver_sequence);
            RETURN_IF_ERROR(writer->init());
            _partition_writers.insert({partition_location, std::move(writer)});
            RETURN_IF_ERROR(_partition_writers[partition_location]->append_chunk(chunk.get(), state));
        } else {
            RETURN_IF_ERROR(partition_writer->second->append_chunk(chunk.get(), state));
        }
    }
    return Status::OK();
}

std::string HiveTableSinkOperator::_get_partition_location(const std::vector<std::string>& values) {
    std::string partition_location = _location;
    for (size_t i = 0; i < _partition_column_names.size(); i++) {
        partition_location += _partition_column_names[i] + "=" + values[i] + "/";
    }
    return partition_location;
}

HiveTableSinkOperatorFactory::HiveTableSinkOperatorFactory(int32_t id, FragmentContext* fragment_ctx,
                                                           const THiveTableSink& thrift_sink,
                                                           vector<TExpr> t_output_expr,
                                                           std::vector<ExprContext*> partition_expr_ctxs)
        : OperatorFactory(id, "hive_table_sink", Operator::s_pseudo_plan_node_id_for_final_sink),
          _t_output_expr(std::move(t_output_expr)),
          _partition_expr_ctxs(std::move(partition_expr_ctxs)),
          _data_column_names(thrift_sink.data_column_names),
          _partition_column_names(thrift_sink.partition_column_names),
          _fragment_ctx(std::move(fragment_ctx)),
          _location(thrift_sink.staging_dir),
          _file_format(thrift_sink.file_format),
          _compression_codec(thrift_sink.compression_type),
          _cloud_conf(thrift_sink.cloud_configuration),
          is_static_partition_insert(thrift_sink.is_static_partition_sink) {
    DCHECK(thrift_sink.__isset.data_column_names);
    DCHECK(thrift_sink.__isset.partition_column_names);
    DCHECK(thrift_sink.__isset.staging_dir);
    DCHECK(thrift_sink.__isset.file_format);
    DCHECK(thrift_sink.__isset.compression_type);
    DCHECK(thrift_sink.__isset.cloud_configuration);
    DCHECK(thrift_sink.__isset.is_static_partition_sink);
}

Status HiveTableSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));

    RETURN_IF_ERROR(Expr::create_expr_trees(state->obj_pool(), _t_output_expr, &_output_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));

    RETURN_IF_ERROR(Expr::prepare(_partition_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_partition_expr_ctxs, state));

    if (_file_format == "parquet") {
        std::vector<ExprContext*> data_col_exprs(_output_expr_ctxs.begin(),
                                                 _output_expr_ctxs.begin() + _data_column_names.size());
        auto result = parquet::ParquetBuildHelper::make_schema(
                _data_column_names, data_col_exprs, std::vector<parquet::FileColumnId>(_data_column_names.size()));
        if (!result.ok()) {
            return Status::NotSupported(result.status().message());
        }
        _parquet_file_schema = result.ValueOrDie();
    }

    return Status::OK();
}

void HiveTableSinkOperatorFactory::close(RuntimeState* state) {
    Expr::close(_partition_expr_ctxs, state);
    Expr::close(_output_expr_ctxs, state);
    OperatorFactory::close(state);
}

void HiveTableSinkOperator::add_hive_commit_info(starrocks::parquet::AsyncFileWriter* writer, RuntimeState* state) {
    THiveFileInfo hive_file_info;
    hive_file_info.__set_file_name(path_util::base_name(writer->file_location()));
    hive_file_info.__set_partition_path(writer->partition_location());
    hive_file_info.__set_record_count(writer->metadata()->num_rows());
    hive_file_info.__set_file_size_in_bytes(writer->file_size());

    TSinkCommitInfo commit_info;
    commit_info.__set_hive_file_info(hive_file_info);

    // update runtime state
    state->add_sink_commit_info(commit_info);
    state->update_num_rows_load_sink(hive_file_info.record_count);
}

} // namespace starrocks::pipeline
