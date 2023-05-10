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

#include "iceberg_table_sink_operator.h"

#include <utility>

#include "exec/parquet_builder.h"

namespace starrocks::pipeline {

[[maybe_unused]] static void add_iceberg_commit_info(starrocks::parquet::AsyncFileWriter* writer, RuntimeState* state);
static const std::string ICEBERG_UNPARTITIONED_TABLE_LOCATION = "iceberg_unpartitioned_table_fake_location";

Status IcebergTableSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    return Status::OK();
}

void IcebergTableSinkOperator::close(RuntimeState* state) {
    for (const auto& writer : _partition_writers) {
        if (!writer.second->closed()) {
            writer.second->close(state);
        }
    }
    Operator::close(state);
}

bool IcebergTableSinkOperator::need_input() const {
    for (const auto& writer : _partition_writers) {
        if (!writer.second->writable()) {
            return false;
        }
    }

    return true;
}

bool IcebergTableSinkOperator::is_finished() const {
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

Status IcebergTableSinkOperator::set_finishing(RuntimeState* state) {
    for (const auto& writer : _partition_writers) {
        if (!writer.second->closed()) {
            writer.second->close(state);
        }
    }

    if (_partition_writers.size() == 0) {
        _is_finished = true;
    }
    return Status::OK();
}

bool IcebergTableSinkOperator::pending_finish() const {
    return !is_finished();
}

Status IcebergTableSinkOperator::set_cancelled(RuntimeState* state) {
    return Status::OK();
}

StatusOr<ChunkPtr> IcebergTableSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from iceberg table sink operator");
}

Status IcebergTableSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    TableInfo tableInfo;
    tableInfo.schema = _parquet_file_schema;
    tableInfo.compress_type = _compression_codec;
    tableInfo.cloud_conf = _cloud_conf;

    if (_iceberg_table->is_unpartitioned_table()) {
        if (_partition_writers.empty()) {
            tableInfo.partition_location = _iceberg_table_data_location;
            auto writer = std::make_unique<RollingAsyncParquetWriter>(tableInfo, _output_expr, _common_metrics.get(),
                                                                      add_iceberg_commit_info, state, _driver_sequence);
            _partition_writers.insert({ICEBERG_UNPARTITIONED_TABLE_LOCATION, std::move(writer)});
        }

        _partition_writers[ICEBERG_UNPARTITIONED_TABLE_LOCATION]->append_chunk(chunk.get(), state);
        return Status::OK();
    } else {
        Columns partitions_columns;
        partitions_columns.resize(_partition_expr.size());
        for (size_t i = 0; i < partitions_columns.size(); ++i) {
            ASSIGN_OR_RETURN(partitions_columns[i], _partition_expr[i]->evaluate(chunk.get()));
            DCHECK(partitions_columns[i] != nullptr);
        }

        std::vector<std::string> partition_column_names = _iceberg_table->partition_column_names();
        std::vector<std::string> partition_column_values;
        for (const ColumnPtr& column : partitions_columns) {
            partition_column_values.emplace_back(_value_to_string(column, 0));
        }

        DCHECK(partition_column_names.size() == partition_column_values.size());

        string partition_location = _get_partition_location(partition_column_names, partition_column_values);

        auto partition_writer = _partition_writers.find(partition_location);
        if (partition_writer == _partition_writers.end()) {
            tableInfo.partition_location = partition_location;
            auto writer = new RollingAsyncParquetWriter(tableInfo, _output_expr, _common_metrics.get(),
                                                        add_iceberg_commit_info, state, _driver_sequence);

            _partition_writers.emplace(partition_location, writer);
            writer->append_chunk(chunk.get(), state);
        } else {
            partition_writer->second->append_chunk(chunk.get(), state);
        }

        return Status::OK();
    }
}

std::string IcebergTableSinkOperator::_get_partition_location(const std::vector<std::string>& names,
                                                              const std::vector<std::string>& values) {
    std::string partition_location = _iceberg_table_data_location;
    for (size_t i = 0; i < names.size(); i++) {
        partition_location += names[i] + "=" + values[i] + "/";
    }
    return partition_location;
}

std::string IcebergTableSinkOperator::_value_to_string(const ColumnPtr& column, size_t index) {
    auto v = column->get(index);
    std::string res;
    v.visit([&](auto& variant) {
        std::visit(
                [&](auto&& arg) {
                    using T = std::decay_t<decltype(arg)>;
                    if constexpr (std::is_same_v<T, Slice> || std::is_same_v<T, TimestampValue> ||
                                  std::is_same_v<T, DateValue> || std::is_same_v<T, decimal12_t> ||
                                  std::is_same_v<T, DecimalV2Value>) {
                        res = arg.to_string();
                    } else if constexpr (std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> ||
                                         std::is_same_v<T, float> || std::is_same_v<T, double>) {
                        res = std::to_string(arg);
                    }
                },
                variant);
    });
    return res;
}

IcebergTableSinkOperatorFactory::IcebergTableSinkOperatorFactory(int32_t id, FragmentContext* fragment_ctx,
                                                                 vector<TExpr> t_output_expr,
                                                                 IcebergTableDescriptor* iceberg_table,
                                                                 const TIcebergTableSink& thrift_sink,
                                                                 std::vector<ExprContext*> partition_expr_ctxs)
        : OperatorFactory(id, "iceberg_table_sink", Operator::s_pseudo_plan_node_id_for_iceberg_table_sink),
          _t_output_expr(std::move(t_output_expr)),
          _fragment_ctx(std::move(fragment_ctx)),
          _iceberg_table(iceberg_table),
          _location(thrift_sink.location),
          _file_format(thrift_sink.file_format),
          _compression_codec(thrift_sink.compression_type),
          _cloud_conf(thrift_sink.cloud_configuration),
          _partition_expr_ctxs(std::move(partition_expr_ctxs)) {
    DCHECK(thrift_sink.__isset.location);
    DCHECK(thrift_sink.__isset.file_format);
    DCHECK(thrift_sink.__isset.compression_type);
    DCHECK(thrift_sink.__isset.cloud_configuration);
}

Status IcebergTableSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));

    RETURN_IF_ERROR(Expr::create_expr_trees(state->obj_pool(), _t_output_expr, &_output_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));

    RETURN_IF_ERROR(Expr::prepare(_partition_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_partition_expr_ctxs, state));

    const TIcebergSchema* t_iceberg_schema = _iceberg_table->get_iceberg_schema();
    if (_file_format == "parquet") {
        std::vector<parquet::FileColumnId> field_ids = generate_parquet_field_ids(t_iceberg_schema->fields);
        auto result = parquet::ParquetBuildHelper::make_schema(_iceberg_table->full_column_names(), _output_expr_ctxs,
                                                               field_ids);
        _parquet_file_schema = result.ValueOrDie();
    }

    return Status::OK();
}

void IcebergTableSinkOperatorFactory::close(RuntimeState* state) {
    Expr::close(_partition_expr_ctxs, state);
    Expr::close(_output_expr_ctxs, state);
    OperatorFactory::close(state);
}

std::vector<parquet::FileColumnId> IcebergTableSinkOperatorFactory::generate_parquet_field_ids(
        const std::vector<TIcebergSchemaField>& fields) {
    std::vector<parquet::FileColumnId> file_column_ids(fields.size());
    for (int i = 0; i < fields.size(); ++i) {
        file_column_ids[i].field_id = fields[i].field_id;
        file_column_ids[i].children = generate_parquet_field_ids(fields[i].children);
    }

    return file_column_ids;
}

[[maybe_unused]] static void add_iceberg_commit_info(starrocks::parquet::AsyncFileWriter* writer, RuntimeState* state) {
    TSinkCommitInfo commit_info;
    TIcebergDataFile iceberg_data_file;
    iceberg_data_file.__set_partition_path(writer->partition_location());
    iceberg_data_file.__set_path(writer->file_location());
    iceberg_data_file.__set_format("parquet");
    iceberg_data_file.__set_record_count(writer->metadata()->num_rows());
    iceberg_data_file.__set_file_size_in_bytes(writer->file_size());
    std::vector<int64_t> split_offsets;
    writer->split_offsets(split_offsets);
    iceberg_data_file.__set_split_offsets(split_offsets);

    std::unordered_map<int32_t, int64_t> column_sizes;
    std::unordered_map<int32_t, int64_t> value_counts;
    std::unordered_map<int32_t, int64_t> null_value_counts;
    std::unordered_map<int32_t, std::string> min_values;
    std::unordered_map<int32_t, std::string> max_values;

    const auto& metadata = writer->metadata();

    for (int i = 0; i < metadata->num_row_groups(); ++i) {
        auto row_group = metadata->RowGroup(i);
        for (int j = 0; j < row_group->num_columns(); j++) {
            auto column_meta = row_group->ColumnChunk(j);
            int32_t field_id = metadata->schema()->Column(j)->schema_node()->field_id();

            if (null_value_counts.find(field_id) == null_value_counts.end()) {
                null_value_counts.insert({field_id, column_meta->statistics()->null_count()});
            } else {
                null_value_counts[field_id] += column_meta->statistics()->null_count();
            }

            if (column_sizes.find(field_id) == column_sizes.end()) {
                column_sizes.insert({field_id, column_meta->total_compressed_size()});
            } else {
                column_sizes[field_id] += column_meta->total_compressed_size();
            }

            if (value_counts.find(field_id) == value_counts.end()) {
                value_counts.insert({field_id, column_meta->num_values()});
            } else {
                value_counts[field_id] += column_meta->num_values();
            }

            // TODO(stephen): use arrow merge function
            min_values[field_id] = column_meta->statistics()->EncodeMin();
            max_values[field_id] = column_meta->statistics()->EncodeMax();
        }
    }

    TIcebergColumnStats stats;
    stats.__set_column_sizes(std::map<int32_t, int64_t>());
    stats.__set_value_counts(std::map<int32_t, int64_t>());
    stats.__set_null_value_counts(std::map<int32_t, int64_t>());
    stats.__set_lower_bounds(std::map<int32_t, std::string>());
    stats.__set_upper_bounds(std::map<int32_t, std::string>());

    for (auto& i : column_sizes) {
        stats.column_sizes.insert({i.first, i.second});
    }
    for (auto& i : value_counts) {
        stats.value_counts.insert({i.first, i.second});
    }
    for (auto& i : null_value_counts) {
        stats.null_value_counts.insert({i.first, i.second});
    }
    for (auto& i : min_values) {
        stats.lower_bounds.insert({i.first, i.second});
    }
    for (auto& i : max_values) {
        stats.upper_bounds.insert({i.first, i.second});
    }

    iceberg_data_file.__set_column_stats(stats);

    commit_info.__set_iceberg_data_file(iceberg_data_file);
    state->add_sink_commit_info(commit_info);

    state->update_num_rows_load_sink(iceberg_data_file.record_count);
}

} // namespace starrocks::pipeline
