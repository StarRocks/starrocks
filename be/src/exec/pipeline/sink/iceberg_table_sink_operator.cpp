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
    } else if (_is_static_partition_insert && !_partition_writers.empty()) {
        _partition_writers.begin()->second->append_chunk(chunk.get(), state);
    } else {
        Columns partitions_columns;
        partitions_columns.resize(_partition_expr.size());
        for (size_t i = 0; i < partitions_columns.size(); ++i) {
            ASSIGN_OR_RETURN(partitions_columns[i], _partition_expr[i]->evaluate(chunk.get()));
            DCHECK(partitions_columns[i] != nullptr);
        }

        const std::vector<std::string>& partition_column_names = _iceberg_table->partition_column_names();
        std::vector<std::string> partition_column_values;
        for (const ColumnPtr& column : partitions_columns) {
            if (column->has_null()) {
                return Status::NotSupported("Partition value can't be null.");
            }

            std::string partition_value;
            RETURN_IF_ERROR(partition_value_to_string(ColumnHelper::get_data_column(column.get()), partition_value));
            partition_column_values.emplace_back(partition_value);
        }

        DCHECK(partition_column_names.size() == partition_column_values.size());

        string partition_location = _get_partition_location(partition_column_names, partition_column_values);

        auto partition_writer = _partition_writers.find(partition_location);
        if (partition_writer == _partition_writers.end()) {
            tableInfo.partition_location = partition_location;
            auto writer = std::make_unique<RollingAsyncParquetWriter>(tableInfo, _output_expr, _common_metrics.get(),
                                                                      add_iceberg_commit_info, state, _driver_sequence);
            _partition_writers.insert({partition_location, std::move(writer)});
            _partition_writers[partition_location]->append_chunk(chunk.get(), state);
        } else {
            partition_writer->second->append_chunk(chunk.get(), state);
        }
    }
    return Status::OK();
}

std::string IcebergTableSinkOperator::_get_partition_location(const std::vector<std::string>& names,
                                                              const std::vector<std::string>& values) {
    std::string partition_location = _iceberg_table_data_location;
    for (size_t i = 0; i < names.size(); i++) {
        partition_location += names[i] + "=" + values[i] + "/";
    }
    return partition_location;
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
          _partition_expr_ctxs(std::move(partition_expr_ctxs)),
          is_static_partition_insert(thrift_sink.is_static_partition_sink) {
    DCHECK(thrift_sink.__isset.location);
    DCHECK(thrift_sink.__isset.file_format);
    DCHECK(thrift_sink.__isset.compression_type);
    DCHECK(thrift_sink.__isset.cloud_configuration);
    DCHECK(thrift_sink.__isset.is_static_partition_sink);
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
        if (!result.ok()) {
            return Status::NotSupported(result.status().message());
        }
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

#define MERGE_STATS_CASE(ParquetType)                                                                              \
    case ParquetType: {                                                                                            \
        auto typed_left_stat =                                                                                     \
                std::static_pointer_cast<::parquet::TypedStatistics<::parquet::PhysicalType<ParquetType>>>(left);  \
        auto typed_right_stat =                                                                                    \
                std::static_pointer_cast<::parquet::TypedStatistics<::parquet::PhysicalType<ParquetType>>>(right); \
        typed_left_stat->Merge(*typed_right_stat);                                                                 \
        return;                                                                                                    \
    }

void merge_stats(const std::shared_ptr<::parquet::Statistics>& left,
                 const std::shared_ptr<::parquet::Statistics>& right) {
    DCHECK(left->physical_type() == right->physical_type());
    switch (left->physical_type()) {
        MERGE_STATS_CASE(::parquet::Type::BOOLEAN);
        MERGE_STATS_CASE(::parquet::Type::INT32);
        MERGE_STATS_CASE(::parquet::Type::INT64);
        MERGE_STATS_CASE(::parquet::Type::INT96);
        MERGE_STATS_CASE(::parquet::Type::FLOAT);
        MERGE_STATS_CASE(::parquet::Type::DOUBLE);
        MERGE_STATS_CASE(::parquet::Type::BYTE_ARRAY);
        MERGE_STATS_CASE(::parquet::Type::FIXED_LEN_BYTE_ARRAY);
    default: {
    }
    }
}

void calculate_column_stats(const std::shared_ptr<::parquet::FileMetaData>& meta, TIcebergColumnStats& t_column_stats) {
    // field_id -> column_stat
    std::map<int32_t, std::shared_ptr<::parquet::Statistics>> column_stats;
    std::map<int32_t, int64_t> column_sizes;
    std::map<int32_t, int64_t> value_counts;
    std::map<int32_t, int64_t> null_value_counts;
    std::map<int32_t, std::string> lower_bounds;
    std::map<int32_t, std::string> upper_bounds;
    bool has_null_count = false;
    bool has_min_max = false;

    // traverse stat of column chunk in each row group
    for (int col_idx = 0; col_idx < meta->num_columns(); col_idx++) {
        auto field_id = meta->schema()->Column(col_idx)->schema_node()->field_id();

        for (int rg_idx = 0; rg_idx < meta->num_row_groups(); rg_idx++) {
            auto column_chunk_meta = meta->RowGroup(rg_idx)->ColumnChunk(col_idx);
            column_sizes[field_id] += column_chunk_meta->total_compressed_size();

            auto column_stat = column_chunk_meta->statistics();
            if (rg_idx == 0) {
                column_stats[field_id] = column_stat;
            } else {
                merge_stats(column_stats[field_id], column_stat);
            }
        }
    }

    for (auto& [field_id, column_stat] : column_stats) {
        value_counts[field_id] = column_stat->num_values();
        if (column_stat->HasNullCount()) {
            has_null_count = true;
            null_value_counts[field_id] = column_stat->null_count();
        }
        if (column_stat->HasMinMax()) {
            has_min_max = true;
            lower_bounds[field_id] = column_stat->EncodeMin();
            upper_bounds[field_id] = column_stat->EncodeMax();
        }
    }

    t_column_stats.__set_column_sizes(column_sizes);
    t_column_stats.__set_value_counts(value_counts);
    if (has_null_count) {
        t_column_stats.__set_null_value_counts(null_value_counts);
    }
    if (has_min_max) {
        t_column_stats.__set_lower_bounds(lower_bounds);
        t_column_stats.__set_upper_bounds(upper_bounds);
    }
}

void IcebergTableSinkOperator::add_iceberg_commit_info(starrocks::parquet::AsyncFileWriter* writer,
                                                       RuntimeState* state) {
    TIcebergColumnStats iceberg_column_stats;
    calculate_column_stats(writer->metadata(), iceberg_column_stats);

    TIcebergDataFile iceberg_data_file;
    iceberg_data_file.__set_partition_path(writer->partition_location());
    iceberg_data_file.__set_path(writer->file_location());
    iceberg_data_file.__set_format("parquet");
    iceberg_data_file.__set_record_count(writer->metadata()->num_rows());
    iceberg_data_file.__set_file_size_in_bytes(writer->file_size());
    std::vector<int64_t> split_offsets;
    writer->split_offsets(split_offsets);
    iceberg_data_file.__set_split_offsets(split_offsets);
    iceberg_data_file.__set_column_stats(iceberg_column_stats);

    TSinkCommitInfo commit_info;
    commit_info.__set_iceberg_data_file(iceberg_data_file);

    // update runtime state
    state->add_sink_commit_info(commit_info);
    state->update_num_rows_load_sink(iceberg_data_file.record_count);
}

Status IcebergTableSinkOperator::partition_value_to_string(Column* column, std::string& partition_value) {
    auto v = column->get(0);
    if (column->is_date()) {
        partition_value = v.get_date().to_string();
        return Status::OK();
    }

    bool not_support = false;
    v.visit([&](auto& variant) {
        std::visit(
                [&](auto&& arg) {
                    using T = std::decay_t<decltype(arg)>;
                    if constexpr (std::is_same_v<T, Slice>) {
                        partition_value = arg.to_string();
                    } else if constexpr (std::is_same_v<T, int8_t> || std::is_same_v<T, uint8_t> ||
                                         std::is_same_v<T, int16_t> || std::is_same_v<T, uint16_t> ||
                                         std::is_same_v<T, uint24_t> || std::is_same_v<T, int32_t> ||
                                         std::is_same_v<T, uint32_t> || std::is_same_v<T, int64_t> ||
                                         std::is_same_v<T, uint64_t>) {
                        partition_value = std::to_string(arg);
                    } else {
                        not_support = true;
                    }
                },
                variant);
    });

    if (not_support) {
        return Status::NotSupported(fmt::format("Partition value can't be {}", column->get_name()));
    }

    return Status::OK();
}

} // namespace starrocks::pipeline
