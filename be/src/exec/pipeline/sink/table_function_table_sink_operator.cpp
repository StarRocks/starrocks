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

bool TableFunctionTableSinkOperator::need_input() const {
    for (const auto& writer : _partition_writers) {
        if (!writer.second->writable()) {
            return false;
        }
    }

    return true;
}

bool TableFunctionTableSinkOperator::is_finished() const {
    if (_partition_writers.size() == 0) {
        return _is_finished.load();
    }
    for (const auto& writer : _partition_writers) {
        if (!writer.second->closed()) {
            return false;
        }

        auto st = writer.second->get_io_status();
        if (!st.ok()) {
            LOG(WARNING) << "cancel fragment: " << st.message();
            _fragment_ctx->cancel(st);
        }
    }

    return true;
}

Status TableFunctionTableSinkOperator::set_finishing(RuntimeState* state) {
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

bool TableFunctionTableSinkOperator::pending_finish() const {
    return !is_finished();
}

Status TableFunctionTableSinkOperator::set_cancelled(RuntimeState* state) {
    return Status::OK();
}

StatusOr<ChunkPtr> TableFunctionTableSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from table function table sink operator");
}

Status TableFunctionTableSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    if (_partition_exprs.empty()) {
        if (_partition_writers.empty()) {
            auto writer = std::make_unique<RollingAsyncParquetWriter>(_make_table_info(_path), _output_exprs,
                                                                      _common_metrics.get(), add_commit_info, state,
                                                                      _driver_sequence);
            RETURN_IF_ERROR(writer->init());
            _partition_writers.insert({"default writer", std::move(writer)});
        }
        return _partition_writers["default writer"]->append_chunk(chunk.get(), state);
    }

    // compute partition values and correspondent location
    std::vector<std::string> partition_column_values(_partition_exprs.size());
    for (size_t i = 0; i < _partition_exprs.size(); ++i) {
        ASSIGN_OR_RETURN(auto column, _partition_exprs[i]->evaluate(chunk.get()));
        ASSIGN_OR_RETURN(partition_column_values[i], column_to_string(_partition_exprs[i]->root()->type(), column))
    }
    string partition_location = get_partition_location(_path, _partition_column_names, partition_column_values);

    auto partition_writer = _partition_writers.find(partition_location);
    // create writer for current partition if not exists
    if (partition_writer == _partition_writers.end()) {
        auto writer = std::make_unique<RollingAsyncParquetWriter>(_make_table_info(partition_location), _output_exprs,
                                                                  _common_metrics.get(), add_commit_info, state,
                                                                  _driver_sequence);
        RETURN_IF_ERROR(writer->init());
        RETURN_IF_ERROR(writer->append_chunk(chunk.get(), state));
        _partition_writers.insert({partition_location, std::move(writer)});
        return Status::OK();
    }

    return _partition_writers[partition_location]->append_chunk(chunk.get(), state);
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

Status TableFunctionTableSinkOperatorFactory::prepare(RuntimeState* state) {
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

OperatorPtr TableFunctionTableSinkOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<TableFunctionTableSinkOperator>(
            this, _id, _plan_node_id, driver_sequence, _path, _file_format, _compression_type, _output_exprs,
            _partition_exprs, _partition_column_names, _write_single_file, _cloud_conf, _fragment_ctx,
            _parquet_file_schema);
}

void TableFunctionTableSinkOperatorFactory::close(RuntimeState* state) {
    Expr::close(_partition_exprs, state);
    Expr::close(_output_exprs, state);
    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
