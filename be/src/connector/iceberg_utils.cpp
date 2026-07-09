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

#include "connector/iceberg_utils.h"

#include <ctime>
#include <sstream>

#include "column/chunk_extra_data.h"
#include "column/column.h"
#include "column/column_viewer.h"
#include "connector/hive_utils.h"

namespace starrocks::connector {

StatusOr<std::string> IcebergUtils::iceberg_make_partition_name(
        const std::vector<std::string>& partition_column_names,
        const std::vector<std::unique_ptr<ColumnEvaluator>>& column_evaluators,
        const std::vector<std::string>& transform_exprs, Chunk* chunk, bool support_null_partition,
        std::vector<int8_t>& field_is_null) {
    DCHECK_EQ(partition_column_names.size(), transform_exprs.size());
    std::stringstream ss;
    field_is_null.resize(partition_column_names.size(), false);
    if (chunk->has_extra_data()) {
        const auto& extra_data = down_cast<ChunkExtraColumnsData*>(chunk->get_extra_data().get());
        const auto& metadatas = extra_data->chunk_data_metas();
        const auto& columns = extra_data->columns();
        DCHECK_EQ(columns.size(), partition_column_names.size());
        for (size_t i = 0; i < columns.size(); i++) {
            const auto& meta = metadatas.at(i);
            auto column = columns[i];
            ASSIGN_OR_RETURN(std::string value,
                             iceberg_column_value(meta.type, column, 0, transform_exprs[i], field_is_null[i]));
            if (!support_null_partition && field_is_null[i]) {
                return Status::NotSupported("Partition value can't be null.");
            }
            ss << partition_column_names[i] << "=" << value << "/";
        }
    } else {
        for (size_t i = 0; i < column_evaluators.size(); i++) {
            ASSIGN_OR_RETURN(auto column, column_evaluators[i]->evaluate(chunk));
            auto type = column_evaluators[i]->type();
            ASSIGN_OR_RETURN(std::string value,
                             iceberg_column_value(type, column, 0, transform_exprs[i], field_is_null[i]));
            if (!support_null_partition && field_is_null[i]) {
                return Status::NotSupported("Partition value can't be null.");
            }
            ss << partition_column_names[i] << "=" << value << "/";
        }
    }
    return ss.str();
}

StatusOr<std::string> IcebergUtils::iceberg_column_value(const TypeDescriptor& type_desc, const ColumnPtr& column,
                                                         const int idx, const std::string& transform_expr,
                                                         int8_t& is_null) {
    std::string value;
    is_null = false;
    if (idx >= column->size()) {
        return Status::InternalError("column size of extra chunk in make partition name is less than index:" +
                                     std::to_string(idx));
    } else if (column->is_null(idx)) {
        value = "null";
        is_null = true;
    } else if (transform_expr == "void") {
        value = "null";
        is_null = true;
    } else if (transform_expr == "year") {
        const auto years_from_epoch = ColumnViewer<TYPE_BIGINT>(column).value(idx);
        value = std::to_string(years_from_epoch + 1970);
    } else if (transform_expr == "month") {
        const auto months_from_epoch = ColumnViewer<TYPE_BIGINT>(column).value(idx);
        int year = 1970 + months_from_epoch / 12;
        int month = months_from_epoch % 12 + 1;
        std::tm timeinfo = {};
        timeinfo.tm_year = year - 1900;
        timeinfo.tm_mon = month - 1;
        char buffer[10];
        size_t written = std::strftime(buffer, sizeof(buffer), "%Y-%m", &timeinfo);
        if (written == 0) {
            return Status::InternalError("strftime overflow formatting iceberg month partition");
        }
        value = std::string(buffer, written);
    } else if (transform_expr == "day") {
        const auto days_from_epoch = ColumnViewer<TYPE_BIGINT>(column).value(idx);
        std::time_t seconds = static_cast<std::time_t>(days_from_epoch) * 86400;
        std::tm tm_utc;
        gmtime_r(&seconds, &tm_utc);
        char buffer[20];
        size_t written = std::strftime(buffer, sizeof(buffer), "%Y-%m-%d", &tm_utc);
        if (written == 0) {
            return Status::InternalError("strftime overflow formatting iceberg day partition");
        }
        value = std::string(buffer, written);
    } else if (transform_expr == "hour") {
        const auto hours_from_epoch = ColumnViewer<TYPE_BIGINT>(column).value(idx);
        std::time_t seconds = static_cast<std::time_t>(hours_from_epoch) * 3600;
        std::tm tm_utc;
        gmtime_r(&seconds, &tm_utc);
        char buffer[20];
        size_t written = std::strftime(buffer, sizeof(buffer), "%Y-%m-%d-%H", &tm_utc);
        if (written == 0) {
            return Status::InternalError("strftime overflow formatting iceberg hour partition");
        }
        value = std::string(buffer, written);
    } else if (transform_expr.compare(0, 8, "truncate") == 0) {
        ASSIGN_OR_RETURN(value, HiveUtils::column_value(type_desc, column, idx));
    } else if (transform_expr.compare(0, 6, "bucket") == 0) {
        ASSIGN_OR_RETURN(value, HiveUtils::column_value(type_desc, column, idx));
    } else if (transform_expr == "identity") {
        ASSIGN_OR_RETURN(value, HiveUtils::column_value(type_desc, column, idx));
    } else {
        return Status::InternalError("Unsupported type for iceberg partition transform:" + transform_expr);
    }
    return value;
}

std::vector<formats::FileColumnId> IcebergUtils::generate_parquet_field_ids(
        const std::vector<TIcebergSchemaField>& fields) {
    std::vector<formats::FileColumnId> file_column_ids(fields.size());
    for (int i = 0; i < fields.size(); ++i) {
        file_column_ids[i].field_id = fields[i].field_id;
        file_column_ids[i].children = generate_parquet_field_ids(fields[i].children);
    }
    return file_column_ids;
}

} // namespace starrocks::connector
