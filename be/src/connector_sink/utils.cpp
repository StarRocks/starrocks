//
// Created by Letian Jiang on 2024/1/29.
//

#include "utils.h"

#include "column/column.h"
#include "column/datum.h"
#include "exprs/expr.h"
#include "util/url_coding.h"
#include "formats/parquet/parquet_file_writer.h"

namespace starrocks::connector {

StatusOr<std::string> HiveUtils::make_partition_name(
        const std::vector<std::string>& column_names,
        const std::vector<std::unique_ptr<ColumnEvaluator>>& column_evaluators, ChunkPtr chunk) {
    DCHECK_EQ(column_names.size(), column_evaluators.size());
    std::stringstream ss;
    for (size_t i = 0; i < column_evaluators.size(); i++) {
        ASSIGN_OR_RETURN(auto column, column_evaluators[i]->evaluate(chunk.get()));
        auto type = column_evaluators[i]->type();
        ASSIGN_OR_RETURN(auto value, column_value(type, column));
        ss << column_names[i] << "=" << value << "/";
    }
    return ss.str();
}

std::vector<formats::FileColumnId>
IcebergUtils::generate_parquet_field_ids(const std::vector<TIcebergSchemaField> &fields) {
    std::vector<formats::FileColumnId> file_column_ids(fields.size());
    for (int i = 0; i < fields.size(); ++i) {
        file_column_ids[i].field_id = fields[i].field_id;
        file_column_ids[i].children = generate_parquet_field_ids(fields[i].children);
    }
    return file_column_ids;
}

// TODO(letian-jiang): translate org.apache.hadoop.hive.common.FileUtils#makePartName
StatusOr<std::string> HiveUtils::column_value(const TypeDescriptor& type_desc, const ColumnPtr& column) {
    DCHECK_GT(column->size(), 0);
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

} // namespace starrocks::connector
