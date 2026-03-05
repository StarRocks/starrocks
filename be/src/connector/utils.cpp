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

#include "connector/utils.h"

<<<<<<< HEAD
=======
#include <boost/algorithm/string.hpp>

#include "arrow/util/decimal.h"
#include "base/url_coding.h"
#include "base/utility/integer_util.h"
#include "column/chunk_extra_data.h"
>>>>>>> 85a2748cd8 ([BugFix] Fix duplicated CSV compression suffix in file sink output names (#69749))
#include "column/column.h"
#include "column/datum.h"
#include "exprs/expr.h"
#include "formats/parquet/parquet_file_writer.h"
<<<<<<< HEAD
#include "util/url_coding.h"
=======
#include "types/datum.h"
#include "util/compression/compression_utils.h"
>>>>>>> 85a2748cd8 ([BugFix] Fix duplicated CSV compression suffix in file sink output names (#69749))

namespace starrocks::connector {

std::string normalize_format_name(std::string format) {
    boost::algorithm::trim(format);
    boost::algorithm::to_lower(format);
    boost::algorithm::trim_left_if(format, boost::is_any_of("."));

    // Strip any accidental extension chain like "csv.gz.csv" to keep format canonical.
    auto dot = boost::algorithm::find_first(format, ".");
    if (!dot.empty()) {
        format.erase(dot.begin(), format.end());
    }
    return format;
}

StatusOr<std::string> build_canonical_file_suffix(const std::string& format, TCompressionType::type compression_type) {
    if (format.empty()) {
        return Status::InvalidArgument("file format is empty");
    }
    if (format == formats::CSV) {
        ASSIGN_OR_RETURN(std::string compression_suffix, CompressionUtils::to_compression_ext(compression_type));
        return format + compression_suffix;
    }
    return format;
}

StatusOr<std::string> HiveUtils::make_partition_name(
        const std::vector<std::string>& column_names,
        const std::vector<std::unique_ptr<ColumnEvaluator>>& column_evaluators, Chunk* chunk,
        bool support_null_partition) {
    DCHECK_EQ(column_names.size(), column_evaluators.size());
    std::stringstream ss;
    for (size_t i = 0; i < column_evaluators.size(); i++) {
        ASSIGN_OR_RETURN(auto column, column_evaluators[i]->evaluate(chunk));
        if (!support_null_partition && column->has_null()) {
            return Status::NotSupported("Partition value can't be null.");
        }
        auto type = column_evaluators[i]->type();
        ASSIGN_OR_RETURN(auto value, column_value(type, column, 0));
        ss << column_names[i] << "=" << value << "/";
    }
    return ss.str();
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

// TODO(letian-jiang): translate org.apache.hadoop.hive.common.FileUtils#makePartName
StatusOr<std::string> HiveUtils::column_value(const TypeDescriptor& type_desc, const ColumnPtr& column, int i) {
    DCHECK(i < column->size() && i >= 0);
    auto datum = column->get(i);
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
