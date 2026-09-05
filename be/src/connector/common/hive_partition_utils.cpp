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

#include "connector/common/hive_partition_utils.h"

#include <cmath>
#include <sstream>

#include "arrow/util/decimal.h"
#include "base/base64.h"
#include "base/url_coding.h"
#include "base/utility/integer_util.h"
#include "column/column.h"
#include "types/datum.h"

namespace starrocks::connector {

StatusOr<std::string> HivePartitionUtils::make_partition_name(
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

template <typename T>
StatusOr<std::string> HivePartitionUtils::format_decimal_value(T value, int scale) {
    if (scale < 0) {
        return Status::InvalidArgument("scale must be non-negative");
    }

    bool is_negative = value < 0;
    std::string res = integer_to_string(value);
    if (is_negative) {
        res = res.substr(1);
    }
    if (scale >= res.length()) {
        res.insert(0, scale - res.length() + 1, '0');
    }

    int position = res.length() - scale;
    res.insert(position, ".");

    if (is_negative) {
        res.insert(0, "-");
    }
    return res;
}

template StatusOr<std::string> HivePartitionUtils::format_decimal_value<int32_t>(int32_t value, int scale);
template StatusOr<std::string> HivePartitionUtils::format_decimal_value<int64_t>(int64_t value, int scale);
template StatusOr<std::string> HivePartitionUtils::format_decimal_value<int128_t>(int128_t value, int scale);

// TODO(letian-jiang): translate org.apache.hadoop.hive.common.FileUtils#makePartName
StatusOr<std::string> HivePartitionUtils::column_value(const TypeDescriptor& type_desc, const ColumnPtr& column,
                                                       int i) {
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
    case TYPE_BINARY: {
        // No secnario will reach here now, use TYPE_VARBINARY instead
        /*
            std::string origin_str = datum.get_slice().to_string();
            if (origin_str.length() < type_desc.len) {
                origin_str.append(type_desc.len - origin_str.length(), ' ');
            }
            std::string base_encode;
            base64_encode(origin_str, &base_encode);
            return url_encode(base_encode);
        */
    }
    case TYPE_VARBINARY: {
        int len = (size_t)(4.0 * ceil((double)datum.get_slice().get_size() / 3.0)) + 1;
        std::string base_encode;
        base_encode.resize(len + 1);
        size_t res_len = base64_encode2((const unsigned char*)datum.get_slice().get_data(),
                                        datum.get_slice().get_size(), (unsigned char*)base_encode.data());
        if (res_len <= 0) {
            base_encode.resize(0);
        }
        base_encode.resize(res_len);
        return url_encode(base_encode);
    }
    case TYPE_DECIMAL32: {
        auto value = datum.get_int32();
        arrow::Decimal128 decimal_value(integer_to_string(value));
        return decimal_value.ToString(type_desc.scale);
    }
    case TYPE_DECIMAL64: {
        auto value = datum.get_int64();
        arrow::Decimal128 decimal_value(integer_to_string(value));
        return decimal_value.ToString(type_desc.scale);
    }
    case TYPE_DECIMAL128: {
        auto value = datum.get_int128();
        arrow::Decimal128 decimal_value(integer_to_string(value));
        return decimal_value.ToString(type_desc.scale);
    }
    default: {
        return Status::InvalidArgument("unsupported partition column type for column value" + type_desc.debug_string());
    }
    }
}

} // namespace starrocks::connector
