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

#include "storage/primitive/tablet_column_type_utils.h"

#include <cstdint>

namespace starrocks {

uint32_t get_tablet_column_field_length_by_type(LogicalType type, uint32_t string_length) {
    switch (type) {
    case TYPE_UNKNOWN:
    case TYPE_DISCRETE_DOUBLE:
    case TYPE_STRUCT:
    case TYPE_MAP:
    case TYPE_NONE:
    case TYPE_NULL:
    case TYPE_FUNCTION:
    case TYPE_TIME:
    case TYPE_BINARY:
    case TYPE_MAX_VALUE:
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_UNSIGNED_TINYINT:
        return 1;
    case TYPE_SMALLINT:
    case TYPE_UNSIGNED_SMALLINT:
        return 2;
    case TYPE_DATE_V1:
        return 3;
    case TYPE_INT:
    case TYPE_UNSIGNED_INT:
    case TYPE_FLOAT:
    case TYPE_DATE:
    case TYPE_DECIMAL32:
        return 4;
    case TYPE_BIGINT:
    case TYPE_UNSIGNED_BIGINT:
    case TYPE_DOUBLE:
    case TYPE_DATETIME_V1:
    case TYPE_DATETIME:
    case TYPE_DECIMAL64:
        return 8;
    case TYPE_DECIMAL:
        return 12;
    case TYPE_LARGEINT:
    case TYPE_OBJECT:
    case TYPE_DECIMALV2:
    case TYPE_DECIMAL128:
        return 16;
    case TYPE_DECIMAL256:
    case TYPE_INT256:
        return 32;
    case TYPE_CHAR:
        return string_length;
    case TYPE_VARCHAR:
    case TYPE_HLL:
    case TYPE_PERCENTILE:
    case TYPE_JSON:
    case TYPE_VARIANT:
    case TYPE_VARBINARY:
        return string_length + sizeof(uint32_t);
    case TYPE_ARRAY:
        return string_length;
    }
    return 0;
}

} // namespace starrocks
