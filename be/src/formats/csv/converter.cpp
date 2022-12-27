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

#include "formats/csv/converter.h"

#include "formats/csv/array_converter.h"
#include "formats/csv/binary_converter.h"
#include "formats/csv/boolean_converter.h"
#include "formats/csv/date_converter.h"
#include "formats/csv/datetime_converter.h"
#include "formats/csv/decimalv2_converter.h"
#include "formats/csv/decimalv3_converter.h"
#include "formats/csv/float_converter.h"
#include "formats/csv/json_converter.h"
#include "formats/csv/nullable_converter.h"
#include "formats/csv/numeric_converter.h"
#include "runtime/types.h"

namespace starrocks::csv {

static std::unique_ptr<Converter> get_converter(const TypeDescriptor& t) {
    switch (t.type) {
    case TYPE_BOOLEAN:
        return std::make_unique<BooleanConverter>();
    case TYPE_TINYINT:
        return std::make_unique<NumericConverter<int8_t>>();
    case TYPE_SMALLINT:
        return std::make_unique<NumericConverter<int16_t>>();
    case TYPE_INT:
        return std::make_unique<NumericConverter<int32_t>>();
    case TYPE_BIGINT:
        return std::make_unique<NumericConverter<int64_t>>();
    case TYPE_LARGEINT:
        return std::make_unique<NumericConverter<int128_t>>();
    case TYPE_FLOAT:
        return std::make_unique<FloatConverter<float>>();
    case TYPE_DOUBLE:
        return std::make_unique<FloatConverter<double>>();
    case TYPE_DECIMALV2:
        return std::make_unique<DecimalV2Converter>();
    case TYPE_CHAR:
    case TYPE_VARCHAR:
        return std::make_unique<BinaryConverter>();
    case TYPE_DATE:
        return std::make_unique<DateConverter>();
    case TYPE_DATETIME:
        return std::make_unique<DatetimeConverter>();
    case TYPE_ARRAY:
        return std::make_unique<ArrayConverter>(get_converter(t.children[0], true));
    case TYPE_DECIMAL32:
        return std::make_unique<DecimalV3Converter<int32_t>>(t.precision, t.scale);
    case TYPE_DECIMAL64:
        return std::make_unique<DecimalV3Converter<int64_t>>(t.precision, t.scale);
    case TYPE_DECIMAL128:
        return std::make_unique<DecimalV3Converter<int128_t>>(t.precision, t.scale);
    case TYPE_JSON:
        return std::make_unique<JsonConverter>();
    case TYPE_DECIMAL:
    case TYPE_UNKNOWN:
    case TYPE_NULL:
    case TYPE_BINARY:
    case TYPE_VARBINARY:
    case TYPE_STRUCT:
    case TYPE_MAP:
    case TYPE_HLL:
    case TYPE_PERCENTILE:
    case TYPE_TIME:
    case TYPE_OBJECT:
    case TYPE_FUNCTION:
    case TYPE_UNSIGNED_TINYINT:
    case TYPE_UNSIGNED_SMALLINT:
    case TYPE_UNSIGNED_INT:
    case TYPE_UNSIGNED_BIGINT:
    case TYPE_DISCRETE_DOUBLE:
    case TYPE_DATE_V1:
    case TYPE_DATETIME_V1:
    case TYPE_NONE:
    case TYPE_MAX_VALUE:
        break;
    }
    return nullptr;
}

std::unique_ptr<Converter> get_converter(const TypeDescriptor& type_desc, bool nullable) {
    auto c = get_converter(type_desc);
    if (c == nullptr) {
        return nullptr;
    }
    return nullable ? std::make_unique<NullableConverter>(std::move(c)) : std::move(c);
}

} // namespace starrocks::csv
