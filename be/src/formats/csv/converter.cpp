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
#include "formats/csv/boolean_converter.h"
#include "formats/csv/date_converter.h"
#include "formats/csv/datetime_converter.h"
#include "formats/csv/decimalv2_converter.h"
#include "formats/csv/decimalv3_converter.h"
#include "formats/csv/default_value_converter.h"
#include "formats/csv/float_converter.h"
#include "formats/csv/json_converter.h"
#include "formats/csv/map_converter.h"
#include "formats/csv/nullable_converter.h"
#include "formats/csv/numeric_converter.h"
#include "formats/csv/string_converter.h"
#include "formats/csv/varbinary_converter.h"
#include "runtime/types.h"

namespace starrocks::csv {

static std::unique_ptr<Converter> create_converter(const TypeDescriptor& t, const bool is_hive) {
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
        return std::make_unique<StringConverter>();
    case TYPE_DATE:
        return std::make_unique<DateConverter>();
    case TYPE_DATETIME:
        return std::make_unique<DatetimeConverter>();
    case TYPE_ARRAY: {
        auto c = get_converter(t.children[0], true);
        if (c == nullptr) {
            // For hive, if array's element converter is nullptr, we use DefaultValueConverter instead
            // For broker load, if array's element converter is nullptr, we return nullptr directly, to avoid NPE
            // problem when reading array's element.
            return is_hive ? std::make_unique<ArrayConverter>(std::make_unique<DefaultValueConverter>()) : nullptr;
        }
        return std::make_unique<ArrayConverter>(std::move(c));
    }
    case TYPE_DECIMAL32:
        return std::make_unique<DecimalV3Converter<int32_t>>(t.precision, t.scale);
    case TYPE_DECIMAL64:
        return std::make_unique<DecimalV3Converter<int64_t>>(t.precision, t.scale);
    case TYPE_DECIMAL128:
        return std::make_unique<DecimalV3Converter<int128_t>>(t.precision, t.scale);
    case TYPE_JSON:
        return std::make_unique<JsonConverter>();
    case TYPE_VARBINARY:
        return std::make_unique<VarBinaryConverter>();
    case TYPE_MAP: {
        auto key_con = get_converter(t.children[0], true);
        auto value_con = get_converter(t.children[1], true);
        if (key_con == nullptr || value_con == nullptr) {
            return nullptr;
        }
        return std::make_unique<MapConverter>(std::move(key_con), std::move(value_con));
    }
    default:
        break;
    }
    return nullptr;
}

std::unique_ptr<Converter> get_converter(const TypeDescriptor& type_desc, bool nullable) {
    auto c = create_converter(type_desc, false);
    if (c == nullptr) {
        return nullptr;
    }
    return nullable ? std::make_unique<NullableConverter>(std::move(c)) : std::move(c);
}

std::unique_ptr<Converter> get_hive_converter(const TypeDescriptor& type_desc, bool nullable) {
    auto c = create_converter(type_desc, true);
    if (c == nullptr) {
        return std::make_unique<DefaultValueConverter>();
    }
    return nullable ? std::make_unique<NullableConverter>(std::move(c)) : std::move(c);
}

} // namespace starrocks::csv
