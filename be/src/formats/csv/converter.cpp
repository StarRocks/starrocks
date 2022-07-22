// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "formats/csv/converter.h"

#include "formats/csv/array_converter.h"
#include "formats/csv/binary_converter.h"
#include "formats/csv/boolean_converter.h"
#include "formats/csv/date_converter.h"
#include "formats/csv/datetime_converter.h"
#include "formats/csv/decimalv2_converter.h"
#include "formats/csv/decimalv3_converter.h"
#include "formats/csv/float_converter.h"
#include "formats/csv/nullable_converter.h"
#include "formats/csv/numeric_converter.h"
#include "runtime/types.h"

namespace starrocks::vectorized::csv {

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
    case TYPE_DECIMAL:
    case INVALID_TYPE:
    case TYPE_NULL:
    case TYPE_BINARY:
    case TYPE_STRUCT:
    case TYPE_MAP:
    case TYPE_HLL:
    case TYPE_PERCENTILE:
    case TYPE_TIME:
    case TYPE_OBJECT:
    case TYPE_JSON:
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

// Hive collection delimiter generate rule refer to:
// https://github.com/apache/hive/blob/master/serde/src/java/org/apache/hadoop/hive/serde2/lazy/LazySerDeParameters.java#L250
static char get_collection_delimiter(char mapkey_delimiter, size_t nested_array_level) {
    DCHECK(nested_array_level > 1 && nested_array_level < 153);

    if (nested_array_level == 2) {
        return mapkey_delimiter;
    }

    // tmp maybe negative, dont use size_t.
    int32_t tmp;
    if (nested_array_level <= 7) {
        // [3, 7] -> [4, 8]
        tmp = static_cast<int32_t>(nested_array_level) + (4 - 3);
    } else if (nested_array_level <= 20) {
        // [8, 20] -> [14, 26]
        tmp = static_cast<int32_t>(nested_array_level) + (14 - 8);
    } else if (nested_array_level <= 24) {
        // [21, 24] -> [28, 31]
        tmp = static_cast<int32_t>(nested_array_level) + (28 - 21);
    } else if (nested_array_level <= 152) {
        // [25, 152] -> [-128, -1]
        tmp = static_cast<int32_t>(nested_array_level) + (-128 - 25);
    }

    return static_cast<char>(tmp);
}

std::unique_ptr<Converter> get_converter(const TypeDescriptor& type_desc, bool nullable, char collection_delimiter,
                                         char mapkey_delimiter, size_t nested_array_level) {
    if (type_desc.type != TYPE_ARRAY || nested_array_level < 1) {
        // this should not happen
        return get_converter(type_desc, nullable);
    }

    // Must be array type and has collection_delimiter
    size_t next_nested_array_level = nested_array_level + 1;
    char next_delimiter = get_collection_delimiter(mapkey_delimiter, next_nested_array_level);

    auto sub_converter =
            get_converter(type_desc.children[0], true, next_delimiter, mapkey_delimiter, next_nested_array_level);
    if (sub_converter == nullptr) {
        return nullptr;
    }
    auto c = std::make_unique<ArrayConverter>(std::move(sub_converter), collection_delimiter);
    if (c == nullptr) {
        return nullptr;
    }

    if (nullable) {
        return std::make_unique<NullableConverter>(std::move(c));
    } else {
        return std::move(c);
    }
}

} // namespace starrocks::vectorized::csv
