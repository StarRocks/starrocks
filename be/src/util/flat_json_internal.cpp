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

#include "util/flat_json_internal.h"

#include <cmath>
#include <cstdint>
#include <string_view>

#include "base/string/slice.h"
#include "base/string/string_parser.hpp"
#include "column/json_column.h"
#include "column/json_converter.h"
#include "column/nullable_column.h"
#include "column/runtime_type_traits.h"
#include "common/compiler_util.h"
#include "common/logging.h"
#include "gutil/casts.h"
#include "types/json_value.h"

namespace starrocks {

namespace flat_json {
template <LogicalType TYPE>
void extract_number(const vpack::Slice* json, NullableColumn* result) {
    // NOTE: this function catch all vpack exceptions, so it must be exception-safe, instead of modifying the result into
    // an inconsistence state where null_column and data_column have different sizes.
    using CppType = RunTimeCppType<TYPE>;
    Datum datum;
    try {
        if (LIKELY(json->isNumber() || json->isString())) {
            auto st = get_number_from_vpjson<TYPE>(*json);
            if (st.ok()) {
                datum.set<CppType>(st.value());
            }
        } else if (json->isNone() || json->isNull()) {
            datum.set_null();
        } else if (json->isBool()) {
            datum.set<CppType>(static_cast<CppType>(json->getBool()));
        } else {
            datum.set_null();
        }
    } catch (const vpack::Exception& e) {
        LOG(INFO) << "vpack::Exception in extract_number: " << e.what();
        datum.set_null();
    }

    if (datum.is_null()) {
        result->append_nulls(1);
    } else {
        result->append_datum(datum);
    }
}

void extract_bool(const vpack::Slice* json, NullableColumn* result) {
    // NOTE: this function catch all vpack exceptions, so it must be exception-safe, instead of modifying the result into
    // an inconsistence state where null_column and data_column have different sizes.
    using CppType = RunTimeCppType<TYPE_BOOLEAN>;
    Datum datum;
    try {
        if (json->isNone() || json->isNull()) {
            datum.set_null();
        } else if (json->isBool()) {
            auto res = json->getBool();
            datum.set<CppType>(res);
        } else if (json->isString()) {
            vpack::ValueLength len;
            const char* str = json->getStringUnchecked(len);
            StringParser::ParseResult parseResult;
            auto r = StringParser::string_to_int<int32_t>(str, len, &parseResult);
            if (parseResult != StringParser::PARSE_SUCCESS || std::isnan(r) || std::isinf(r)) {
                bool b = StringParser::string_to_bool(str, len, &parseResult);
                if (parseResult != StringParser::PARSE_SUCCESS) {
                    datum.set_null();
                } else {
                    datum.set<CppType>(b);
                }
            } else {
                datum.set<CppType>(r != 0);
            }
        } else if (json->isNumber()) {
            auto res = json->getNumber<double>();
            datum.set<CppType>(res != 0);
        } else {
            datum.set_null();
        }
    } catch (const vpack::Exception& e) {
        LOG(INFO) << "vpack::Exception in extract_bool: " << e.what();
        datum.set_null();
    }

    if (datum.is_null()) {
        result->append_nulls(1);
    } else {
        result->append_datum(datum);
    }
}

void extract_string(const vpack::Slice* json, NullableColumn* result) {
    // NOTE: this function catch all vpack exceptions, so it must be exception-safe, instead of modifying the result into
    // an inconsistence state where null_column and data_column have different sizes.
    try {
        if (json->isNone() || json->isNull()) {
            result->append_nulls(1);
        } else if (json->isString()) {
            vpack::ValueLength len;
            const char* str = json->getStringUnchecked(len);
            result->append_datum(Datum(Slice(str, len)));
        } else {
            vpack::Options options = vpack::Options::Defaults;
            options.singleLinePrettyPrint = true;
            options.dumpAttributesInIndexOrder = false;
            std::string str = json->toJson(&options);
            result->append_datum(Datum(Slice(str)));
        }
    } catch (const vpack::Exception& e) {
        LOG(INFO) << "vpack::Exception in extract_string: " << e.what();
        result->append_nulls(1);
    }
}

void extract_json(const vpack::Slice* json, NullableColumn* result) {
    if (json->isNone()) {
        result->append_nulls(1);
    } else {
        down_cast<JsonColumn*>(result->data_column_raw_ptr())->append(JsonValue(*json));
        result->null_column_raw_ptr()->append(0);
    }
}

template <LogicalType TYPE>
void merge_number(vpack::Builder* builder, const std::string_view& name, const Column* src, size_t idx) {
    DCHECK(src->is_nullable());
    auto* nullable_column = down_cast<const NullableColumn*>(src);
    auto* col = down_cast<const RunTimeColumnType<TYPE>*>(nullable_column->data_column().get());
    const auto data = col->immutable_data().data();

    if constexpr (TYPE == LogicalType::TYPE_LARGEINT) {
        // the value is from json, must be uint64_t
        builder->addUnchecked(name.data(), name.size(), vpack::Value((uint64_t)data[idx]));
    } else {
        builder->addUnchecked(name.data(), name.size(), vpack::Value(data[idx]));
    }
}

void merge_string(vpack::Builder* builder, const std::string_view& name, const Column* src, size_t idx) {
    DCHECK(src->is_nullable());
    auto* nullable_column = down_cast<const NullableColumn*>(src);
    auto* col = down_cast<const BinaryColumn*>(nullable_column->data_column().get());
    builder->addUnchecked(name.data(), name.size(), vpack::Value(col->get_slice(idx).to_string()));
}

void merge_json(vpack::Builder* builder, const std::string_view& name, const Column* src, size_t idx) {
    DCHECK(src->is_nullable());
    auto* nullable_column = down_cast<const NullableColumn*>(src);
    auto* col = down_cast<const JsonColumn*>(nullable_column->data_column().get());
    builder->addUnchecked(name.data(), name.size(), col->get_object(idx)->to_vslice());
}

// clang-format off
using JsonFlatExtractFunc = void (*)(const vpack::Slice* json, NullableColumn* result);
using JsonFlatMergeFunc = void (*)(vpack::Builder* builder, const std::string_view& name, const Column* src, size_t idx);
const uint8_t JSON_BASE_TYPE_BITS = 0;   // least flat to JSON type
const uint8_t JSON_BIGINT_TYPE_BITS = 7; // bigint compatible type
// static const uint8_t JSON_NULL_TYPE_BITS = 31;  // JSON_NULL_TYPE_BITS, initial value for JsonFlatDesc::type

// bool will flatting as string, because it's need save string-literal(true/false)
// int & string compatible type is json, because int cast to string will add double quote, it's different with json
const FlatJsonHashMap<vpack::ValueType, uint8_t> JSON_TYPE_BITS {
        {vpack::ValueType::None, 31},      //  00011111, 31
        {vpack::ValueType::SmallInt, 15},  //  00001111, 15
        {vpack::ValueType::Int, 7},        //  00000111, 7
        {vpack::ValueType::UInt, 3},       //  00000011, 3
        {vpack::ValueType::Double, 1},     //  00000001, 1
        {vpack::ValueType::String, 16},    //  00010000, 16
};

// json base type
const phmap::flat_hash_set<vpack::ValueType> JSON_BASE_TYPE {
        vpack::ValueType::None,
        vpack::ValueType::Null,
        vpack::ValueType::Bool,
        vpack::ValueType::SmallInt,
        vpack::ValueType::Int,
        vpack::ValueType::UInt,
        vpack::ValueType::Double,
        vpack::ValueType::String,
        vpack::ValueType::Binary,
};

// starrocks json fucntio only support read as bigint/string/bool/double, smallint will cast to bigint, so we save as bigint directly
const FlatJsonHashMap<uint8_t, LogicalType> JSON_BITS_TO_LOGICAL_TYPE {
    {JSON_TYPE_BITS.at(vpack::ValueType::None),        LogicalType::TYPE_TINYINT},
    {JSON_TYPE_BITS.at(vpack::ValueType::SmallInt),    LogicalType::TYPE_BIGINT},
    {JSON_TYPE_BITS.at(vpack::ValueType::Int),         LogicalType::TYPE_BIGINT},
    {JSON_TYPE_BITS.at(vpack::ValueType::UInt),        LogicalType::TYPE_LARGEINT},
    {JSON_TYPE_BITS.at(vpack::ValueType::Double),      LogicalType::TYPE_DOUBLE},
    {JSON_TYPE_BITS.at(vpack::ValueType::String),      LogicalType::TYPE_VARCHAR},
    {JSON_BASE_TYPE_BITS,                                LogicalType::TYPE_JSON},
};

const FlatJsonHashMap<LogicalType, uint8_t> LOGICAL_TYPE_TO_JSON_BITS {
    {LogicalType::TYPE_TINYINT,         JSON_TYPE_BITS.at(vpack::ValueType::None)},
    {LogicalType::TYPE_BIGINT,          JSON_TYPE_BITS.at(vpack::ValueType::Int)},
    {LogicalType::TYPE_LARGEINT,        JSON_TYPE_BITS.at(vpack::ValueType::UInt)},
    {LogicalType::TYPE_DOUBLE,          JSON_TYPE_BITS.at(vpack::ValueType::Double)},
    {LogicalType::TYPE_VARCHAR,         JSON_TYPE_BITS.at(vpack::ValueType::String)},
    {LogicalType::TYPE_JSON,            JSON_BASE_TYPE_BITS},
};

const FlatJsonHashMap<LogicalType, JsonFlatExtractFunc> JSON_EXTRACT_FUNC {
    {LogicalType::TYPE_TINYINT,         &extract_number<LogicalType::TYPE_TINYINT>},
    {LogicalType::TYPE_BIGINT,          &extract_number<LogicalType::TYPE_BIGINT>},
    {LogicalType::TYPE_LARGEINT,        &extract_number<LogicalType::TYPE_LARGEINT>},
    {LogicalType::TYPE_DOUBLE,          &extract_number<LogicalType::TYPE_DOUBLE>},
    {LogicalType::TYPE_BOOLEAN,         &extract_bool},
    {LogicalType::TYPE_VARCHAR,         &extract_string},
    {LogicalType::TYPE_CHAR,            &extract_string},
    {LogicalType::TYPE_JSON,            &extract_json},
};

// should match with extract function
const FlatJsonHashMap<LogicalType, JsonFlatMergeFunc> JSON_MERGE_FUNC {
    {LogicalType::TYPE_TINYINT,       &merge_number<LogicalType::TYPE_TINYINT>},
    {LogicalType::TYPE_BIGINT,        &merge_number<LogicalType::TYPE_BIGINT>},
    {LogicalType::TYPE_LARGEINT,      &merge_number<LogicalType::TYPE_LARGEINT>},
    {LogicalType::TYPE_DOUBLE,        &merge_number<LogicalType::TYPE_DOUBLE>},
    {LogicalType::TYPE_VARCHAR,       &merge_string},
    {LogicalType::TYPE_JSON,          &merge_json},
};
// clang-format on

uint8_t get_compatibility_type(vpack::ValueType type1, uint8_t type2) {
    auto iter = JSON_TYPE_BITS.find(type1);
    return iter != JSON_TYPE_BITS.end() ? type2 & iter->second : JSON_BASE_TYPE_BITS;
}

} // namespace flat_json

} // namespace starrocks
