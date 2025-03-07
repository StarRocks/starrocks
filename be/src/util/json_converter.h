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

#include <simdjson.h>

#include "column/column_builder.h"
#include "column/type_traits.h"
#include "common/compiler_util.h"
#include "common/statusor.h"
#include "util/json.h"

namespace starrocks {

using SimdJsonValue = simdjson::ondemand::value;
using SimdJsonObject = simdjson::ondemand::object;

// Convert SIMD-JSON object/value to a JsonValue
StatusOr<JsonValue> convert_from_simdjson(SimdJsonValue value);
StatusOr<JsonValue> convert_from_simdjson(SimdJsonObject value);

// like getNumber, but don't check
template <LogicalType ResultType>
static StatusOr<RunTimeCppType<ResultType>> get_number_from_vpjson(const vpack::Slice& slice) {
    constexpr auto min = RunTimeTypeLimits<ResultType>::min_value();
    constexpr auto max = RunTimeTypeLimits<ResultType>::max_value();

    // signed interger type
    if constexpr (lt_is_integer<ResultType>) {
        // signed integral type
        if (LIKELY(slice.isInt() || slice.isSmallInt())) {
            auto v = slice.getIntUnchecked();
            if constexpr (ResultType != TYPE_BIGINT && ResultType != TYPE_LARGEINT) {
                // small to big, don't need check
                if (v < static_cast<int64_t>(min) || v > static_cast<int64_t>(max)) {
                    return Status::JsonFormatError("cast number overflow");
                }
            }
            return static_cast<RunTimeCppType<ResultType>>(v);
        } else if (slice.isDouble()) {
            auto v = slice.getDouble();
            if (v < static_cast<double>(min) || v > static_cast<double>(max)) {
                return Status::JsonFormatError("cast number overflow");
            }
            return static_cast<RunTimeCppType<ResultType>>(v);
        } else if (slice.isUInt()) {
            auto v = slice.getUIntUnchecked();
            if constexpr (ResultType != TYPE_LARGEINT) {
                // uint must be positive
                if (v > static_cast<uint64_t>(max)) {
                    return Status::JsonFormatError("cast number overflow");
                }
            }
            return static_cast<RunTimeCppType<ResultType>>(v);
        } else if (slice.isString()) {
            // keep same as cast(string as int)
            vpack::ValueLength len;
            const char* str = slice.getStringUnchecked(len);
            StringParser::ParseResult parseResult;
            auto r = StringParser::string_to_int<RunTimeCppType<ResultType>>(str, len, &parseResult);
            if (parseResult != StringParser::PARSE_SUCCESS) {
                return Status::JsonFormatError("cast number from string failed");
            }
            return r;
        }
    } else if (lt_is_float<ResultType>) {
        // floating point type
        if (LIKELY(slice.isDouble())) {
            return static_cast<RunTimeCppType<ResultType>>(slice.getDouble());
        } else if (slice.isInt() || slice.isSmallInt()) {
            return static_cast<RunTimeCppType<ResultType>>(slice.getIntUnchecked());
        } else if (slice.isUInt()) {
            return static_cast<RunTimeCppType<ResultType>>(slice.getUIntUnchecked());
        } else if (slice.isString()) {
            // keep same as cast(string as double)
            vpack::ValueLength len;
            const char* str = slice.getStringUnchecked(len);
            StringParser::ParseResult parseResult;
            auto r = StringParser::string_to_float<RunTimeCppType<ResultType>>(str, len, &parseResult);
            if (parseResult != StringParser::PARSE_SUCCESS || std::isnan(r) || std::isinf(r)) {
                return Status::JsonFormatError("cast float from string failed");
            }
            return r;
        }
    }
    return Status::JsonFormatError("not a number");
}

template <LogicalType ResultType, bool AllowThrowException>
static Status cast_vpjson_to(const vpack::Slice& slice, ColumnBuilder<ResultType>& result) {
    if constexpr (!lt_is_arithmetic<ResultType> && !lt_is_string<ResultType> && ResultType != TYPE_JSON) {
        if constexpr (AllowThrowException) {
            return Status::NotSupported(fmt::format("not supported type {}", type_to_string(ResultType)));
        }
        result.append_null();
        return Status::OK();
    }

    if (slice.isNone()) {
        result.append_null();
        return Status::OK();
    }

    if constexpr (ResultType == TYPE_JSON) {
        JsonValue jv(slice);
        result.append(std::move(jv));
        return Status::OK();
    }
    if (slice.isNull()) {
        result.append_null();
        return Status::OK();
    }
    try {
        if constexpr (ResultType == TYPE_BOOLEAN) {
            if (LIKELY(slice.isBoolean())) {
                result.append(slice.getBool());
            } else if (slice.isString()) {
                // keep same as cast(string as boolean)
                vpack::ValueLength len;
                const char* str = slice.getStringUnchecked(len);
                StringParser::ParseResult parseResult;
                auto r = StringParser::string_to_int<int32_t>(str, len, &parseResult);
                if (parseResult != StringParser::PARSE_SUCCESS || std::isnan(r) || std::isinf(r)) {
                    bool b = StringParser::string_to_bool(str, len, &parseResult);
                    if (parseResult != StringParser::PARSE_SUCCESS) {
                        if constexpr (AllowThrowException) {
                            return Status::JsonFormatError(
                                    fmt::format("cast from JSON({}) to BOOLEAN failed", std::string(str, len)));
                        }
                        result.append_null();
                        return Status::OK();
                    }
                    result.append(b);
                } else {
                    result.append(r != 0);
                }
            } else {
                auto res = get_number_from_vpjson<TYPE_DOUBLE>(slice);
                res.ok() ? result.append(res.value() != 0) : result.append_null();
            }
            return Status::OK();
        }
        if constexpr (lt_is_arithmetic<ResultType>) {
            if (slice.isBool()) {
                result.append(slice.getBool());
            } else {
                auto res = get_number_from_vpjson<ResultType>(slice);
                if (res.ok()) {
                    result.append(res.value());
                } else {
                    if constexpr (AllowThrowException) {
                        return res.status();
                    }
                    result.append_null();
                }
            }
            return Status::OK();
        }
        if constexpr (lt_is_string<ResultType>) {
            if (LIKELY(slice.isString())) {
                vpack::ValueLength len;
                const char* str = slice.getStringUnchecked(len);
                result.append(Slice(str, len));
            } else {
                vpack::Options options = vpack::Options::Defaults;
                options.singleLinePrettyPrint = true;
                std::string str = slice.toJson(&options);

                result.append(Slice(str));
            }
        }
    } catch (const vpack::Exception& e) {
        if constexpr (AllowThrowException) {
            return Status::JsonFormatError(fmt::format("cast from JSON to {} failed", type_to_string(ResultType)));
        }
        result.append_null();
    }
    return Status::OK();
}
} // namespace starrocks
