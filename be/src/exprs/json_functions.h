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

#pragma once

#include <re2/re2.h>
#include <simdjson.h>
#include <velocypack/vpack.h>

#include <utility>

#include "column/column_builder.h"
#include "column/vectorized_fwd.h"
#include "common/compiler_util.h"
#include "common/status.h"
#include "exprs/function_context.h"
#include "exprs/function_helper.h"
#include "exprs/jsonpath.h"

namespace starrocks {

enum JsonFunctionType {
    JSON_FUN_INT = 0,
    JSON_FUN_DOUBLE,
    JSON_FUN_STRING,

    JSON_FUN_UNKOWN //The last
};

template <LogicalType logical_type>
struct JsonTypeTraits {};

template <>
struct JsonTypeTraits<TYPE_INT> {
    static JsonFunctionType JsonType;
};

template <>
struct JsonTypeTraits<TYPE_DOUBLE> {
    static JsonFunctionType JsonType;
};

template <>
struct JsonTypeTraits<TYPE_VARCHAR> {
    static JsonFunctionType JsonType;
};

extern const re2::RE2 SIMPLE_JSONPATH_PATTERN;

struct SimpleJsonPath {
    std::string key; // key of a json object
    int idx;         // array index of a json array, -1 means not set, -2 means *
    bool is_valid;   // true if the path is successfully parsed

    SimpleJsonPath(std::string key_, int idx_, bool is_valid_) : key(std::move(key_)), idx(idx_), is_valid(is_valid_) {}

    std::string to_string() const {
        std::stringstream ss;
        if (!is_valid) {
            return "INVALID";
        }
        if (!key.empty()) {
            ss << key;
        }
        if (idx == -2) {
            ss << "[*]";
        } else if (idx > -1) {
            ss << "[" << idx << "]";
        }
        return ss.str();
    }

    std::string debug_string() const {
        std::stringstream ss;
        ss << "key: " << key << ", idx: " << idx << ", valid: " << is_valid;
        return ss.str();
    }
};

class JsonFunctions {
public:
    [[nodiscard]] static Status json_path_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    [[nodiscard]] static Status json_path_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    /**
     * @param: [json_string, tagged_value]
     * @paramType: [BinaryColumn, BinaryColumn]
     * @return: Int32Column
     */
    DEFINE_VECTORIZED_FN(get_json_int);

    /**
     * @param: [json_string, tagged_value]
     * @paramType: [BinaryColumn, BinaryColumn]
     * @return: DoubleColumn
     */
    DEFINE_VECTORIZED_FN(get_json_double);

    /**
     * @param: [json_string, tagged_value]
     * @paramType: [BinaryColumn, BinaryColumn]
     * @return: BinaryColumn
     */
    DEFINE_VECTORIZED_FN(get_json_string);

    /**
     * @param: [json_string, tagged_value]
     * @paramType: [JsonColumn, BinaryColumn]
     * @return: Int32Column
     */
    DEFINE_VECTORIZED_FN(get_native_json_int);

    /**
     * @param: [json_string, tagged_value]
     * @paramType: [JsonColumn, BinaryColumn]
     * @return: DoubleColumn
     */
    DEFINE_VECTORIZED_FN(get_native_json_double);

    /**
     * @param: [json_string, tagged_value]
     * @paramType: [JsonColumn, BinaryColumn]
     * @return: BinaryColumn
     */
    DEFINE_VECTORIZED_FN(get_native_json_string);

    /**
     * @param: [json_string]
     * @paramType: [BinaryColumn]
     * @return: JsonColumn
     */
    DEFINE_VECTORIZED_FN(parse_json);

    /**
     * @param: [json_column]
     * @paramType: [JsonColumn]
     * @return: BinaryColumn
     */
    DEFINE_VECTORIZED_FN(json_string);

    DEFINE_VECTORIZED_FN(json_query);

    /**
     * @param: [json_object, json_path]
     * @paramType: [JsonColumn, BinaryColumn]
     * @return: BooleanColumn
     */
    DEFINE_VECTORIZED_FN(json_exists);

    /**
     * Build json object from json values
     * @param: [field_name, field_value, ...]
     * @paramType: [JsonColumn, JsonColumn, ...]
     * @return: JsonColumn
     */
    DEFINE_VECTORIZED_FN(json_object);

    /**
     * Build empty json object 
     * @param: 
     * @paramType: 
     * @return: JsonColumn
     */
    DEFINE_VECTORIZED_FN(json_object_empty);

    /**
     * Build json array from json values
     * @param: [json_object, ...]
     * @paramType: [JsonColumn, ...]
     * @return: JsonColumn
     */
    DEFINE_VECTORIZED_FN(json_array);

    /**
     * Build empty json array 
     * @param: 
     * @paramType: 
     * @return: JsonColumn
     */
    DEFINE_VECTORIZED_FN(json_array_empty);

    /**
     * Return number of elements in a JSON object/array
     * @param JSON, JSONPath
     * @return number of elements if it's object or array, otherwise return 1
     */
    DEFINE_VECTORIZED_FN(json_length);

    /**
     * Returns the keys from the top-level value of a JSON object as a JSON array
     * 
     */
    DEFINE_VECTORIZED_FN(json_keys);

    /**
     * Return json built from struct/map
     */
    DEFINE_VECTORIZED_FN(to_json);

    [[nodiscard]] static Status native_json_path_prepare(FunctionContext* context,
                                                         FunctionContext::FunctionStateScope scope);
    [[nodiscard]] static Status native_json_path_close(FunctionContext* context,
                                                       FunctionContext::FunctionStateScope scope);

    // extract_from_object extracts value from object according to the json path.
    // Now, we do not support complete functions of json path.
    [[nodiscard]] static Status extract_from_object(simdjson::ondemand::object& obj,
                                                    const std::vector<SimpleJsonPath>& jsonpath,
                                                    simdjson::ondemand::value* value) noexcept;

    [[nodiscard]] static Status parse_json_paths(const std::string& path_strings,
                                                 std::vector<SimpleJsonPath>* parsed_paths);

    // jsonpaths_to_string serializes json patsh to std::string. Setting sub_index to serializes paritially json paths.
    static std::string jsonpaths_to_string(const std::vector<SimpleJsonPath>& jsonpaths, size_t sub_index = -1);

    template <typename ValueType>
    static std::string_view to_json_string(ValueType&& val, size_t limit) {
        std::string_view sv = simdjson::to_json_string(std::forward<ValueType>(val));
        if (sv.size() > limit) {
            return sv.substr(0, limit);
        }
        return sv;
    }

private:
    template <LogicalType ResultType>
    [[nodiscard]] static StatusOr<ColumnPtr> _json_query_impl(FunctionContext* context, const Columns& columns);

    template <LogicalType RresultType>
    DEFINE_VECTORIZED_FN(_flat_json_query_impl);

    template <LogicalType RresultType>
    DEFINE_VECTORIZED_FN(_full_json_query_impl);

    /**
     * @param: [json_object, json_path]
     * @paramType: [JsonColumn, BinaryColumn]
     * @return: BooleanColumn
     */
    DEFINE_VECTORIZED_FN(_flat_json_exists);
    DEFINE_VECTORIZED_FN(_full_json_exists);

    /**
     * Return number of elements in a JSON object/array
     * @param JSON, JSONPath
     * @return number of elements if it's object or array, otherwise return 1
     */
    DEFINE_VECTORIZED_FN(_flat_json_length);
    DEFINE_VECTORIZED_FN(_full_json_length);

    /**
     * Parse string column as json column
     * @param: 
     * @paramType: 
     * @return: JsonColumn
     */
    DEFINE_VECTORIZED_FN(_string_json);

    /**
     * Convert json column to unescaped binary column with the first/last quote trimmed.
     * @param: 
     * @paramType: 
     * @return: BinaryColumn
     */
    DEFINE_VECTORIZED_FN(_json_string_unescaped);

    /**
     * Convert json column to int column
     * @param: [json_column]
     * @paramType: [JsonColumn]
     * @return: Int32Column
     */
    DEFINE_VECTORIZED_FN(_json_int);

    /**
     * Convert json column to double column
     * @param: [json_column]
     * @paramType: [JsonColumn]
     * @return: DoubleColumn
     */
    DEFINE_VECTORIZED_FN(_json_double);

    /**
     * @param: [json_object, json_path]
     * @paramType: [JsonColumn, BinaryColumn]
     * @return: JsonColumn
     */

    [[nodiscard]] static Status _get_parsed_paths(const std::vector<std::string>& path_exprs,
                                                  std::vector<SimpleJsonPath>* parsed_paths);
};

} // namespace starrocks
