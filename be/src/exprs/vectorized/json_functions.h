// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <re2/re2.h>
#include <simdjson.h>
#include <velocypack/vpack.h>

#include <utility>

#include "column/column_builder.h"
#include "common/compiler_util.h"
#include "exprs/vectorized/function_helper.h"
#include "exprs/vectorized/jsonpath.h"
#include "udf/udf.h"

namespace starrocks::vectorized {

enum JsonFunctionType {
    JSON_FUN_INT = 0,
    JSON_FUN_DOUBLE,
    JSON_FUN_STRING,

    JSON_FUN_UNKOWN //The last
};

template <PrimitiveType primitive_type>
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
    static Status json_path_prepare(starrocks_udf::FunctionContext* context,
                                    starrocks_udf::FunctionContext::FunctionStateScope scope);
    static Status json_path_close(starrocks_udf::FunctionContext* context,
                                  starrocks_udf::FunctionContext::FunctionStateScope scope);

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

    static Status native_json_path_prepare(starrocks_udf::FunctionContext* context,
                                           starrocks_udf::FunctionContext::FunctionStateScope scope);
    static Status native_json_path_close(starrocks_udf::FunctionContext* context,
                                         starrocks_udf::FunctionContext::FunctionStateScope scope);
    /**
     * Return json built from struct/map
     */
    DEFINE_VECTORIZED_FN(to_json);

    // extract_from_object extracts value from object according to the json path.
    // Now, we do not support complete functions of json path.
    static Status extract_from_object(simdjson::ondemand::object& obj, const std::vector<SimpleJsonPath>& jsonpath,
                                      simdjson::ondemand::value* value) noexcept;

    static void parse_json_paths(const std::string& path_strings, std::vector<SimpleJsonPath>* parsed_paths);

    template <typename ValueType>
    static std::string_view to_json_string(ValueType&& val, size_t limit) {
        std::string_view sv = simdjson::to_json_string(std::forward<ValueType>(val));
        if (sv.size() > limit) {
            return sv.substr(0, limit);
        }
        return sv;
    }

private:
    template <PrimitiveType ResultType>
    static StatusOr<ColumnPtr> _json_query_impl(starrocks_udf::FunctionContext* context, const Columns& columns);

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

    static Status _get_parsed_paths(const std::vector<std::string>& path_exprs,
                                    std::vector<SimpleJsonPath>* parsed_paths);
};

} // namespace starrocks::vectorized
