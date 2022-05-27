// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "common/compiler_util.h"
DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wclass-memaccess")

#include <utility>
DIAGNOSTIC_POP

#include "column/column_builder.h"
#include "exprs/vectorized/function_helper.h"
#include "exprs/vectorized/jsonpath.h"
#include "simdjson.h"
#include "velocypack/vpack.h"

namespace starrocks {
namespace vectorized {

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

    /**
     * @param: [json_object, json_path]
     * @paramType: [JsonColumn, BinaryColumn]
     * @return: JsonColumn
     */
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

    static Status native_json_path_prepare(starrocks_udf::FunctionContext* context,
                                           starrocks_udf::FunctionContext::FunctionStateScope scope);
    static Status native_json_path_close(starrocks_udf::FunctionContext* context,
                                         starrocks_udf::FunctionContext::FunctionStateScope scope);

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
#define APPEND_NULL_AND_RETURN_IF_ERROR(builder, err) \
    do {                                              \
        const simdjson::error_code& e = err;          \
        if (UNLIKELY(e)) {                            \
            builder.append_null();                    \
            return;                                   \
        }                                             \
    } while (false)

    static Status _get_parsed_paths(const std::vector<std::string>& path_exprs,
                                    std::vector<SimpleJsonPath>* parsed_paths);

    /* Following functions are only used in test. */
    template <PrimitiveType primitive_type>
    static ColumnPtr _iterate_rows(FunctionContext* context, const Columns& columns);

    template <PrimitiveType primitive_type>
    static void _build_column(ColumnBuilder<primitive_type>& result, simdjson::ondemand::value& value);
};

} // namespace vectorized
} // namespace starrocks
