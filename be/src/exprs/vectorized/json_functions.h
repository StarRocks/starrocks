// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "common/compiler_util.h"
DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wclass-memaccess")
#include <rapidjson/document.h>

#include <utility>
DIAGNOSTIC_POP

#include "column/column_builder.h"
#include "exprs/vectorized/function_helper.h"

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

struct JsonPath {
    std::string key; // key of a json object
    int idx;         // array index of a json array, -1 means not set, -2 means *
    bool is_valid;   // true if the path is successfully parsed

    JsonPath(std::string key_, int idx_, bool is_valid_) : key(std::move(key_)), idx(idx_), is_valid(is_valid_) {}

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
     * The `document` parameter must be has parsed.
     * return Value Is Array object
     */
    static rapidjson::Value* get_json_array_from_parsed_json(const std::vector<JsonPath>& parsed_paths,
                                                             rapidjson::Value* document,
                                                             rapidjson::Document::AllocatorType& mem_allocator);

    // this is only for test, it will parse the json path inside,
    // so that we can easily pass a json path as string.
    static rapidjson::Value* get_json_array_from_parsed_json(const std::string& jsonpath, rapidjson::Value* document,
                                                             rapidjson::Document::AllocatorType& mem_allocator);

    static rapidjson::Value* get_json_object_from_parsed_json(const std::vector<JsonPath>& parsed_paths,
                                                              rapidjson::Value* document,
                                                              rapidjson::Document::AllocatorType& mem_allocator);

    static void parse_json_paths(const std::string& path_strings, std::vector<JsonPath>* parsed_paths);

    static std::string get_raw_json_string(const rapidjson::Value& value);

private:
    template <PrimitiveType primitive_type>
    static ColumnPtr iterate_rows(FunctionContext* context, const Columns& columns);

    static rapidjson::Value* get_json_object(FunctionContext* context, const std::string& json_string,
                                             const std::string& path_string, const JsonFunctionType& fntype,
                                             rapidjson::Document* document);

    static rapidjson::Value* match_value(const std::vector<JsonPath>& parsed_paths, rapidjson::Value* document,
                                         rapidjson::Document::AllocatorType& mem_allocator,
                                         bool is_insert_null = false);

    static void get_parsed_paths(const std::vector<std::string>& path_exprs, std::vector<JsonPath>* parsed_paths);
};

} // namespace vectorized
} // namespace starrocks
