// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exprs/json_functions.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "common/compiler_util.h"
DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wclass-memaccess")
#include <rapidjson/document.h>
DIAGNOSTIC_POP

#include "runtime/string_value.h"

namespace starrocks {

// Our new vectorized query executor is more powerful and stable than old query executor,
// The executor query executor related codes could be deleted safely.
// TODO: Remove old query executor related codes before 2021-09-30

enum JsonFunctionType {
    JSON_FUN_INT = 0,
    JSON_FUN_DOUBLE,
    JSON_FUN_STRING,

    JSON_FUN_UNKOWN //The last
};

class Expr;
class OpcodeRegistry;
class TupleRow;

struct JsonPath {
    std::string key; // key of a json object
    int idx;         // array index of a json array, -1 means not set, -2 means *
    bool is_valid;   // true if the path is successfully parsed

    JsonPath(const std::string& key_, int idx_, bool is_valid_) : key(key_), idx(idx_), is_valid(is_valid_) {}

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
    static void init();
    static starrocks_udf::IntVal get_json_int(starrocks_udf::FunctionContext* context,
                                              const starrocks_udf::StringVal& json_str,
                                              const starrocks_udf::StringVal& path);
    static starrocks_udf::StringVal get_json_string(starrocks_udf::FunctionContext* context,
                                                    const starrocks_udf::StringVal& json_str,
                                                    const starrocks_udf::StringVal& path);
    static starrocks_udf::DoubleVal get_json_double(starrocks_udf::FunctionContext* context,
                                                    const starrocks_udf::StringVal& json_str,
                                                    const starrocks_udf::StringVal& path);

    static rapidjson::Value* get_json_object(FunctionContext* context, const std::string& json_string,
                                             const std::string& path_string, const JsonFunctionType& fntype,
                                             rapidjson::Document* document);

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

    static void json_path_prepare(starrocks_udf::FunctionContext*, starrocks_udf::FunctionContext::FunctionStateScope);

    static void json_path_close(starrocks_udf::FunctionContext*, starrocks_udf::FunctionContext::FunctionStateScope);

    static void parse_json_paths(const std::string& path_strings, std::vector<JsonPath>* parsed_paths);

private:
    static rapidjson::Value* match_value(const std::vector<JsonPath>& parsed_paths, rapidjson::Value* document,
                                         rapidjson::Document::AllocatorType& mem_allocator,
                                         bool is_insert_null = false);
    static void get_parsed_paths(const std::vector<std::string>& path_exprs, std::vector<JsonPath>* parsed_paths);
};
} // namespace starrocks
