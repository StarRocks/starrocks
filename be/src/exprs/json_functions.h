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

#include <velocypack/vpack.h>

#include "common/status.h"
#include "exprs/function_context.h"
#include "exprs/function_helper.h"
#include "types/logical_type.h"

namespace starrocks {

// Forward declarations
struct JsonPath;

class JsonFunctions {
public:
    /**
     * @param: [json_string, tagged_value]
     * @paramType: [BinaryColumn, BinaryColumn]
     * @return: type column
     */
    DEFINE_VECTORIZED_FN(get_json_int);
    DEFINE_VECTORIZED_FN(get_json_bigint);
    DEFINE_VECTORIZED_FN(get_json_double);
    DEFINE_VECTORIZED_FN(get_json_string);
    DEFINE_VECTORIZED_FN(get_json_scalar_string);

    /**
     * @param: [json, tagged_value]
     * @paramType: [JsonColumn, BinaryColumn]
     * @return: type column
     */
    DEFINE_VECTORIZED_FN(get_native_json_bool);
    DEFINE_VECTORIZED_FN(get_native_json_int);
    DEFINE_VECTORIZED_FN(get_native_json_bigint);
    DEFINE_VECTORIZED_FN(get_native_json_double);
    DEFINE_VECTORIZED_FN(get_native_json_string);
    DEFINE_VECTORIZED_FN(get_native_json_scalar_string);
    DEFINE_VECTORIZED_FN(json_query);

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
    DEFINE_VECTORIZED_FN(json_pretty);

    /**
     * @param: [json_column]
     * @paramType: [JsonColumn]
     * @return: BinaryColumn
     */
    DEFINE_VECTORIZED_FN(json_string);

    /**
     * @param: [json_object, json_path]
     * @paramType: [JsonColumn, BinaryColumn]
     * @return: BooleanColumn
     */
    DEFINE_VECTORIZED_FN(json_exists);

    /**
     * @param: [json_object, json_value]
     * @paramType: [JsonColumn, JsonColumn]
     * @return: BooleanColumn
     */
    DEFINE_VECTORIZED_FN(json_contains);

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
     * Remove data from a JSON document at one or more specified JSON paths
     * @param JSON, JSONPath, [JSONPath, ...]
     * @return JSON with specified paths removed
     */
    DEFINE_VECTORIZED_FN(json_remove);

    /**
     * Inserts or updates data in a JSON document at one or more specified JSON paths
     * @param JSON, JSONPath, Value, [JSONPath, Value, ...]
     * @return Modified JSON
     */
    DEFINE_VECTORIZED_FN(json_set);

    /**
     * Determine if a JSON value is a scalar value
     * @param JSON
     * @return true if the JSON value is a scalar (not an object or array), false otherwise
     */
    DEFINE_VECTORIZED_FN(is_json_scalar);

    /**
     * Return json built from struct/map
     */
    DEFINE_VECTORIZED_FN(to_json);

    static Status native_json_path_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    static Status native_json_path_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

private:
    template <LogicalType ResultType>
    static StatusOr<ColumnPtr> _json_query_impl(FunctionContext* context, const Columns& columns);
    static StatusOr<ColumnPtr> _json_query_scalar_impl(FunctionContext* context, const Columns& columns);

    template <LogicalType RresultType>
    static StatusOr<ColumnPtr> _flat_json_query_impl(FunctionContext* context, const Columns& columns,
                                                     bool scalar_type_only = false);

    template <LogicalType RresultType>
    static StatusOr<ColumnPtr> _full_json_query_impl(FunctionContext* context, const Columns& columns,
                                                     bool scalar_type_only = false);

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
     * Returns the keys from the top-level value of a JSON object as a JSON array
     */
    DEFINE_VECTORIZED_FN(_json_keys_without_path);
    DEFINE_VECTORIZED_FN(_flat_json_keys_with_path);
    DEFINE_VECTORIZED_FN(_full_json_keys_with_path);

    template <LogicalType RresultType>
    DEFINE_VECTORIZED_FN(_get_json_value);
    DEFINE_VECTORIZED_FN(_get_json_scalar_value);

    /**
     * @param: [json_object, json_path]
     * @paramType: [JsonColumn, BinaryColumn]
     * @return: JsonColumn
     */

    // Helper function to check if target JSON contains candidate JSON
    static bool json_value_contains(JsonValue* target, JsonValue* candidate);
    static bool is_slice_scalar_type(const vpack::Slice& slice);
};

} // namespace starrocks
