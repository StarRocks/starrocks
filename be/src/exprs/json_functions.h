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

#include <simdjson.h>
#include <velocypack/vpack.h>

#include "base/string/faststring.h"
#include "common/status.h"
#include "exprs/function_context.h"
#include "exprs/function_helper.h"
#include "types/logical_type.h"

namespace starrocks {

// Forward declarations
struct JsonPath;
template <LogicalType LT>
class ColumnBuilder;
namespace vpack = arangodb::velocypack;

// Pre-planned move step for fast simdjson path. Owned by NativeJsonState (FRAGMENT_LOCAL),
// shared read-only between cloned drivers.
struct JsonMoveStep {
    enum class Kind : uint8_t { Field, ArrayIndex };
    Kind kind;
    std::string field;  // owned; non-empty only when kind==Field
    int32_t index = -1; // valid only when kind==ArrayIndex
};

enum class JsonPathShape : uint8_t {
    Unsupported, // wildcard / slice / unrecognized -> legacy fallback
    SimpleFlat,  // only Field steps
    HasIndex,    // Field + ArrayIndex steps (chained selectors OK)
};

// Mutable scratch shared by calls on one OS thread. Columns own copies of extracted values.
struct JsonGetThreadState {
    simdjson::ondemand::parser parser; // reused across calls on this OS thread
    faststring padded_scratch;         // input copy + SIMDJSON_PADDING zero tail
    faststring unescape_scratch;       // backs value_get_string_safe outputs
    faststring key_scratch;            // backs field_unescaped_key_safe during object descent
    vpack::Builder leaf_builder;       // .clear()-ed before each leaf conversion
};

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
    DEFINE_VECTORIZED_FN(get_json_bool); // (VARCHAR, VARCHAR) -> BOOLEAN
    DEFINE_VECTORIZED_FN(json_query_many_from_string);
    DEFINE_VECTORIZED_FN(json_query_from_string); // (VARCHAR, VARCHAR) -> JSON, FE-fusion target

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

    static Status json_query_many_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    static Status json_query_many_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

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

    // Pre-decompose a constant JsonPath into flat moves + classify shape.
    // Sets state->fast_shape and state->fast_moves. Called once at FRAGMENT_LOCAL prepare.
    static void _plan_fast_moves(const JsonPath& path, struct NativeJsonState* state);

    enum class ExtractResult : uint8_t {
        Handled,       // value or NULL appended; row complete
        FallbackRow,   // caller should invoke _fallback_extract_one for this row
        FallbackBatch, // reserved for future; v4 does not use it
    };

    // Per-row fast extraction. Returns Handled when the row produced output via the simdjson path,
    // FallbackRow for bare-scalar / empty / whitespace-only inputs that need legacy parse_json_or_string semantics.
    template <LogicalType ResultType>
    static ExtractResult _fused_extract_one(const Slice& raw_json, const struct NativeJsonState* fragment_state,
                                            JsonGetThreadState* thread_state, ColumnBuilder<ResultType>& out);

    // Per-row fallback that mirrors legacy parse_json_or_string + JsonPath::extract + cast_vpjson_to.
    // Always appends exactly one row (value or NULL). Status::OK is the only return.
    template <LogicalType ResultType>
    static Status _fallback_extract_one(const Slice& raw_json, const struct NativeJsonState* fragment_state,
                                        JsonGetThreadState* thread_state, ColumnBuilder<ResultType>& out);

    // Drives the fully-fused per-row loop. Caller has already verified fast-path eligibility.
    template <LogicalType ResultType>
    static StatusOr<ColumnPtr> _fused_get_json_value(FunctionContext* context, const Columns& columns,
                                                     const struct NativeJsonState* fstate, JsonGetThreadState* tstate);

    friend void set_json_fast_path_disabled_for_test(FunctionContext*, bool);

    /**
     * @param: [json_object, json_path]
     * @paramType: [JsonColumn, BinaryColumn]
     * @return: JsonColumn
     */

    // Helper function to check if target JSON contains candidate JSON
    static bool json_value_contains(JsonValue* target, JsonValue* candidate);
    static bool is_slice_scalar_type(const vpack::Slice& slice);
};

// Test-only: force the fused fast path on/off for the given FunctionContext after
// FRAGMENT_LOCAL prepare. Differential tests and the json-extract benchmark use this
// to feed identical inputs through both paths and compare results.
void set_json_fast_path_disabled_for_test(FunctionContext* ctx, bool disabled);

} // namespace starrocks
