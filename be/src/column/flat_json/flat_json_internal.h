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

#include <cstddef>
#include <cstdint>
#include <string_view>

#include "base/phmap/phmap.h"
#include "column/flat_json/json_flat_path.h"
#include "column/nullable_column.h"
#include "types/logical_type.h"

namespace starrocks {

class Column;

namespace flat_json {

using JsonFlatExtractFunc = void (*)(const vpack::Slice* json, NullableColumn* result);
using JsonFlatMergeFunc = void (*)(vpack::Builder* builder, const std::string_view& name, const Column* src,
                                   size_t idx);

extern const uint8_t JSON_BASE_TYPE_BITS;
extern const uint8_t JSON_BIGINT_TYPE_BITS;
extern const FlatJsonHashMap<vpack::ValueType, uint8_t> JSON_TYPE_BITS;
extern const phmap::flat_hash_set<vpack::ValueType> JSON_BASE_TYPE;
extern const FlatJsonHashMap<uint8_t, LogicalType> JSON_BITS_TO_LOGICAL_TYPE;
extern const FlatJsonHashMap<LogicalType, uint8_t> LOGICAL_TYPE_TO_JSON_BITS;
extern const FlatJsonHashMap<LogicalType, JsonFlatExtractFunc> JSON_EXTRACT_FUNC;
extern const FlatJsonHashMap<LogicalType, JsonFlatMergeFunc> JSON_MERGE_FUNC;

uint8_t get_compatibility_type(vpack::ValueType type1, uint8_t type2);

// What JsonPathDeriver::_visit_json_paths collects for a path, gathered from the values of a JSON-typed
// sub-column. Kept in the segment (JsonValueStatsPB) so a later derivation from metadata can type the path by
// its values instead of leaving it JSON.
struct JsonValueStats {
    uint8_t json_type = 31; // JSON_NULL_TYPE_BITS
    // non-NULL rows
    uint64_t values = 0;
    // values of a base JSON type (JSON_BASE_TYPE)
    uint64_t base_type_count = 0;
    uint64_t max_uint = 0;
    int64_t min_int = 0;

    // `column` is a JSON column, nullable or not.
    void update(const Column& column);
    void merge(const JsonValueStats& other);
};

} // namespace flat_json
} // namespace starrocks
