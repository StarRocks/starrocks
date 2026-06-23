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
#include "column/nullable_column.h"
#include "types/logical_type.h"
#include "util/json_flat_path.h"

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

} // namespace flat_json
} // namespace starrocks
