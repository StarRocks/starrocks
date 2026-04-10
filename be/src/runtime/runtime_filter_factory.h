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

#include "column/vectorized_fwd.h"
#include "runtime/runtime_filter.h"
#include "types/logical_type.h"

namespace starrocks {
class ObjectPool;

class RuntimeFilterFactory {
public:
    static RuntimeFilter* create_empty_filter(ObjectPool* pool, LogicalType type, int8_t join_mode);
    static RuntimeFilter* create_bloom_filter(ObjectPool* pool, LogicalType type, int8_t join_mode);
    static RuntimeFilter* create_bitset_filter(ObjectPool* pool, LogicalType type, int8_t join_mode);
    static RuntimeFilter* create_in_filter(ObjectPool* pool, LogicalType type, int8_t join_mode);
    static RuntimeFilter* create_filter(ObjectPool* pool, RuntimeFilterSerializeType rf_type, LogicalType ltype,
                                        int8_t join_mode);
    static RuntimeFilter* create_join_filter(ObjectPool* pool, LogicalType type, int8_t join_mode,
                                             const Columns& columns, size_t column_offset, size_t row_count);
    static RuntimeFilter* to_empty_filter(ObjectPool* pool, RuntimeFilter* rf);
};

} // namespace starrocks
