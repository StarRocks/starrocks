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
#include "common/statusor.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/runtime_filter.h"

namespace starrocks {
class ObjectPool;
class RuntimeState;

struct RuntimeFilterSkewMaterial {
    LogicalType build_type;
    bool eq_null;
    ColumnPtr key_column;
};

class RuntimeFilterSerde {
public:
    static size_t max_size(const RuntimeState* state, const RuntimeFilter* rf);
    static size_t max_size(int rf_version, const RuntimeFilter* rf);

    static size_t serialize(const RuntimeState* state, const RuntimeFilter* rf, uint8_t* data);
    static size_t serialize(int rf_version, const RuntimeFilter* rf, uint8_t* data);

    static int deserialize(ObjectPool* pool, RuntimeFilter** rf, const uint8_t* data, size_t size);

    static size_t skew_max_size(const ColumnPtr& column);
    static StatusOr<size_t> skew_serialize(const ColumnPtr& column, bool eq_null, uint8_t* data);
    static StatusOr<int> skew_deserialize(ObjectPool* pool, RuntimeFilterSkewMaterial** material, const uint8_t* data,
                                          size_t size, const PTypeDesc& ptype);
};

} // namespace starrocks
