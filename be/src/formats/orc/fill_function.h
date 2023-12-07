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

#include <exception>
#include <orc/OrcFile.hh>
#include <set>
#include <unordered_map>
#include <utility>

#include "cctz/civil_time.h"
#include "cctz/time_zone.h"
#include "column/vectorized_fwd.h"
#include "formats/orc/orc_mapping.h"
#include "formats/orc/utils.h"
#include "gen_cpp/orc_proto.pb.h"
#include "runtime/primitive_type.h"
#include "runtime/types.h"
#include "types/date_value.h"
#include "types/timestamp_value.h"

namespace starrocks::vectorized {

using FillColumnFunction = void (*)(orc::ColumnVectorBatch* cvb, ColumnPtr& col, size_t from, size_t size,
                                    const TypeDescriptor& type_desc, const OrcMappingPtr& mapping, void* ctx);

extern const std::unordered_map<orc::TypeKind, PrimitiveType> g_orc_starrocks_primitive_type_mapping;
extern const std::set<PrimitiveType> g_starrocks_int_type;
extern const std::set<orc::TypeKind> g_orc_decimal_type;
extern const std::set<PrimitiveType> g_starrocks_decimal_type;

template <typename T>
struct DecimalVectorBatchSelector {
    using Type = std::conditional_t<
            std::is_same_v<T, int64_t>, orc::Decimal64VectorBatch,
            std::conditional_t<std::is_same_v<T, starrocks::vectorized::int128_t>, orc::Decimal128VectorBatch, void>>;
};

class FunctionsMap {
public:
    static FunctionsMap* instance() {
        static FunctionsMap map;
        return &map;
    }

    const FillColumnFunction& get_func(PrimitiveType type) const { return _funcs[type]; }

    const FillColumnFunction& get_nullable_func(PrimitiveType type) const { return _nullable_funcs[type]; }

private:
    FunctionsMap();

    std::array<FillColumnFunction, 64> _funcs;
    std::array<FillColumnFunction, 64> _nullable_funcs;
};

const FillColumnFunction& find_fill_func(PrimitiveType type, bool nullable);

} // namespace starrocks::vectorized
