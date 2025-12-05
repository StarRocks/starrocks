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

#include "exec/join/join_hash_map.h"
#include "exec/join/join_hash_map.hpp"
#include "exec/join/join_hash_map_method.hpp"

namespace starrocks {

#define DEF_JOIN_MAP(LT, CT, MT) JoinHashMap<LT, JoinKeyConstructorType::CT, JoinHashMapMethodType::MT>
#define INSTANTIATE_JOIN_HASH_MAP(LT, CT, MT)                                                       \
    template class DEF_JOIN_MAP(LT, CT, MT);                                                        \
    template void DEF_JOIN_MAP(LT, CT, MT)::lazy_output<true>(RuntimeState*, ChunkPtr*, ChunkPtr*); \
    template void DEF_JOIN_MAP(LT, CT, MT)::lazy_output<false>(RuntimeState*, ChunkPtr*, ChunkPtr*);

INSTANTIATE_JOIN_HASH_MAP(TYPE_INT, ONE_KEY, LINEAR_CHAINED_SET)
INSTANTIATE_JOIN_HASH_MAP(TYPE_BIGINT, ONE_KEY, LINEAR_CHAINED_SET)
INSTANTIATE_JOIN_HASH_MAP(TYPE_LARGEINT, ONE_KEY, LINEAR_CHAINED_SET)
INSTANTIATE_JOIN_HASH_MAP(TYPE_FLOAT, ONE_KEY, LINEAR_CHAINED_SET)
INSTANTIATE_JOIN_HASH_MAP(TYPE_DOUBLE, ONE_KEY, LINEAR_CHAINED_SET)
INSTANTIATE_JOIN_HASH_MAP(TYPE_DATE, ONE_KEY, LINEAR_CHAINED_SET)
INSTANTIATE_JOIN_HASH_MAP(TYPE_DATETIME, ONE_KEY, LINEAR_CHAINED_SET)
INSTANTIATE_JOIN_HASH_MAP(TYPE_DECIMALV2, ONE_KEY, LINEAR_CHAINED_SET)
INSTANTIATE_JOIN_HASH_MAP(TYPE_DECIMAL32, ONE_KEY, LINEAR_CHAINED_SET)
INSTANTIATE_JOIN_HASH_MAP(TYPE_DECIMAL64, ONE_KEY, LINEAR_CHAINED_SET)
INSTANTIATE_JOIN_HASH_MAP(TYPE_DECIMAL128, ONE_KEY, LINEAR_CHAINED_SET)
INSTANTIATE_JOIN_HASH_MAP(TYPE_VARCHAR, ONE_KEY, LINEAR_CHAINED_SET)
INSTANTIATE_JOIN_HASH_MAP(TYPE_INT, SERIALIZED_FIXED_SIZE, LINEAR_CHAINED_SET)
INSTANTIATE_JOIN_HASH_MAP(TYPE_BIGINT, SERIALIZED_FIXED_SIZE, LINEAR_CHAINED_SET)
INSTANTIATE_JOIN_HASH_MAP(TYPE_LARGEINT, SERIALIZED_FIXED_SIZE, LINEAR_CHAINED_SET)
INSTANTIATE_JOIN_HASH_MAP(TYPE_VARCHAR, SERIALIZED, LINEAR_CHAINED_SET)

} // namespace starrocks