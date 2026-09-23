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

// Runtime factory table for JoinHashMap. Each join_hash_map_*_inst.cpp registers, at static
// init, a `&make_join_hash_map<LT,CT,MT>` pointer keyed by (key-constructor-unary, method-unary).
// JoinHashTable::build() then just looks the pointer up and calls it, so build.cpp never names a
// concrete JoinHashMap<LT,CT,MT> and does not pay the ~8s-per-combo instantiation cost that a
// compile-time dispatch incurs in a TU where those types are not otherwise instantiated.

#include <unordered_map>

#include "exec/join/join_hash_map.h"

namespace starrocks {

// The join_hash_map_*_inst.cpp TUs self-register their factories via static initializers, but
// nothing else references them now that build() dispatches through JoinHashMapBase + this table,
// so the linker would drop those objects. Reference one anchor from each below to force-link them.
extern int join_hash_map_inst_anchor_bucket_chained;
extern int join_hash_map_inst_anchor_linear_chained;
extern int join_hash_map_inst_anchor_linear_chained_set;
extern int join_hash_map_inst_anchor_asof;
extern int join_hash_map_inst_anchor_mapping;

namespace {
[[gnu::used]] int* const g_join_hash_map_inst_link_anchors[] = {
        &join_hash_map_inst_anchor_bucket_chained,
        &join_hash_map_inst_anchor_linear_chained,
        &join_hash_map_inst_anchor_linear_chained_set,
        &join_hash_map_inst_anchor_asof,
        &join_hash_map_inst_anchor_mapping,
};
} // namespace

namespace {
uint32_t factory_key(JoinKeyConstructorUnaryType cut, JoinHashMapMethodUnaryType mut) {
    return (static_cast<uint32_t>(cut) << 16) | static_cast<uint32_t>(mut);
}

std::unordered_map<uint32_t, JoinHashMapFactoryFn>& factory_registry() {
    static auto* registry = new std::unordered_map<uint32_t, JoinHashMapFactoryFn>();
    return *registry;
}
} // namespace

void register_join_hash_map_factory(JoinKeyConstructorUnaryType cut, JoinHashMapMethodUnaryType mut,
                                    JoinHashMapFactoryFn fn) {
    factory_registry()[factory_key(cut, mut)] = fn;
}

JoinHashMapFactoryFn find_join_hash_map_factory(JoinKeyConstructorUnaryType cut, JoinHashMapMethodUnaryType mut) {
    (void)g_join_hash_map_inst_link_anchors;
    const auto& registry = factory_registry();
    const auto it = registry.find(factory_key(cut, mut));
    return it == registry.end() ? nullptr : it->second;
}

} // namespace starrocks
