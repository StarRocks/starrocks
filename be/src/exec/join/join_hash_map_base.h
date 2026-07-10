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

#include <memory>

#include "column/column.h"
#include "column/vectorized_fwd.h"

namespace starrocks {

class RuntimeState;
struct JoinHashTableItems;
struct HashTableProbeState;

// Runtime-polymorphic interface over the concrete JoinHashMap<LT, CT, MT> instantiations
// (and JoinHashMapForEmpty). Previously JoinHashTable stored an ~80-alternative
// std::variant and dispatched every operation through std::visit; that forced each
// std::visit site to instantiate the generic visitor for all ~80 heavy JoinHashMap<...>
// types in the caller TU, which dominated compile time (~1500s for join_hash_table.cpp).
//
// With this interface, JoinHashTable holds a single unique_ptr<JoinHashMapBase> and every
// operation is one ordinary virtual call. The per-type dispatch information (the vtable)
// is emitted where each concrete type is already explicitly instantiated (the
// join_hash_map_*_inst.cpp files), so the heavy instantiation stays there instead of being
// re-done at every call site. All dispatch is at chunk granularity, so the extra virtual
// call is negligible at runtime.
class JoinHashMapBase {
public:
    virtual ~JoinHashMapBase() = default;

    virtual void build_prepare(RuntimeState* state) = 0;
    virtual void probe_prepare(RuntimeState* state) = 0;
    virtual void build(RuntimeState* state) = 0;
    virtual void probe(RuntimeState* state, const Columns& key_columns, ChunkPtr* probe_chunk, ChunkPtr* chunk,
                       bool* has_remain) = 0;
    virtual void probe_remain(RuntimeState* state, ChunkPtr* chunk, bool* has_remain) = 0;

    // `lazy_output<is_remain>` is a compile-time-parameterized member template; a virtual
    // function cannot be a template, so the two instantiations are exposed as two entry points.
    virtual void lazy_output_for_remain(RuntimeState* state, ChunkPtr* probe_chunk, ChunkPtr* result_chunk) = 0;
    virtual void lazy_output_for_probe(RuntimeState* state, ChunkPtr* probe_chunk, ChunkPtr* result_chunk) = 0;

    // Construct a fresh instance of the SAME concrete type, bound to the given items/state.
    // Replaces the `make_unique<same-concrete-type>(...)` that clone_readable_table and
    // reset_probe_state used to do inside a std::visit lambda.
    virtual std::unique_ptr<JoinHashMapBase> clone(JoinHashTableItems* table_items,
                                                   HashTableProbeState* probe_state) const = 0;
};

// CRTP adapter that supplies the boilerplate glue (the two lazy_output entry points and
// clone) once for every concrete hash map, forwarding to the concrete type's own
// `lazy_output<bool>` template and constructor. Concrete classes still override the five
// core operations (build_prepare/probe_prepare/build/probe/probe_remain) directly.
template <class Derived>
class JoinHashMapAdapter : public JoinHashMapBase {
public:
    void lazy_output_for_remain(RuntimeState* state, ChunkPtr* probe_chunk, ChunkPtr* result_chunk) final {
        static_cast<Derived*>(this)->template lazy_output<true>(state, probe_chunk, result_chunk);
    }
    void lazy_output_for_probe(RuntimeState* state, ChunkPtr* probe_chunk, ChunkPtr* result_chunk) final {
        static_cast<Derived*>(this)->template lazy_output<false>(state, probe_chunk, result_chunk);
    }
    std::unique_ptr<JoinHashMapBase> clone(JoinHashTableItems* table_items,
                                           HashTableProbeState* probe_state) const final {
        return std::make_unique<Derived>(table_items, probe_state);
    }
};

} // namespace starrocks
