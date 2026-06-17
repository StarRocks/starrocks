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
#include <unordered_set>

#include "column/chunk.h"
#include "column/column.h"
#include "column/vectorized_fwd.h"
#include "common/global_types.h"
#include "common/status.h"
#include "common/statusor.h"
#include "storage/primitive/range.h"

namespace starrocks::parquet {

class ColumnMaterializer;
class VariantProjectionHandler;

// Expression-facing facade over ColumnMaterializer.  Scoped to a single
// get_next() iteration: created before predicate evaluation, destroyed
// before the scanner emits a Chunk.  Lazy state must not escape the scanner.
//
// Implements MissingColumnProvider so it can be attached to active_chunk
// during predicate evaluation.  When ColumnRef encounters a missing slot
// it calls can_provide() / provide() on this context, which loads the
// column on demand through ColumnMaterializer or VariantProjectionHandler.
//
// Column state invariant:
//   provide() / materialize_slot() always calls finalize_lazy_state()
//   after read_range().  The column returned to the caller is LOGICAL
//   (e.g. LowCardDictColumn, BinaryColumn), never PHYSICAL (dict codes
//   or intermediate raw values).  This guarantees expression evaluators
//   always receive StarRocks-native logical columns.
//
//   Because materialize_slot() finalizes the column, the lazy column
//   in _read_chunk is already LOGICAL when read_lazy_columns() moves it
//   into active_chunk.  emit_physical_columns() must therefore bypass
//   fill_dst_column() for these slots (see _logical_slot_ids in
//   ColumnMaterializer).
class LazyMaterializationContext : public MissingColumnProvider {
public:
    // variant may be nullptr when no variant columns are present.
    LazyMaterializationContext(ColumnMaterializer& materializer, VariantProjectionHandler* variant,
                               const Range<uint64_t>& range, const Filter* filter, ChunkPtr& active_chunk)
            : _materializer(materializer),
              _variant(variant),
              _range(range),
              _filter(filter),
              _active_chunk(active_chunk) {}

    LazyMaterializationContext(const LazyMaterializationContext&) = delete;
    LazyMaterializationContext& operator=(const LazyMaterializationContext&) = delete;

    // Whether the slot is currently available (in active_chunk or cached).
    bool has_slot(SlotId slot_id) const;

    // Whether the context can materialize this slot on demand.
    // Returns true for slots in the lazy column sets or variant hidden sources.
    bool can_materialize(SlotId slot_id) const;

    // Materialize a lazy slot, reading through its ColumnReader if needed.
    // No-op if the slot is already cached.
    Status materialize_slot(SlotId slot_id);

    // Return the column for a slot, materializing it on demand first.
    // Returns nullptr if the slot cannot be resolved.
    ColumnPtr get_column(SlotId slot_id);

    // MissingColumnProvider implementation: called by ColumnRef when a slot
    // is absent from the active chunk during predicate evaluation.
    bool can_provide(SlotId slot_id) const override { return can_materialize(slot_id); }
    StatusOr<ColumnPtr> provide(SlotId slot_id) override;

    const Range<uint64_t>& range() const { return _range; }
    const Filter* filter() const { return _filter; }
    ChunkPtr& active_chunk() { return _active_chunk; }

private:
    ColumnMaterializer& _materializer;
    VariantProjectionHandler* _variant;
    Range<uint64_t> _range;
    const Filter* _filter;
    ChunkPtr& _active_chunk;
    // Tracks slots triggered for the first time this iteration, to avoid
    // double-counting in parquet_lazy_slot_triggered.
    // Cleared implicitly when the context is destroyed at end of get_next().
    std::unordered_set<SlotId> _triggered_slots;
};

} // namespace starrocks::parquet
