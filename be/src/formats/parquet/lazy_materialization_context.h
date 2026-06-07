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
#include "common/global_types.h"
#include "common/status.h"
#include "storage/primitive/range.h"

namespace starrocks::parquet {

class ColumnMaterializer;

// Expression-facing facade over ColumnMaterializer.  Scoped to a single
// get_next() iteration: created before predicate evaluation, destroyed
// before the scanner emits a Chunk.  Lazy state must not escape the scanner.
//
// When expression evaluation reaches a missing slot, the context can
// materialize it on demand through ColumnMaterializer.  This is the
// interface that Phase 6 (expression trigger) will call.
class LazyMaterializationContext {
public:
    LazyMaterializationContext(ColumnMaterializer& materializer, const Range<uint64_t>& range, const Filter* filter,
                               ChunkPtr& active_chunk)
            : _materializer(materializer), _range(range), _filter(filter), _active_chunk(active_chunk) {}

    LazyMaterializationContext(const LazyMaterializationContext&) = delete;
    LazyMaterializationContext& operator=(const LazyMaterializationContext&) = delete;

    // Whether the slot is currently available (in active_chunk or cached).
    bool has_slot(SlotId slot_id) const;

    // Whether the context can materialize this slot on demand.
    // Returns true for slots in the lazy column sets or variant hidden sources.
    bool can_materialize(SlotId slot_id) const;

    // Materialize a lazy slot, reading through its ColumnReader if needed.
    // No-op if the slot is already cached.  Appends the column to active_chunk.
    Status materialize_slot(SlotId slot_id);

    // Return the column for a slot, materializing it on demand first.
    // Returns nullptr if the slot cannot be resolved.
    ColumnPtr get_column(SlotId slot_id);

    const Range<uint64_t>& range() const { return _range; }
    const Filter* filter() const { return _filter; }
    ChunkPtr& active_chunk() { return _active_chunk; }

private:
    ColumnMaterializer& _materializer;
    Range<uint64_t> _range;
    const Filter* _filter;
    ChunkPtr& _active_chunk;
};

} // namespace starrocks::parquet
