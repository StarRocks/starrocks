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

#include "column/column.h"
#include "common/global_types.h"
#include "common/status.h"
#include "storage/primitive/range.h"

namespace starrocks::parquet {

class ColumnMaterializer;

// Expression-facing facade over ColumnMaterializer.  Provides on-demand
// single-slot materialization to the scanner-local predicate evaluator.
//
// Lifetime: created per get_next() range/batch, destroyed before the
// scanner emits a fully materialized chunk.  No lazy state leaks out.
class LazyMaterializationContext {
public:
    LazyMaterializationContext(ColumnMaterializer* materializer, const Range<uint64_t>& range, const Filter* filter)
            : _materializer(materializer), _range(range), _filter(filter) {}

    bool has_slot(SlotId slot_id) const { return _materializer->get_slot_cache(slot_id) != nullptr; }

    Status ensure_slot_materialized(SlotId slot_id) {
        return _materializer->materialize_slot(slot_id, _range, _filter);
    }

    ColumnPtr get_column(SlotId slot_id) const {
        auto* entry = _materializer->get_slot_cache(slot_id);
        return entry ? entry->values : nullptr;
    }

private:
    ColumnMaterializer* _materializer;
    Range<uint64_t> _range;
    const Filter* _filter;
};

} // namespace starrocks::parquet
