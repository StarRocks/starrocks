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

#include "formats/parquet/lazy_materialization_context.h"

#include <algorithm>

#include "column/chunk.h"
#include "formats/parquet/column_materializer.h"
#include "formats/parquet/variant_projection.h"

namespace starrocks::parquet {

bool LazyMaterializationContext::has_slot(SlotId slot_id) const {
    if (_materializer.get_slot_cache(slot_id) != nullptr) {
        return true;
    }
    return _active_chunk->is_slot_exist(slot_id);
}

bool LazyMaterializationContext::can_materialize(SlotId slot_id) const {
    const auto& lazy_slots = _materializer.lazy_slot_ids();
    if (std::find(lazy_slots.begin(), lazy_slots.end(), slot_id) != lazy_slots.end()) {
        return true;
    }
    if (_variant != nullptr) {
        const auto& hidden_lazy = _variant->lazy_hidden_slot_ids();
        return std::find(hidden_lazy.begin(), hidden_lazy.end(), slot_id) != hidden_lazy.end();
    }
    return false;
}

Status LazyMaterializationContext::materialize_slot(SlotId slot_id) {
    // Variant lazy hidden sources are dispatched to VariantProjectionHandler
    // before checking has_slot(), because they live outside the regular
    // slot_cache / slot_id_index.  materialize_hidden_source() own dedup
    // via _fetched_hidden_slots.
    if (_variant != nullptr) {
        const auto& hidden_lazy = _variant->lazy_hidden_slot_ids();
        if (std::find(hidden_lazy.begin(), hidden_lazy.end(), slot_id) != hidden_lazy.end()) {
            return _variant->materialize_hidden_source(slot_id, _range, _filter, _active_chunk);
        }
    }
    if (has_slot(slot_id)) {
        return Status::OK();
    }
    return _materializer.materialize_slot(slot_id, _range, _filter);
}

StatusOr<ColumnPtr> LazyMaterializationContext::provide(SlotId slot_id) {
    bool first_trigger = _triggered_slots.insert(slot_id).second;
    RETURN_IF_ERROR(materialize_slot(slot_id));
    if (first_trigger) {
        if (auto* s = _materializer.stats()) {
            s->parquet_lazy_slot_triggered++;
        }
    }
    const auto* entry = _materializer.get_slot_cache(slot_id);
    if (entry != nullptr) {
        return entry->values;
    }
    // Variant lazy hidden source slot: result is in active_chunk.
    if (_active_chunk->is_slot_exist(slot_id)) {
        return _active_chunk->get_column_by_slot_id(slot_id);
    }
    return Status::InternalError(fmt::format("provide: slot {} not found after materialize", slot_id));
}

ColumnPtr LazyMaterializationContext::get_column(SlotId slot_id) {
    auto* entry = _materializer.get_slot_cache(slot_id);
    if (entry != nullptr) {
        return entry->values;
    }
    if (_active_chunk->is_slot_exist(slot_id)) {
        return _active_chunk->get_column_by_slot_id(slot_id);
    }
    if (!can_materialize(slot_id)) {
        return nullptr;
    }
    auto st = materialize_slot(slot_id);
    if (!st.ok()) {
        return nullptr;
    }
    // Physical lazy slot: result lands in slot_cache.
    entry = _materializer.get_slot_cache(slot_id);
    if (entry != nullptr) {
        return entry->values;
    }
    // Variant lazy hidden source slot: result lands in active_chunk.
    if (_active_chunk->is_slot_exist(slot_id)) {
        return _active_chunk->get_column_by_slot_id(slot_id);
    }
    return nullptr;
}

} // namespace starrocks::parquet
