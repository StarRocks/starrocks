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

#include "storage/lake/sdcg_overlay_merge.h"

#include <algorithm>
#include <map>
#include <roaring/roaring.hh>
#include <set>
#include <unordered_map>

#include "column/column.h"
#include "column/fixed_length_column.h"
#include "common/status.h"

namespace starrocks::lake {

StatusOr<MergedOverlay> merge_overlay_layers(const std::vector<OverlaySparseLayer>& layers) {
    if (layers.empty()) {
        return Status::InternalError("merge_overlay_layers: no layers");
    }

    // 1. Union column uids in first-seen order, with a representative source column per uid (for type
    //    cloning). uid_pos maps a uid to its index in the merged value-column vector.
    std::vector<int32_t> union_uids;
    std::unordered_map<int32_t, size_t> uid_pos;
    std::vector<const Column*> uid_proto; // parallel to union_uids; a sample source column for the type
    for (const auto& layer : layers) {
        if (layer.values.size() != layer.column_uids.size()) {
            return Status::InternalError("merge_overlay_layers: values/column_uids size mismatch");
        }
        for (size_t c = 0; c < layer.column_uids.size(); ++c) {
            if (layer.values[c] == nullptr || layer.values[c]->size() != layer.source_rowids.size()) {
                return Status::InternalError("merge_overlay_layers: value column size != source_rowids size");
            }
            const int32_t uid = layer.column_uids[c];
            if (uid_pos.find(uid) == uid_pos.end()) {
                uid_pos.emplace(uid, union_uids.size());
                union_uids.push_back(uid);
                uid_proto.push_back(layer.values[c].get());
            }
        }
    }
    const size_t ncols = union_uids.size();

    // 2. Resolve the winning source for each (column, source_rowid): the highest-version layer that
    //    covers it (ties -> later in input order, which is also later/equal version). Collect the union
    //    of all covered source_rowids.
    struct Winner {
        int64_t version = -1;
        uint32_t layer_idx = 0;
        uint32_t layer_col_idx = 0; // index into layer.values / layer.column_uids
        uint32_t local_idx = 0;     // row index within that layer
    };
    std::vector<std::unordered_map<uint32_t, Winner>> winners(ncols); // [col_pos] -> (source_rowid -> winner)
    std::set<uint32_t> union_rowids;
    for (uint32_t li = 0; li < layers.size(); ++li) {
        const auto& layer = layers[li];
        for (uint32_t c = 0; c < layer.column_uids.size(); ++c) {
            const size_t cp = uid_pos[layer.column_uids[c]];
            auto& wmap = winners[cp];
            for (uint32_t i = 0; i < layer.source_rowids.size(); ++i) {
                const uint32_t rid = layer.source_rowids[i];
                auto it = wmap.find(rid);
                if (it == wmap.end() || layer.version >= it->second.version) {
                    wmap[rid] = Winner{layer.version, li, c, i};
                }
                union_rowids.insert(rid);
            }
        }
    }

    const int64_t k_union = static_cast<int64_t>(union_rowids.size());
    if (k_union == 0) {
        return Status::InternalError("merge_overlay_layers: empty merged overlay");
    }

    MergedOverlay out;
    out.num_rows = k_union;
    out.min_source_rowid = static_cast<int64_t>(*union_rowids.begin());
    out.max_source_rowid = static_cast<int64_t>(*union_rowids.rbegin());
    out.column_uids = union_uids;

    // base_rowid -> union ordinal [0, k_union). The set is ascending, so this is column 0's order.
    std::unordered_map<uint32_t, uint32_t> rowid_to_ordinal;
    rowid_to_ordinal.reserve(union_rowids.size());
    std::vector<int64_t> sorted_rowids;
    sorted_rowids.reserve(union_rowids.size());
    {
        uint32_t ord = 0;
        for (uint32_t rid : union_rowids) {
            rowid_to_ordinal.emplace(rid, ord++);
            sorted_rowids.push_back(static_cast<int64_t>(rid));
        }
    }

    // 3. Column 0: the ascending union source_rowid column (Int64).
    auto rowid_col = Int64Column::create();
    rowid_col->append_numbers(sorted_rowids.data(), sorted_rowids.size() * sizeof(int64_t));
    out.source_rowid_column = std::move(rowid_col);

    // 4. Each value column: clone the representative type, default-fill to k_union, then overlay the
    //    winning values at their union ordinals. We group winners by their source (layer, col) so each
    //    source column is gathered once. Build the per-column covered roaring as we go.
    out.value_columns.resize(ncols);
    std::vector<roaring::Roaring> col_roaring(ncols);
    for (size_t cp = 0; cp < ncols; ++cp) {
        auto value_col = uid_proto[cp]->clone_empty();
        value_col->append_default(k_union);

        // Collect ALL winners for this column as (union_ordinal, source layer/col/local), then write
        // them with a SINGLE update_rows in ascending-ordinal order. The previous code grouped winners
        // by source (layer, col) and called update_rows ONCE PER GROUP -- for a variable-length
        // (BinaryColumn) value column each call rebuilds the whole k_union-row column, so a column whose
        // winners span G source groups paid G full rebuilds (O(k_union * G)). Resolving to one
        // update_rows collapses that to a single rebuild. Ascending ordinal order is required:
        // BinaryColumn::update_rows' rebuild path assumes ascending destination indexes.
        struct OrdWinner {
            uint32_t ordinal;
            uint32_t layer_idx;
            uint32_t layer_col_idx;
            uint32_t local_idx;
        };
        std::vector<OrdWinner> ord_winners;
        ord_winners.reserve(winners[cp].size());
        for (const auto& [rid, w] : winners[cp]) {
            ord_winners.push_back(OrdWinner{rowid_to_ordinal[rid], w.layer_idx, w.layer_col_idx, w.local_idx});
            col_roaring[cp].add(rid);
        }
        std::sort(ord_winners.begin(), ord_winners.end(),
                  [](const OrdWinner& a, const OrdWinner& b) { return a.ordinal < b.ordinal; });

        auto gathered = uid_proto[cp]->clone_empty();
        std::vector<uint32_t> ordinals;
        ordinals.reserve(ord_winners.size());
        for (const auto& w : ord_winners) {
            // All layers' value columns for this uid share the same type/nullability (uid_proto[cp]),
            // so appending a single winning row from any layer into `gathered` is type-compatible. Built
            // in ascending-ordinal order so gathered row i corresponds to ordinals[i] (also ascending).
            gathered->append(*layers[w.layer_idx].values[w.layer_col_idx], w.local_idx, 1);
            ordinals.push_back(w.ordinal);
        }
        if (!ordinals.empty()) {
            value_col->update_rows(*gathered, ordinals.data());
        }
        out.value_columns[cp] = std::move(value_col);
    }

    // 5. Per-column presence: serialize each covered roaring + [min,max]+count. A column covering no row
    //    is impossible here (every union uid came from at least one layer), but guard anyway.
    out.presences.reserve(ncols);
    for (size_t cp = 0; cp < ncols; ++cp) {
        if (col_roaring[cp].isEmpty()) {
            continue;
        }
        MergedColumnPresence p;
        p.column_uid = union_uids[cp];
        p.count = static_cast<int64_t>(col_roaring[cp].cardinality());
        p.min_source_rowid = static_cast<int64_t>(col_roaring[cp].minimum());
        p.max_source_rowid = static_cast<int64_t>(col_roaring[cp].maximum());
        col_roaring[cp].runOptimize();
        std::string buf;
        buf.resize(col_roaring[cp].getSizeInBytes());
        col_roaring[cp].write(buf.data());
        p.roaring = std::move(buf);
        out.presences.push_back(std::move(p));
    }

    return out;
}

} // namespace starrocks::lake
