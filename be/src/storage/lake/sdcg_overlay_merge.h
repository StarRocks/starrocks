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

#include <cstdint>
#include <string>
#include <vector>

#include "column/vectorized_fwd.h"
#include "common/statusor.h"

namespace starrocks::lake {

// One already-materialized SPARSE overlay layer to merge, read back from a `.spcols` file (or inline
// patch) for ONE base segment. `source_rowids` is K ascending base-segment ordinals; each entry of
// `values` is the K-row value column for the corresponding `column_uids` entry, aligned 1:1 to
// `source_rowids` (row i of values[c] is the value for base ordinal source_rowids[i] of column
// column_uids[c]). Layers are merged version-ascending (later version wins per (rowid, column)).
struct OverlaySparseLayer {
    int64_t version = 0;
    std::vector<int32_t> column_uids;    // value columns this layer carries
    std::vector<uint32_t> source_rowids; // K ascending base-segment ordinals
    std::vector<ColumnPtr> values;       // parallel to column_uids; each size() == source_rowids.size()
};

// Per-column presence of the merged packed overlay: the exact covered base-rowid set (serialized
// 32-bit CRoaring portable bitmap) plus the cheap [min,max]+count summary. Mirrors the packed
// `.spcols` ColumnSparsePresencePB the reader already understands.
struct MergedColumnPresence {
    int32_t column_uid = 0;
    int64_t min_source_rowid = 0;
    int64_t max_source_rowid = 0;
    int64_t count = 0;
    std::string roaring;
};

// Result of merging: a single packed overlay covering the union of all touched rows, with
// per-(source_rowid, column) last-write-wins applied. The caller assembles `source_rowid_column`
// (column 0) followed by `value_columns` (in `column_uids` order) into the `.spcols` chunk and emits
// `presences` into the DCG entry. `value_columns` rows uncovered by a column hold append_default
// placeholders (NEVER applied -- gated by that column's `roaring`).
struct MergedOverlay {
    MutableColumnPtr source_rowid_column;        // Int64, num_rows ascending base-segment ordinals
    std::vector<int32_t> column_uids;            // value column order (first-seen across input layers)
    std::vector<MutableColumnPtr> value_columns; // parallel to column_uids, num_rows rows each
    std::vector<MergedColumnPresence> presences; // one per column covering >= 1 row
    int64_t num_rows = 0;                        // K_union (== source_rowid_column->size())
    int64_t min_source_rowid = 0;
    int64_t max_source_rowid = 0;
};

// Merge SPARSE overlay layers of ONE base segment into a single packed overlay, applying
// last-write-wins per (source_rowid, column_uid) by ascending `version`. Pure function: NO base
// segment read, NO I/O, NO rows mapper (the base rowset is untouched so source_rowid coordinates stay
// valid). Layers may carry different / overlapping column sets; the union column order is first-seen
// across the input order. Returns an error if a layer is internally inconsistent (values vs column_uids
// vs source_rowids size mismatch) or empty after merge.
//
// SPARSE layers only: the caller must exclude DENSE (.cols) conflicting layers, flexible/packed and
// inline layers it cannot represent here, falling back to leaving the chain unmerged (always correct).
StatusOr<MergedOverlay> merge_overlay_layers(const std::vector<OverlaySparseLayer>& layers);

} // namespace starrocks::lake
