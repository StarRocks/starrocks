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
#include <memory>
#include <vector>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "storage/delta_column_group.h"
#include "storage/primitive/rowid_types.h"
#include "storage/rowset/column_iterator.h"
#include "storage/tablet_schema.h"

namespace starrocks {

namespace io {
class SeekableInputStream;
} // namespace io

class Column;
class ColumnAccessPath;
class Segment;

// LayeredOverlayColumnIterator implements the Sparse Delta Column Group (SDCG)
// read path described in handbook/plans/active/2026-06-01-partial-update-sdcg-design.md (§4.3, §6).
//
// For a single column uid, the DCG layer-stack resolution (newest -> oldest) may end at:
//   - a DENSE `.cols` file  (row-complete; becomes the base), or
//   - the original base segment column (no dense layer in the stack),
// with zero or more SPARSE `.spcols` overlay layers stacked on top.
//
// This iterator wraps:
//   - `base`   : a ColumnIterator over the dense file (if the walk ended at DENSE) or over the
//                original base segment column. It is row-complete over the base segment ordinal space.
//   - `layers` : SPARSE overlay layers in version-ASCENDING order (oldest first, newest last).
//                Apply order is ascending so the newest version is applied last => last-write-wins.
//                A layer is either a `.spcols` FILE or an INLINE patch carried in the tablet meta
//                (DeltaColumnGroupVerPB.inline_patches). File and inline layers interleave purely by
//                version; the apply machinery is identical (inline layers just skip all file IO).
//
// A SPARSE layer's `.spcols` file is a standard Segment v2 file whose column0 is the
// source_rowid (reserved uid kSDCGSourceRowidUid, u32, non-nullable, ascending) and whose
// remaining columns are the real update value columns, all K rows. `footer.num_rows == K`.
// The layer is keyed positionally: base segment rowid -> local ordinal in [0, K) via binary
// search of the (ascending) source_rowid vector.
//
// PoC simplifications (see contract):
//   - The layer's source_rowid column AND the value column are fully loaded into memory on first
//     use (K <= sdcg_sparse_max_rows, bounded, default 50k).
//   - Pruning hooks (zone map / bloom filter / dict) are conservative no-prune: overlay-covered
//     rows must never be pruned by the base column's page-level metadata, which is stale for them.
//     Correctness over pruning is the accepted PoC tradeoff.
//
// Local engine never produces SPARSE kinds in this PoC, so this iterator is only ever constructed
// when at least one SPARSE layer is present. When all DCG file kinds are DENSE, the legacy
// first-hit-replaces-base path is taken and this iterator is never built.
class LayeredOverlayColumnIterator final : public ColumnIterator {
public:
    // One overlay layer over the base segment. A layer is one of two physical flavors:
    //   - FILE   : a `.spcols` Segment v2 file (source_rowid column + value columns), loaded on
    //              first use (zero or one file IO per layer).
    //   - INLINE : an inline sparse patch carried directly in the tablet meta
    //              (DeltaColumnGroupVerPB.inline_patches). Its source_rowids (decoded by
    //              DeltaColumnGroup from the LE u32 bytes) and per-uid value blob
    //              (serde::ColumnArraySerde) travel inside the meta PUT, so the read path
    //              materializes them WITHOUT any file IO. See design §5.4/§5.7.
    // Both flavors share the same SparseLayer overlay machinery (binary-search positional apply,
    // presence pre-filter, version-ascending last-write-wins): only the materialization source
    // differs, fully hidden behind _ensure_layer_loaded.
    struct SparseLayer {
        // === FILE layer state (unused for INLINE) ===
        // The `.spcols` Segment, opened with a schema that contains BOTH the value column(s)
        // and the synthetic source_rowid column (uid == kSDCGSourceRowidUid). See
        // Segment::new_sparse_dcg_segment.
        std::shared_ptr<Segment> spcols_segment;
        // The read file for `spcols_segment`. Owned here so it outlives the transient column
        // iterators created during _ensure_layer_loaded. Each `.spcols` is its own physical file,
        // distinct from the base column's read file.
        std::shared_ptr<io::SeekableInputStream> read_file;

        // === INLINE layer state (unused for FILE) ===
        // When true this layer materializes from the inline patch data carried in the tablet meta
        // (DeltaColumnGroup::InlinePatch) instead of opening a `.spcols` file. _ensure_layer_loaded
        // deserializes the value blob; ZERO file IO. The decoded source_rowids are seeded directly
        // into the shared `source_rowids` member below (DeltaColumnGroup already decoded the LE
        // bytes), so an inline layer is "loaded" except for its value column.
        bool is_inline = false;
        // The serde::ColumnArraySerde blob for THIS layer's value column: the inline patch's
        // column_values[value_idx_of(uid)] for the requested uid. Copied from meta so it outlives
        // the InlinePatch / PB.
        std::string inline_value_blob;

        // === Shared layer state ===
        // The value column to read for this overlay (its real uid/type/nullability). For a FILE
        // layer it selects the `.spcols` value column; for an INLINE layer it drives the type and
        // nullability the value blob is deserialized into.
        TabletColumn value_column;
        // The DCG entry version of this layer (used for version-ascending ordering / diagnostics).
        int64_t version = 0;
        // Number of rows (K): footer.num_rows for FILE, InlineSparsePatchPB.row_count for INLINE.
        int64_t row_count = 0;
        // Source segment row count fingerprint (M). 0 means unknown (skip the guard).
        int64_t source_segment_num_rows = 0;

        // === Presence pre-filter (meta-provided, zero-IO layer skip) ===
        // Closed range [presence_min, presence_max] of base-segment ordinals (source_rowid values)
        // this `.spcols` touches, carried from the DCG's SparsePresencePB. When `presence_known` is
        // true the iterator may skip the layer for a read window disjoint from [min, max] WITHOUT
        // opening/loading the file. When false (DENSE / legacy / pre-presence writer), the bounds are
        // unknown and the layer must always be opened (the safe legacy behavior). Once a layer is
        // loaded, the exact in-memory source_rowids supersede these coarse bounds for filtering.
        int64_t presence_min = kSDCGPresenceUnknown;
        int64_t presence_max = kSDCGPresenceUnknown;
        bool presence_known = false;

        // True iff the meta-provided presence range [presence_min, presence_max] is disjoint from
        // the half-open base-ordinal window [win_begin, win_end). Only meaningful when presence_known.
        bool presence_disjoint(rowid_t win_begin, rowid_t win_end) const {
            if (!presence_known) {
                return false; // unknown bounds => cannot skip
            }
            // window is empty => trivially disjoint
            if (win_begin >= win_end) {
                return true;
            }
            // [min,max] closed vs [win_begin, win_end) half-open.
            return presence_max < static_cast<int64_t>(win_begin) || presence_min >= static_cast<int64_t>(win_end);
        }

        // Lazily-loaded full materialization (loaded on first use by load()):
        bool loaded = false;
        // The K source_rowids, ascending. Read from the reserved-uid column.
        std::vector<uint32_t> source_rowids;
        // The K value rows, in the same order as `source_rowids` (i.e. local ordinal order).
        MutableColumnPtr values;
    };

    // |base_initialized| indicates the caller already wired the base iterator's own read file and
    // called base->init(...). When true, init() does NOT re-init the base (its read file differs
    // from the overlay opts' read file); it only records opts for the lazily-opened layer iterators.
    LayeredOverlayColumnIterator(ColumnIteratorUPtr base, std::vector<SparseLayer> layers,
                                 bool base_initialized = false);
    ~LayeredOverlayColumnIterator() override = default;

    Status init(const ColumnIteratorOptions& opts) override;

    Status seek_to_first() override;
    Status seek_to_ordinal(ordinal_t ord) override;
    ordinal_t get_current_ordinal() const override { return _current_ordinal; }
    ordinal_t num_rows() const override { return _base->num_rows(); }

    Status next_batch(size_t* n, Column* dst) override;
    Status next_batch(const SparseRange<>& range, Column* dst) override;

    Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) override;

    // ---- Pruning hooks: conservative no-prune (overlay-covered rows must not be pruned) ----
    // The base column's zone map / bloom filter describe the *base* values, which are stale for any
    // row covered by an overlay layer. Since this PoC keeps no effective zone map, the safe behavior
    // is to never prune. We therefore do NOT delegate to the base for these hooks; we keep the
    // ColumnIterator default (full range / OK / index-absent) which prunes nothing.

    bool has_zone_map() const override { return false; }
    // get_row_ranges_by_zone_map: inherit ColumnIterator default (keeps the full src range).
    bool has_original_bloom_filter_index() const override { return false; }
    bool has_ngram_bloom_filter_index() const override { return false; }
    // get_row_ranges_by_bloom_filter: inherit ColumnIterator default (returns OK, prunes nothing).
    bool all_page_dict_encoded() const override { return false; }
    Status fetch_all_dict_words(std::vector<Slice>* words) const override {
        return Status::NotSupported("LayeredOverlayColumnIterator does not support dict words");
    }
    int dict_size() override { return 0; }
    int dict_lookup(const Slice& word) override { return -1; }

    bool only_nulls() const override { return false; }

    // Late-materialization predicate pushdown is disabled (overlay rows must not be page-pruned),
    // so the upper read loop never selects this iterator as the pushdown-driving (first) column.
    bool support_push_down_predicate(const std::vector<const ColumnPredicate*>& compound_and_predicates) override {
        return false;
    }
    // Reached only as a NON-first late-materialize column (i==1), where |compound_and_predicates|
    // is empty and the job is to materialize the rows already selected by the first column. We
    // read the full range with overlay applied, then append only the selected rows.
    Status next_batch_with_filter(const SparseRange<>& range, Column* dst,
                                  const std::vector<const ColumnPredicate*>& compound_and_predicates,
                                  Buffer<uint8_t>* selection, Buffer<uint16_t>* selected_idx,
                                  size_t* processed_rows) override;

    std::string name() const override { return "LayeredOverlayColumnIterator"; }

private:
    // Materialize layer `l` (source_rowids + values, full K rows) into memory if not already done.
    // FILE layers read their `.spcols` (one IO pass); INLINE layers decode the LE source_rowids and
    // deserialize the value blob already resident in the tablet meta (zero file IO).
    Status _ensure_layer_loaded(SparseLayer& l);
    // INLINE materialization helper: decode `inline_source_rowids` (LE u32) and deserialize
    // `inline_value_blob` (serde::ColumnArraySerde) into the layer's value_column type/nullability.
    Status _load_inline_layer(SparseLayer& l);

    // Apply all overlay layers (ascending) onto `dst` for a contiguous base ordinal window
    // [base_begin, base_begin + n), whose corresponding rows were just appended into `dst`
    // starting at dst offset `dst_offset`.
    Status _apply_layers_contiguous(rowid_t base_begin, size_t n, size_t dst_offset, Column* dst);

    // Apply all overlay layers (ascending) onto `dst` for a (sorted) SparseRange `range`, whose
    // corresponding rows were just appended into `dst` starting at dst offset `dst_offset`. The
    // i-th covered base rowid in `range` maps to dst position `dst_offset + i`.
    Status _apply_layers_range(const SparseRange<>& range, size_t dst_offset, Column* dst);

    // Apply one layer for the covered (dst_offset[], local_ordinal[]) pairs.
    Status _apply_one_layer(SparseLayer& l, const std::vector<uint32_t>& dst_offsets,
                            const std::vector<uint32_t>& local_ordinals, Column* dst);

    ColumnIteratorUPtr _base;
    std::vector<SparseLayer> _layers; // version-ASCENDING
    ordinal_t _current_ordinal = 0;
    ColumnIteratorOptions _opts;
    bool _base_initialized = false;
};

} // namespace starrocks
