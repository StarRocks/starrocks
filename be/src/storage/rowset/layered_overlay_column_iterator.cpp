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

#include "storage/rowset/layered_overlay_column_iterator.h"

#include <algorithm>
#include <limits>
#include <roaring/roaring.hh>

#include "column/append_with_mask.h"
#include "column/chunk_factory.h"
#include "column/column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/serde/column_array_serde.h"
#include "common/logging.h"
#include "fmt/format.h"
#include "storage/rowset/segment.h"
#include "storage/tablet_schema.h"

namespace starrocks {

LayeredOverlayColumnIterator::LayeredOverlayColumnIterator(ColumnIteratorUPtr base, std::vector<SparseLayer> layers,
                                                           bool base_initialized)
        : _base(std::move(base)), _layers(std::move(layers)), _base_initialized(base_initialized) {}

StatusOr<std::vector<uint32_t>> LayeredOverlayColumnIterator::decode_covered_rowids(const std::string& roaring_bytes) {
    // Decode Agent C's serialized 32-bit CRoaring portable bitmap into an ASCENDING base-rowid
    // vector. readSafe validates against the byte length (corruption-safe). The toUint32Array
    // output is already ascending (Roaring iterates in value order), so the apply gate can binary
    // search it directly. Wrapped against CRoaring throwing on a malformed blob.
    std::vector<uint32_t> covered;
    RETURN_IF_EXCEPTION({
        roaring::Roaring r = roaring::Roaring::readSafe(roaring_bytes.data(), roaring_bytes.size());
        covered.resize(r.cardinality());
        // toUint32Array fills the buffer in ASCENDING value order, exactly the form the apply gate
        // binary-searches. The 32-bit CRoaring container holds base-segment ordinals (< INT32_MAX).
        r.toUint32Array(covered.data());
    });
    return covered;
}

Status LayeredOverlayColumnIterator::init(const ColumnIteratorOptions& opts) {
    _opts = opts;
    if (_base_initialized) {
        // The base iterator was already wired to its own read file and initialized by the caller
        // (segment_iterator / update_manager). Do not re-init it with `opts` (whose read_file points
        // at a different file). `_opts` is recorded above for the lazily-opened layer iterators.
        return Status::OK();
    }
    return _base->init(opts);
}

StatusOr<bool> LayeredOverlayColumnIterator::_layer_value_could_match(
        SparseLayer& l, const std::vector<const ColumnPredicate*>& predicates, CompoundNodeType pred_relation) {
    if (predicates.empty()) {
        return true;
    }
    DCHECK(l.spcols_segment != nullptr);
    // Lightweight value-column iterator on THIS layer's `.spcols` (its own read file); we only query the
    // zone-map index, not the value data pages. Mirrors the opts wiring in _ensure_layer_loaded.
    ColumnIteratorOptions layer_opts = _opts;
    layer_opts.read_file = l.read_file.get();
    layer_opts.is_io_coalesce = false;
    ASSIGN_OR_RETURN(auto value_iter, l.spcols_segment->new_column_iterator(l.value_column, nullptr));
    RETURN_IF_ERROR(value_iter->init(layer_opts));
    if (!value_iter->has_zone_map()) {
        return true; // no value zone map on this .spcols column -> cannot rule the layer out -> keep (sound)
    }
    // Query the value column's zone map over the layer's full K-row space. Non-empty => some overlay value
    // could match the predicate. A packed `.spcols` value column includes placeholder rows, which only
    // WIDEN the zone map (=> more likely "could match") -> still sound, just less selective.
    SparseRange<> k_ranges;
    Range<> full_k(0, static_cast<rowid_t>(l.row_count));
    RETURN_IF_ERROR(value_iter->get_row_ranges_by_zone_map(predicates, nullptr, &k_ranges, pred_relation, &full_k));
    return !k_ranges.empty();
}

Status LayeredOverlayColumnIterator::get_row_ranges_by_zone_map(const std::vector<const ColumnPredicate*>& predicates,
                                                                const ColumnPredicate* del_predicate,
                                                                SparseRange<>* row_ranges,
                                                                CompoundNodeType pred_relation,
                                                                const Range<>* src_range) {
    DCHECK(row_ranges->empty());
    const auto n = static_cast<rowid_t>(_base->num_rows());
    Range<> full(0, n);
    const Range<>* clip = src_range != nullptr ? src_range : &full;

    // 1. Base-column zone-map pruning. The base column holds base/dense values; for a row later covered by
    //    an overlay the base value is STALE, but that only makes base_ranges KEEP more pages (never fewer),
    //    so it is sound -- the overlaid rows are separately re-included in step 2.
    if (!_base->has_zone_map()) {
        // No base zone map -> nothing to prune (this iterator's has_zone_map() also returns false, so the
        // engine normally won't call us). Keep the whole window WITHOUT opening any layer file: the union
        // with any layer's presence range would be a no-op over the full clip anyway. Avoids per-layer
        // zone-map index IO in the degenerate case.
        row_ranges->add(*clip);
        return Status::OK();
    }
    SparseRange<> result;
    RETURN_IF_ERROR(_base->get_row_ranges_by_zone_map(predicates, del_predicate, &result, pred_relation, src_range));

    // 2. Re-include each overlay layer's PRESENCE range (the base-rowid span it covers) when that layer's
    //    value zone map could match. A covered row whose overlay value matches lies within its layer's
    //    presence range, so it is never pruned. Layers disjoint from the read window are skipped without
    //    opening their file.
    for (auto& l : _layers) {
        const bool presence_known = l.presence_known && l.presence_min >= 0 && l.presence_max >= l.presence_min;
        Range<> presence =
                presence_known ? Range<>(static_cast<rowid_t>(l.presence_min), static_cast<rowid_t>(l.presence_max) + 1)
                               : *clip;
        Range<> clipped = presence.intersection(*clip);
        if (clipped.empty()) {
            continue; // layer does not touch this read window
        }
        ASSIGN_OR_RETURN(const bool could, _layer_value_could_match(l, predicates, pred_relation));
        if (could) {
            result |= SparseRange<>(clipped);
        }
    }
    *row_ranges = result;
    return Status::OK();
}

Status LayeredOverlayColumnIterator::seek_to_first() {
    RETURN_IF_ERROR(_base->seek_to_first());
    _current_ordinal = 0;
    return Status::OK();
}

Status LayeredOverlayColumnIterator::seek_to_ordinal(ordinal_t ord) {
    RETURN_IF_ERROR(_base->seek_to_ordinal(ord));
    _current_ordinal = ord;
    return Status::OK();
}

Status LayeredOverlayColumnIterator::_ensure_layer_loaded(SparseLayer& l) {
    if (l.loaded) {
        return Status::OK();
    }
    DCHECK(l.spcols_segment != nullptr);
    const int64_t k = l.row_count;

    // Per-layer iterator options: same as the overlay's opts but reading from THIS layer's
    // `.spcols` file (each layer is a distinct physical file).
    ColumnIteratorOptions layer_opts = _opts;
    layer_opts.read_file = l.read_file.get();
    layer_opts.is_io_coalesce = false;

    // --- Load source_rowid column (reserved uid, u32, non-nullable, ascending) ---
    // Build a synthetic TabletColumn for the reserved-uid source_rowid column. The `.spcols`
    // Segment was opened (via Segment::new_sparse_dcg_segment) with a read schema that contains
    // this column, so a ColumnReader keyed by kSDCGSourceRowidUid exists.
    TabletColumn rowid_col;
    rowid_col.set_unique_id(kSDCGSourceRowidUid);
    rowid_col.set_name("__sdcg_source_rowid__");
    rowid_col.set_type(TYPE_BIGINT); // see writer: u32 rowids widened to BIGINT for encoding support
    rowid_col.set_is_nullable(false);
    rowid_col.set_length(sizeof(int64_t));

    ASSIGN_OR_RETURN(auto rowid_iter, l.spcols_segment->new_column_iterator(rowid_col, nullptr));
    RETURN_IF_ERROR(rowid_iter->init(layer_opts));
    RETURN_IF_ERROR(rowid_iter->seek_to_first());
    auto rowid_holder = Int64Column::create();
    {
        auto remaining = static_cast<size_t>(k);
        while (remaining > 0) {
            size_t n = remaining;
            RETURN_IF_ERROR(rowid_iter->next_batch(&n, rowid_holder.get()));
            if (n == 0) {
                break;
            }
            remaining -= n;
        }
    }
    if (static_cast<int64_t>(rowid_holder->size()) != k) {
        return Status::Corruption(fmt::format("sdcg overlay: source_rowid row count {} != expected K {} in file {}",
                                              rowid_holder->size(), k, l.spcols_segment->file_name()));
    }
    l.source_rowids.resize(k);
    const auto& rowid_data = rowid_holder->immutable_data();
    for (int64_t i = 0; i < k; ++i) {
        l.source_rowids[i] = static_cast<uint32_t>(rowid_data[i]);
    }

    // Fingerprint guard (§4.3 / §8): max(source_rowid) must address a row that exists in the base
    // segment. A violation means the `.spcols` is bound to a stale/rewritten base layout.
    if (l.source_segment_num_rows > 0 && k > 0) {
        const uint32_t max_rowid = l.source_rowids.back(); // ascending => last is max
        DCHECK_LT(static_cast<int64_t>(max_rowid), l.source_segment_num_rows)
                << "sdcg overlay: max(source_rowid)=" << max_rowid
                << " >= source_segment_num_rows=" << l.source_segment_num_rows
                << " file=" << l.spcols_segment->file_name();
        if (static_cast<int64_t>(max_rowid) >= l.source_segment_num_rows) {
            return Status::Corruption(
                    fmt::format("sdcg overlay: max(source_rowid)={} >= source_segment_num_rows={} in file {}",
                                max_rowid, l.source_segment_num_rows, l.spcols_segment->file_name()));
        }
    }

    // --- Load value column (real uid / type), full K rows ---
    ASSIGN_OR_RETURN(auto value_iter, l.spcols_segment->new_column_iterator(l.value_column, nullptr));
    RETURN_IF_ERROR(value_iter->init(layer_opts));
    RETURN_IF_ERROR(value_iter->seek_to_first());
    // The materialized value column must match the destination column's runtime type so that
    // Column::update_rows(src, ...) accepts it. new_column_iterator already applies a
    // CastColumnIterator when the stored type differs from value_column.type(), so values read
    // here are in the value_column's logical type / nullability.
    l.values = ChunkFactory::column_from_field_type(l.value_column.type(), l.value_column.is_nullable());
    {
        auto remaining = static_cast<size_t>(k);
        while (remaining > 0) {
            size_t n = remaining;
            RETURN_IF_ERROR(value_iter->next_batch(&n, l.values.get()));
            if (n == 0) {
                break;
            }
            remaining -= n;
        }
    }
    if (static_cast<int64_t>(l.values->size()) != k) {
        return Status::Corruption(fmt::format("sdcg overlay: value row count {} != expected K {} in file {}",
                                              l.values->size(), k, l.spcols_segment->file_name()));
    }

    // === PACKED-LAYER COMPACTION (read-amp reduction) ===
    // A packed `.spcols` value column physically holds K_union rows with append_default PLACEHOLDERS at
    // the ordinals this column does not cover. Without this step, every read window iterates the full
    // union source_rowids and gates each ordinal on `covered_rowids` (O(K_union) work + a binary search
    // per candidate), and the winner resolution scans/gathers over the union. For a heterogeneous batch
    // (K_covered << K_union) that is pure waste, repeated per layer per read -- the dominant per-packed-
    // layer read amplification. Compact ONCE here to ONLY this column's covered rows: the layer then
    // behaves like a homogeneous K_covered-row layer (covered_known=false => no per-ordinal gating), so
    // all subsequent applies (contiguous / range / fetch_by_rowid) are O(K_covered). Correctness-
    // preserving: covered_rowids is a subset of source_rowids (the column covers a subset of the union),
    // and the dropped placeholders were never applied anyway (gated out by column_covers).
    if (l.covered_known && !l.covered_rowids.empty()) {
        if (l.covered_rowids.size() >= static_cast<size_t>(k)) {
            // Column covers the whole union (no placeholders): just drop the gate, keep the data.
            l.covered_known = false;
            l.covered_rowids.clear();
            l.covered_rowids.shrink_to_fit();
        } else {
            // Merge-intersect the two ASCENDING sets to find the covered rows' local (union) ordinals.
            std::vector<uint32_t> covered_local;
            std::vector<uint32_t> compact_rowids;
            covered_local.reserve(l.covered_rowids.size());
            compact_rowids.reserve(l.covered_rowids.size());
            size_t si = 0, ci = 0;
            while (si < l.source_rowids.size() && ci < l.covered_rowids.size()) {
                if (l.source_rowids[si] == l.covered_rowids[ci]) {
                    covered_local.push_back(static_cast<uint32_t>(si));
                    compact_rowids.push_back(l.source_rowids[si]);
                    ++si;
                    ++ci;
                } else if (l.source_rowids[si] < l.covered_rowids[ci]) {
                    ++si;
                } else {
                    ++ci; // covered rowid absent from union (should not happen); skip defensively
                }
            }
            auto compact_values = l.values->clone_empty();
            RETURN_IF_EXCEPTION(compact_values->append_selective(*l.values, covered_local.data(), 0,
                                                                 static_cast<uint32_t>(covered_local.size())));
            l.source_rowids = std::move(compact_rowids);
            l.values = std::move(compact_values);
            l.covered_known = false; // now homogeneous: every retained source_rowid is a real value
            l.covered_rowids.clear();
            l.covered_rowids.shrink_to_fit();
        }
    }

    l.loaded = true;
    return Status::OK();
}

Status LayeredOverlayColumnIterator::_finalize_winners(const std::vector<int32_t>& winner_layer,
                                                       const std::vector<uint32_t>& winner_local, size_t n,
                                                       size_t dst_offset, Column* dst) {
    // The newest-wins resolution (winner_layer / winner_local) is already done by the caller using
    // index-only work over the layers' ascending source_rowids -- NO value bytes were copied. Here we
    // gather each winning row's value ONCE into a contiguous `src` (in ascending dst order) and write
    // it with a SINGLE update_rows.
    //
    // WHY: the previous design called update_rows once PER overlay layer. For a variable-length
    // (BinaryColumn) dst, update_rows falls into rebuild_column() whenever a replaced value's length
    // differs from the base (the normal case for VARCHAR), which copies the ENTIRE dst column. With L
    // layers over an N-row read window that is O(N*L) byte copies though only ~K rows actually change
    // -- the dominant wide-column read amplification. Resolving the winner per row first collapses it
    // to ONE update_rows (one rebuild), i.e. O(N + total_winners). Fixed-length dst is unaffected
    // (update_rows overwrites in place either way) and now issues a single scatter instead of L.
    MutableColumnPtr src;
    std::vector<uint32_t> dst_offsets;
    const size_t dst_size = dst->size();
    for (size_t rel = 0; rel < n; ++rel) {
        const int32_t li = winner_layer[rel];
        if (li < 0) {
            continue; // no overlay covers this row: keep the base value already in dst
        }
        SparseLayer& l = _layers[static_cast<size_t>(li)];
        const uint32_t local_ord = winner_local[rel];
        const size_t values_size = (l.values != nullptr) ? l.values->size() : 0;
        const size_t dst_pos = dst_offset + rel;
        // RUNTIME bounds validation (NOT just DCHECK). Column::append / update_rows only DCHECK their
        // indices, so in a release build an out-of-range index silently reads/writes past a buffer and
        // clobbers ADJACENT heap -- the historical delayed null-deref of an UNRELATED metacache-shared
        // OrdinalIndexReader during a later compaction seek. Reject as a localized Corruption instead.
        if (dst_pos >= dst_size || local_ord >= values_size) {
            return Status::Corruption(
                    fmt::format("sdcg overlay finalize out of range: dst_pos={} (dst_size={}), local_ord={} "
                                "(values_size={}), layer version={} file={}",
                                dst_pos, dst_size, local_ord, values_size, l.version,
                                l.spcols_segment ? l.spcols_segment->file_name() : std::string("<null>")));
        }
        if (src == nullptr) {
            // All layers materialize l.values in the SAME logical type / nullability (the value
            // column), so a clone of the first winner's column accepts appends from any layer.
            src = l.values->clone_empty();
        }
        // Append the single winning row. Built in ascending dst order, so src row i corresponds to
        // dst_offsets[i] -- and dst_offsets stays ASCENDING, which BinaryColumn::update_rows'
        // rebuild path requires.
        RETURN_IF_EXCEPTION(src->append(*l.values, local_ord, 1));
        dst_offsets.push_back(static_cast<uint32_t>(dst_pos));
    }
    if (src != nullptr && !dst_offsets.empty()) {
        RETURN_IF_EXCEPTION(dst->update_rows(*src, dst_offsets.data()));
    }
    return Status::OK();
}

Status LayeredOverlayColumnIterator::_apply_layers_contiguous(rowid_t base_begin, size_t n, size_t dst_offset,
                                                              Column* dst) {
    if (n == 0) {
        return Status::OK();
    }
    const rowid_t base_end = base_begin + static_cast<rowid_t>(n); // exclusive
    // Resolve newest-wins per row across ALL layers FIRST (index-only on the layers' ascending
    // source_rowids -- NO value copy), then assemble the winning values in a SINGLE pass
    // (_finalize_winners). This replaces a per-layer update_rows, which rebuilt the whole dst column
    // PER LAYER for a variable-length dst (O(N*L) byte copies). winner_* are indexed by the
    // dst-relative position rel = base_rowid - base_begin in [0, n).
    std::vector<int32_t> winner_layer(n, -1);
    std::vector<uint32_t> winner_local(n);
    // Layers are version-ASCENDING; process oldest first so the newest layer overwrites the winner.
    for (size_t li = 0; li < _layers.size(); ++li) {
        auto& l = _layers[li];
        // Zero-IO pre-filter: when the meta-provided presence range is known and disjoint from the
        // read window, skip this layer WITHOUT opening/loading its `.spcols` file. Unknown presence
        // (DENSE/legacy/pre-presence writer) falls through to the load-then-filter path below.
        if (l.presence_disjoint(base_begin, base_end)) {
            continue;
        }
        RETURN_IF_ERROR(_ensure_layer_loaded(l));
        if (l.source_rowids.empty()) {
            continue;
        }
        if (l.source_rowids.front() >= base_end || l.source_rowids.back() < base_begin) {
            continue; // disjoint window
        }
        // Binary-search the union source_rowids intersecting [base_begin, base_end). For a packed
        // file source_rowids is the UNION (superset of this column's covered set), so each matched
        // union ordinal MUST be gated on the per-column covered set before applying: a placeholder
        // ordinal (not covered) would overwrite a real base value with NULL == corruption (§4.1).
        auto lo = std::lower_bound(l.source_rowids.begin(), l.source_rowids.end(), base_begin);
        auto hi = std::lower_bound(l.source_rowids.begin(), l.source_rowids.end(), base_end);
        for (auto it = lo; it != hi; ++it) {
            const uint32_t base_rowid = *it;
            if (!l.column_covers(base_rowid)) {
                continue; // placeholder union ordinal: never apply (corruption-prevention gate)
            }
            // The value column is read union-indexed, so the union ordinal is the row to gather from
            // `values`. A newer layer (larger li) overwrites the winner => last-write-wins.
            const auto rel = static_cast<uint32_t>(base_rowid - base_begin);
            winner_layer[rel] = static_cast<int32_t>(li);
            winner_local[rel] = static_cast<uint32_t>(it - l.source_rowids.begin());
        }
    }
    return _finalize_winners(winner_layer, winner_local, n, dst_offset, dst);
}

Status LayeredOverlayColumnIterator::next_batch(size_t* n, Column* dst) {
    const rowid_t base_begin = static_cast<rowid_t>(_current_ordinal);
    const size_t dst_offset = dst->size();
    RETURN_IF_ERROR(_base->next_batch(n, dst));
    const size_t read = *n;
    RETURN_IF_ERROR(_apply_layers_contiguous(base_begin, read, dst_offset, dst));
    _current_ordinal = base_begin + read;
    return Status::OK();
}

Status LayeredOverlayColumnIterator::next_batch(const SparseRange<>& range, Column* dst) {
    const size_t dst_offset = dst->size();
    RETURN_IF_ERROR(_base->next_batch(range, dst));
    // _apply_layers_range maps each covered base rowid to a destination position derived purely
    // from the REQUESTED range geometry (dst_offset + position-within-range). That is correct only
    // if the base appended exactly range.span_size() rows. If the base under-reads (e.g. the base
    // file's row count is smaller than the segment ordinal space this range spans -- a torn/stale
    // dense `.cols` base, or a base/overlay row-count mismatch), every overlay write would land
    // past the end of dst and corrupt the heap. Enforce the invariant before applying.
    const size_t appended = dst->size() - dst_offset;
    if (appended != range.span_size()) {
        return Status::Corruption(
                fmt::format("sdcg overlay base under-read: base appended {} rows but range span is {} "
                            "(base num_rows={}, range [{}, {}))",
                            appended, range.span_size(), _base->num_rows(), range.begin(), range.end()));
    }
    RETURN_IF_ERROR(_apply_layers_range(range, dst_offset, dst));
    // Track ordinal at the end of the consumed range (mirrors base behavior).
    if (!range.empty()) {
        _current_ordinal = range.end();
    }
    return Status::OK();
}

Status LayeredOverlayColumnIterator::_apply_layers_range(const SparseRange<>& range, size_t dst_offset, Column* dst) {
    if (range.empty()) {
        return Status::OK();
    }
    // Build, per layer, the (dst_offset, local_ordinal) pairs by walking the (sorted) sub-ranges.
    // The i-th covered base rowid in `range` (in ascending order) lands at dst position
    // `dst_offset + i` because next_batch(range, dst) appends rows in range order.
    const rowid_t range_lo = range.begin();
    const rowid_t range_hi = range.end(); // exclusive (end of last sub-range)
    const size_t span = range.span_size();
    // Newest-wins resolution first (index-only), single assembly after. winner_* are indexed by the
    // dst-relative position rel in [0, span): the i-th covered base rowid in `range` lands at
    // dst_offset + i, mirroring how next_batch(range, dst) appends rows in range order.
    std::vector<int32_t> winner_layer(span, -1);
    std::vector<uint32_t> winner_local(span);
    for (size_t li = 0; li < _layers.size(); ++li) {
        auto& l = _layers[li];
        // Zero-IO pre-filter against the meta-provided presence range, spanning the whole
        // [range_lo, range_hi) envelope. A layer disjoint from the envelope cannot cover any
        // sub-range, so skip it without opening the `.spcols` file.
        if (l.presence_disjoint(range_lo, range_hi)) {
            continue;
        }
        RETURN_IF_ERROR(_ensure_layer_loaded(l));
        if (l.source_rowids.empty()) {
            continue;
        }
        if (l.source_rowids.front() >= range_hi || l.source_rowids.back() < range_lo) {
            continue; // disjoint
        }
        // Walk sub-ranges accumulating the dst position of each row in the range.
        uint32_t rows_before = 0; // count of base rows in range strictly before the current sub-range
        auto iter = range.new_iterator();
        // Iterate sub-ranges by repeatedly pulling the next contiguous chunk.
        while (iter.has_more()) {
            Range<> sub = iter.next(std::numeric_limits<rowid_t>::max());
            const rowid_t sub_begin = sub.begin();
            const rowid_t sub_end = sub.end(); // exclusive
            // Union source_rowids within [sub_begin, sub_end). For a packed file these are the UNION;
            // each matched union ordinal MUST be gated on the per-column covered set before applying
            // (placeholder ordinals are skipped — corruption-prevention gate, §4.1).
            auto lo = std::lower_bound(l.source_rowids.begin(), l.source_rowids.end(), sub_begin);
            auto hi = std::lower_bound(l.source_rowids.begin(), l.source_rowids.end(), sub_end);
            for (auto it = lo; it != hi; ++it) {
                const uint32_t base_rowid = *it;
                if (!l.column_covers(base_rowid)) {
                    continue; // placeholder union ordinal: never apply
                }
                const auto rel = static_cast<uint32_t>(rows_before + (base_rowid - sub_begin));
                winner_layer[rel] = static_cast<int32_t>(li); // newer layer (larger li) overwrites
                winner_local[rel] = static_cast<uint32_t>(it - l.source_rowids.begin());
            }
            rows_before += static_cast<uint32_t>(sub_end - sub_begin);
        }
    }
    return _finalize_winners(winner_layer, winner_local, span, dst_offset, dst);
}

Status LayeredOverlayColumnIterator::next_batch_with_filter(
        const SparseRange<>& range, Column* dst, const std::vector<const ColumnPredicate*>& compound_and_predicates,
        Buffer<uint8_t>* selection, Buffer<uint16_t>* selected_idx, size_t* processed_rows) {
    // Only the empty-predicate (rowid materialization) form is reachable for this iterator: as a
    // NON-first late-materialize column, the upper loop passes ColumnPredicates() here and relies on
    // the selection already computed by the first column. Pushing predicates down to overlay pages
    // is intentionally unsupported (no-prune correctness).
    if (!compound_and_predicates.empty()) {
        return Status::NotSupported(
                "LayeredOverlayColumnIterator does not support predicate pushdown in next_batch_with_filter");
    }
    const size_t base_size = dst->size();
    const size_t num_rows = range.span_size();
    *processed_rows += num_rows;

    // Read the full (overlay-applied) range into a temp column, then append only the rows whose
    // selection bit (already set by the first predicate column at the matching offset) is true.
    auto temp = dst->clone_empty();
    RETURN_IF_ERROR(next_batch(range, temp.get()));
    DCHECK_EQ(temp->size(), num_rows);

    const uint8_t* sel = selection->data() + base_size;
    RETURN_IF_ERROR(append_with_mask</*PositiveSelect=*/true>(dst, *temp, sel, num_rows));
    return Status::OK();
}

Status LayeredOverlayColumnIterator::fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) {
    const size_t dst_offset = values->size();
    RETURN_IF_ERROR(_base->fetch_values_by_rowid(rowids, size, values));
    if (size == 0) {
        return Status::OK();
    }
    // rowids are ascending (contract of fetch_values_by_rowid). The i-th requested rowid lands at
    // dst position dst_offset + i. Resolve newest-wins per requested rowid first (index-only), then a
    // single assembly. winner_* are indexed by the request index i in [0, size).
    const rowid_t req_lo = rowids[0];
    const rowid_t req_hi = rowids[size - 1] + 1; // exclusive envelope of the requested rowids
    std::vector<int32_t> winner_layer(size, -1);
    std::vector<uint32_t> winner_local(size);
    for (size_t li = 0; li < _layers.size(); ++li) {
        auto& l = _layers[li];
        // Zero-IO pre-filter: skip a layer whose meta-provided presence range is disjoint from the
        // requested rowid envelope, without opening its `.spcols` file.
        if (l.presence_disjoint(req_lo, req_hi)) {
            continue;
        }
        RETURN_IF_ERROR(_ensure_layer_loaded(l));
        if (l.source_rowids.empty()) {
            continue;
        }
        // Two-pointer merge: both `rowids` (requested) and `l.source_rowids` (layer UNION) are
        // ascending. A union match at base_rowid is only applied when this column actually covers it
        // (packed files); a placeholder union ordinal is skipped — corruption-prevention gate (§4.1).
        size_t i = 0;
        size_t j = 0;
        const size_t m = l.source_rowids.size();
        while (i < size && j < m) {
            const uint32_t want = rowids[i];
            const uint32_t have = l.source_rowids[j];
            if (want < have) {
                ++i;
            } else if (want > have) {
                ++j;
            } else {
                if (l.column_covers(have)) {
                    winner_layer[i] = static_cast<int32_t>(li); // newer layer (larger li) overwrites
                    winner_local[i] = static_cast<uint32_t>(j); // union ordinal == value row
                }
                ++i;
                ++j;
            }
        }
    }
    return _finalize_winners(winner_layer, winner_local, size, dst_offset, values);
}

} // namespace starrocks
