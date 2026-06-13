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
#include "common/logging.h"
#include "fmt/format.h"
#include "serde/column_array_serde.h"
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

Status LayeredOverlayColumnIterator::_load_inline_layer(SparseLayer& l) {
    // INLINE layer: source_rowids were already decoded by DeltaColumnGroup (seeded into
    // l.source_rowids by the stack builder) and the value blob is resident in the tablet meta
    // (copied into l.inline_value_blob). Materialize the value column with ZERO file IO. Mirrors
    // the FILE path's post-conditions: l.source_rowids ascending (size K), l.values size K in
    // value_column logical type/nullability.
    const int64_t k = l.row_count;

    if (static_cast<int64_t>(l.source_rowids.size()) != k) {
        return Status::Corruption(fmt::format("sdcg inline patch: source_rowid count {} != K {} (version {})",
                                              l.source_rowids.size(), k, l.version));
    }

    // Fingerprint guard (§4.3 / §8): max(source_rowid) must address a row that exists in the base
    // segment. Identical to the FILE path's guard.
    if (l.source_segment_num_rows > 0 && k > 0) {
        const uint32_t max_rowid = l.source_rowids.back(); // ascending => last is max
        if (static_cast<int64_t>(max_rowid) >= l.source_segment_num_rows) {
            return Status::Corruption(
                    fmt::format("sdcg inline patch: max(source_rowid)={} >= source_segment_num_rows={} (version {})",
                                max_rowid, l.source_segment_num_rows, l.version));
        }
    }

    // --- Deserialize the value blob via serde::ColumnArraySerde ---
    // The writer serialized the value column directly (carrying its nullability), so we deserialize
    // into a column built with the SAME logical type / nullability. ColumnArraySerde round-trips the
    // null markers when the column is nullable, exactly mirroring how `.spcols` value columns and
    // del-vec columns are (de)serialized elsewhere.
    l.values = ChunkFactory::column_from_field_type(l.value_column.type(), l.value_column.is_nullable());
    {
        const auto* buff = reinterpret_cast<const uint8_t*>(l.inline_value_blob.data());
        const auto* end = buff + l.inline_value_blob.size();
        StatusOr<const uint8_t*> res = serde::ColumnArraySerde::deserialize(buff, end, l.values.get());
        RETURN_IF_ERROR(res.status());
    }
    if (static_cast<int64_t>(l.values->size()) != k) {
        return Status::Corruption(fmt::format("sdcg inline patch: value row count {} != expected K {} (version {})",
                                              l.values->size(), k, l.version));
    }

    l.loaded = true;
    return Status::OK();
}

Status LayeredOverlayColumnIterator::_ensure_layer_loaded(SparseLayer& l) {
    if (l.loaded) {
        return Status::OK();
    }
    if (l.is_inline) {
        return _load_inline_layer(l);
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

    l.loaded = true;
    return Status::OK();
}

Status LayeredOverlayColumnIterator::_apply_one_layer(SparseLayer& l, const std::vector<uint32_t>& dst_offsets,
                                                      const std::vector<uint32_t>& local_ordinals, Column* dst) {
    DCHECK_EQ(dst_offsets.size(), local_ordinals.size());
    if (dst_offsets.empty()) {
        return Status::OK();
    }
    // Gather the layer's value rows at local_ordinals into a contiguous src column, then
    // blind positional overwrite into dst at dst_offsets via update_rows. update_rows requires
    // src.size() == dst_offsets.size() and uses src row i for dst row dst_offsets[i].
    auto src = l.values->clone_empty();
    RETURN_IF_EXCEPTION(
            src->append_selective(*l.values, local_ordinals.data(), 0, static_cast<uint32_t>(local_ordinals.size())));
    RETURN_IF_EXCEPTION(dst->update_rows(*src, dst_offsets.data()));
    return Status::OK();
}

Status LayeredOverlayColumnIterator::_apply_layers_contiguous(rowid_t base_begin, size_t n, size_t dst_offset,
                                                              Column* dst) {
    if (n == 0) {
        return Status::OK();
    }
    const rowid_t base_end = base_begin + static_cast<rowid_t>(n); // exclusive
    // Layers are version-ASCENDING; apply oldest first so the newest wins per row.
    for (auto& l : _layers) {
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
        std::vector<uint32_t> dst_offsets;
        std::vector<uint32_t> local_ordinals;
        dst_offsets.reserve(hi - lo);
        local_ordinals.reserve(hi - lo);
        for (auto it = lo; it != hi; ++it) {
            const uint32_t base_rowid = *it;
            if (!l.column_covers(base_rowid)) {
                continue; // placeholder union ordinal: never apply (corruption-prevention gate)
            }
            const auto local_ord = static_cast<uint32_t>(it - l.source_rowids.begin());
            // dst position for base_rowid within this contiguous window. The value column is read
            // union-indexed, so local_ord (the union ordinal) is the row to gather from `values`.
            dst_offsets.push_back(static_cast<uint32_t>(dst_offset + (base_rowid - base_begin)));
            local_ordinals.push_back(local_ord);
        }
        RETURN_IF_ERROR(_apply_one_layer(l, dst_offsets, local_ordinals, dst));
    }
    return Status::OK();
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
    for (auto& l : _layers) {
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
        std::vector<uint32_t> dst_offsets;
        std::vector<uint32_t> local_ordinals;
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
                const auto local_ord = static_cast<uint32_t>(it - l.source_rowids.begin());
                dst_offsets.push_back(static_cast<uint32_t>(dst_offset + rows_before + (base_rowid - sub_begin)));
                local_ordinals.push_back(local_ord);
            }
            rows_before += static_cast<uint32_t>(sub_end - sub_begin);
        }
        RETURN_IF_ERROR(_apply_one_layer(l, dst_offsets, local_ordinals, dst));
    }
    return Status::OK();
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
    // dst position dst_offset + i. For each layer, find the requested rowids covered by the layer.
    const rowid_t req_lo = rowids[0];
    const rowid_t req_hi = rowids[size - 1] + 1; // exclusive envelope of the requested rowids
    for (auto& l : _layers) {
        // Zero-IO pre-filter: skip a layer whose meta-provided presence range is disjoint from the
        // requested rowid envelope, without opening its `.spcols` file.
        if (l.presence_disjoint(req_lo, req_hi)) {
            continue;
        }
        RETURN_IF_ERROR(_ensure_layer_loaded(l));
        if (l.source_rowids.empty()) {
            continue;
        }
        std::vector<uint32_t> dst_offsets;
        std::vector<uint32_t> local_ordinals;
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
                    dst_offsets.push_back(static_cast<uint32_t>(dst_offset + i));
                    local_ordinals.push_back(static_cast<uint32_t>(j)); // union ordinal == value row
                }
                ++i;
                ++j;
            }
        }
        RETURN_IF_ERROR(_apply_one_layer(l, dst_offsets, local_ordinals, values));
    }
    return Status::OK();
}

} // namespace starrocks
