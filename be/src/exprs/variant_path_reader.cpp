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

#include "exprs/variant_path_reader.h"

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/nullable_column.h"
#include "column/variant_encoder.h"
#include "common/logging.h"

namespace starrocks {

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Recursively navigate a typed column following `suffix` from `seg_offset`, staying
// in native column space for ARRAY and reaching VARIANT only at the leaves.
//
// Rules:
//   - ARRAY + suffix[seg_offset] is array-index → access element directly, recurse
//   - VARIANT (any suffix remaining) → get_row_value() then VariantPath::seek
//   - scalar / exhausted suffix → encode single datum to VariantRowValue and return
//
// seg_offset avoids constructing a new VariantPath on every ARRAY recursion level.
// This avoids encoding an entire ARRAY<...> into variant binary just to seek [i].
static VariantReadResult drill_down_column(const Column* col, size_t row, const TypeDescriptor& type_desc,
                                           const VariantPath* suffix, size_t seg_offset = 0) {
    // Unwrap nullable wrapper.
    if (col->is_nullable()) {
        if (col->is_null(row)) return VariantReadResult{.state = VariantReadState::kNull};
        col = ColumnHelper::get_data_column(const_cast<Column*>(col));
    }
    // Unwrap const wrapper (row stays 0 — caller already normalised const rows).
    if (col->is_constant()) {
        col = down_cast<const ConstColumn*>(col)->data_column().get();
        row = 0;
    }

    const bool suffix_empty = (suffix == nullptr || seg_offset >= suffix->segments.size());

    // VARIANT node: delegate to a sub-VariantPathReader so that the inner typed
    // columns of this sub-VariantColumn are used properly (kTypedNoSuffix, kTypedRowSeek,
    // kRemainSeek, kFullMaterialize), rather than forcing a full merge via get_row_value().
    if (type_desc.type == TYPE_VARIANT) {
        const auto* variant_col = down_cast<const VariantColumn*>(col);
        if (suffix_empty) {
            // No further navigation needed — return the whole sub-variant value.
            VariantRowRef row_ref;
            if (variant_col->try_get_row_ref(row, &row_ref)) {
                return VariantReadResult{.state = VariantReadState::kValue, .value = row_ref.to_owned()};
            }
            VariantRowValue buf;
            const VariantRowValue* val = variant_col->get_row_value(row, &buf);
            if (val == nullptr) return VariantReadResult{.state = VariantReadState::kMissing};
            return VariantReadResult{.state = VariantReadState::kValue, .value = std::move(buf)};
        }
        // Reuse the same VariantPath with seg_offset — no sub-path allocation.
        // ARRAY layers above this point are also traversed without allocation.
        VariantPathReader sub_reader;
        sub_reader.prepare(variant_col, suffix, seg_offset);
        return sub_reader.read_row(row);
    }

    // ARRAY: drill down without encoding the whole array.
    if (!suffix_empty && suffix->segments[seg_offset].is_array() && type_desc.type == TYPE_ARRAY) {
        const auto* array_col = down_cast<const ArrayColumn*>(col);
        const auto& offsets = array_col->offsets().get_data();
        const size_t start = offsets[row];
        const size_t end = offsets[row + 1];
        const int idx = suffix->segments[seg_offset].get_index();
        // idx is always >= 0: the path parser rejects negative indices.
        if (static_cast<size_t>(idx) >= end - start) {
            return VariantReadResult{.state = VariantReadState::kMissing};
        }
        return drill_down_column(array_col->elements_column_raw_ptr(), start + idx, type_desc.children[0], suffix,
                                 seg_offset + 1);
    }

    // Scalar / other complex types: encode datum and seek any remaining suffix.
    Datum datum = col->get(row);
    auto encoded = VariantEncoder::encode_datum(datum, type_desc);
    if (!encoded.ok()) return VariantReadResult{.state = VariantReadState::kMissing};
    auto val = std::move(encoded).value();
    if (suffix_empty) return VariantReadResult{.state = VariantReadState::kValue, .value = std::move(val)};
    VariantRowRef val_ref = val.as_ref();
    DCHECK(suffix != nullptr);
    auto field = VariantPath::seek_view(val_ref, *suffix, seg_offset);
    if (!field.ok()) return VariantReadResult{.state = VariantReadState::kMissing};
    return VariantReadResult{.state = VariantReadState::kValue, .value = std::move(field).value().to_owned()};
}

// ---------------------------------------------------------------------------
// VariantPathReader
// ---------------------------------------------------------------------------

void VariantPathReader::prepare(const VariantColumn* col, const VariantPath* path, size_t seg_offset) {
    _col = col;
    _path = path;
    _seg_offset = seg_offset;
    _match_index = -1;
    _suffix.segments.clear();
    _has_typed_child = false;
    if (_path != nullptr) {
        _try_match_typed(_path);
    }
}

bool VariantPathReader::is_typed_exact() const {
    return _match_index >= 0 && _suffix.segments.empty();
}

LogicalType VariantPathReader::typed_type() const {
    DCHECK(_match_index >= 0);
    return _col->shredded_types()[_match_index].type;
}

const TypeDescriptor& VariantPathReader::typed_type_desc() const {
    DCHECK(_match_index >= 0);
    return _col->shredded_types()[_match_index];
}

const Column* VariantPathReader::typed_column() const {
    DCHECK(_match_index >= 0);
    return _col->typed_column_by_index(_match_index);
}

void VariantPathReader::_try_match_typed(const VariantPath* path) {
    if (path == nullptr) return;

    // Collect the leading object-key segments of the query path from _seg_offset.
    std::vector<std::string_view> object_keys;
    for (size_t i = _seg_offset; i < path->segments.size(); ++i) {
        const auto& seg = path->segments[i];
        if (!seg.is_object()) break;
        object_keys.emplace_back(seg.get_key());
    }
    if (object_keys.empty()) return;

    // Try longest-to-shortest object-key prefix against canonical shredded paths.
    VariantPath typed_prefix_path;
    typed_prefix_path.segments.reserve(object_keys.size());
    for (size_t prefix_len = object_keys.size(); prefix_len > 0; --prefix_len) {
        typed_prefix_path.segments.clear();
        for (size_t i = 0; i < prefix_len; ++i) {
            typed_prefix_path.segments.emplace_back(VariantSegment::make_object(std::string(object_keys[i])));
        }
        auto typed_key = typed_prefix_path.to_shredded_path();
        if (!typed_key.has_value()) {
            continue;
        }

        int idx = _col->find_shredded_path(*typed_key);
        if (idx < 0) continue;

        _match_index = idx;
        const size_t suffix_start = _seg_offset + prefix_len;
        if (suffix_start < path->segments.size()) {
            _suffix.segments.assign(path->segments.begin() + suffix_start, path->segments.end());
        } else {
            _suffix.segments.clear();
        }
        return;
    }

    // No typed match. Check whether the leading object-key prefix of the query
    // path is a strict parent of any shredded path.  If so, the remain at that
    // prefix is incomplete (typed children were shredded out), and _seek_base
    // would return stale/partial data — Layer 2 must be skipped.
    //
    // This check applies regardless of whether array segments follow the object-key
    // prefix (e.g. "$.a[0]" with shredded "a.b": remain's "a" is missing "b").
    const auto& parsed_paths = _col->parsed_shredded_paths();
    for (const auto& sp : parsed_paths) {
        if (sp.segments.size() <= object_keys.size()) {
            continue;
        }
        bool is_parent = true;
        for (size_t i = 0; i < object_keys.size(); ++i) {
            if (!sp.segments[i].is_object() || sp.segments[i].get_key() != object_keys[i]) {
                is_parent = false;
                break;
            }
        }
        if (is_parent) {
            _has_typed_child = true;
            break;
        }
    }
}

VariantReadResult VariantPathReader::_read_typed_row(size_t row) {
    DCHECK(_match_index >= 0);
    const Column* typed_col = _col->typed_column_by_index(_match_index);
    DCHECK(typed_col != nullptr);
    // For const columns all rows are represented by index 0.
    size_t typed_row = typed_col->is_constant() ? 0 : row;
    const TypeDescriptor& type_desc = _col->shredded_types()[_match_index];
    const VariantPath* suffix = _suffix.segments.empty() ? nullptr : &_suffix;
    return drill_down_column(typed_col, typed_row, type_desc, suffix);
}

bool VariantPathReader::_seek_base(size_t row, VariantRowValue* out) {
    if (!_col->has_metadata_column() || !_col->has_remain_value()) {
        return false;
    }

    Slice metadata_slice;
    Slice remain_slice;
    if (!ColumnHelper::get_binary_slice_at(_col->metadata_column().get(), row, &metadata_slice) ||
        !ColumnHelper::get_binary_slice_at(_col->remain_value_column().get(), row, &remain_slice)) {
        return false;
    }
    if (metadata_slice.size == 0 || remain_slice.size == 0) {
        return false;
    }

    VariantRowRef base_row(std::string_view(metadata_slice.data, metadata_slice.size),
                           std::string_view(remain_slice.data, remain_slice.size));
    DCHECK(_path != nullptr);
    auto field = VariantPath::seek_view(base_row, *_path, _seg_offset);
    if (!field.ok()) {
        return false;
    }
    *out = std::move(field).value().to_owned();
    return true;
}

VariantReadResult VariantPathReader::_read_full(size_t row) {
    VariantRowRef variant_ref;
    if (_col->try_get_row_ref(row, &variant_ref)) {
        DCHECK(_path != nullptr);
        auto field = VariantPath::seek_view(variant_ref, *_path, _seg_offset);
        if (!field.ok()) {
            return VariantReadResult{.state = VariantReadState::kMissing};
        }
        return VariantReadResult{.state = VariantReadState::kValue, .value = std::move(field).value().to_owned()};
    }

    VariantRowValue variant_buffer;
    const VariantRowValue* variant = _col->get_row_value(row, &variant_buffer);
    if (variant == nullptr) {
        return VariantReadResult{.state = VariantReadState::kMissing};
    }
    variant_ref = variant->as_ref();
    DCHECK(_path != nullptr);
    auto field = VariantPath::seek_view(variant_ref, *_path, _seg_offset);
    if (!field.ok()) {
        return VariantReadResult{.state = VariantReadState::kMissing};
    }
    return VariantReadResult{.state = VariantReadState::kValue, .value = std::move(field).value().to_owned()};
}

VariantReadResult VariantPathReader::read_row(size_t row) {
    DCHECK(_path != nullptr);

    if (_match_index >= 0) {
        return _read_typed_row(row);
    }

    // Root path ("$"): the remain payload is always incomplete because typed columns
    // were shredded out.  Skip Layer 2 and go directly to full materialisation.
    if (_seg_offset >= _path->segments.size()) {
        return _read_full(row);
    }

    // Layer 2: base remain-payload seek.
    // When no shredded path is a child of the query path, the remain is the sole
    // source for this path.  _seek_base success → return the value; failure →
    // the path simply does not exist (no typed column can contribute it either).
    // When a shredded path IS a child of the query path (_has_typed_child), the
    // remain at this prefix is incomplete, so skip Layer 2 and fall through to
    // full materialisation which merges typed columns and remain correctly.
    if (!_has_typed_child) {
        VariantRowValue base_field;
        if (_seek_base(row, &base_field)) {
            return VariantReadResult{.state = VariantReadState::kValue, .value = std::move(base_field)};
        }
        return VariantReadResult{.state = VariantReadState::kMissing};
    }

    // Layer 3: full row materialisation + seek.
    return _read_full(row);
}

} // namespace starrocks
