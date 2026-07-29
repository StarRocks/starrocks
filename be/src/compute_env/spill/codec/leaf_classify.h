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

// Classifies a top-level spill column into the codec decision tree's first layer, and
// exposes a raw byte view of fixed-length scalar leaves (the working substrate of the
// M1 integer/bool codecs). Everything not classified keeps the M0 behavior (RAW/LEGACY
// on the whole column).

#include "column/binary_column.h"
#include "column/column.h"
#include "column/decimalv3_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "types/date_value.h"
#include "types/timestamp_value.h"

namespace starrocks::spill {

enum class LeafKind {
    OTHER = 0, // floats, complex types, int128 -- not handled by leaf codecs yet
    BOOL,      // FixedLengthColumn<uint8_t> (BooleanColumn / null flags)
    I32,       // 4-byte fixed: int32 / uint32 / date / decimal32
    I64,       // 8-byte fixed: int64 / uint64 / timestamp / decimal64
    STR,       // BinaryColumn (uint32 offsets): varchar/char
    F64,       // FixedLengthColumn<double>
};

inline size_t leaf_kind_width(LeafKind k) {
    switch (k) {
    case LeafKind::BOOL:
        return 1;
    case LeafKind::I32:
        return 4;
    case LeafKind::I64:
    case LeafKind::F64:
        return 8;
    default:
        return 0;
    }
}

struct LeafView {
    LeafKind kind = LeafKind::OTHER;
    const Column* leaf = nullptr;             // the data column (nullable unwrapped)
    const NullableColumn* nullable = nullptr; // set iff the top-level column is nullable
    const uint8_t* data = nullptr;            // raw fixed-length bytes (BOOL/I32/I64 only)
    size_t rows = 0;
};

namespace leaf_detail {

template <typename T>
bool try_view(const Column& c, LeafKind kind, LeafView* v) {
    if (const auto* fc = dynamic_cast<const FixedLengthColumnBase<T>*>(&c); fc != nullptr) {
        v->kind = kind;
        v->data = fc->raw_data();
        v->rows = fc->size();
        return true;
    }
    return false;
}

inline void classify_data_column(const Column& c, LeafView* v) {
    // note: FixedLengthColumnBase<T> also covers DecimalV3Column<T>
    if (try_view<uint8_t>(c, LeafKind::BOOL, v)) return;
    if (try_view<int32_t>(c, LeafKind::I32, v) || try_view<uint32_t>(c, LeafKind::I32, v) ||
        try_view<DateValue>(c, LeafKind::I32, v)) {
        return;
    }
    if (try_view<int64_t>(c, LeafKind::I64, v) || try_view<uint64_t>(c, LeafKind::I64, v) ||
        try_view<TimestampValue>(c, LeafKind::I64, v)) {
        return;
    }
    if (try_view<double>(c, LeafKind::F64, v)) return;
    if (const auto* bc = dynamic_cast<const BinaryColumnBase<uint32_t>*>(&c); bc != nullptr) {
        v->kind = LeafKind::STR;
        v->rows = bc->size();
        return; // data stays null: bytes/offsets are accessed via the column
    }
    v->kind = LeafKind::OTHER;
}

template <typename T>
bool try_mutable_bytes(Column* c, size_t rows, uint8_t** out) {
    if (auto* fc = dynamic_cast<FixedLengthColumnBase<T>*>(c); fc != nullptr) {
        fc->resize_uninitialized(rows);
        *out = reinterpret_cast<uint8_t*>(fc->get_data().data());
        return true;
    }
    return false;
}

template <typename T>
bool try_element_bytes(const Column* c, size_t* out) {
    if (dynamic_cast<const FixedLengthColumnBase<T>*>(c) != nullptr) {
        *out = sizeof(T);
        return true;
    }
    return false;
}

} // namespace leaf_detail

inline LeafView classify_leaf(const Column& col) {
    LeafView v;
    if (const auto* nc = dynamic_cast<const NullableColumn*>(&col); nc != nullptr) {
        v.nullable = nc;
        v.leaf = nc->data_column_raw_ptr();
    } else {
        v.leaf = &col;
    }
    leaf_detail::classify_data_column(*v.leaf, &v);
    return v;
}

// Resize the (empty) decode-target leaf to `rows` and expose its mutable byte view.
// Returns false if the leaf is not a supported fixed-length scalar. Note `*out` may
// legitimately be nullptr when rows == 0.
inline bool leaf_mutable_bytes(Column* leaf, size_t rows, uint8_t** out) {
    if (leaf_detail::try_mutable_bytes<uint8_t>(leaf, rows, out)) return true;
    if (leaf_detail::try_mutable_bytes<int32_t>(leaf, rows, out)) return true;
    if (leaf_detail::try_mutable_bytes<uint32_t>(leaf, rows, out)) return true;
    if (leaf_detail::try_mutable_bytes<DateValue>(leaf, rows, out)) return true;
    if (leaf_detail::try_mutable_bytes<int64_t>(leaf, rows, out)) return true;
    if (leaf_detail::try_mutable_bytes<uint64_t>(leaf, rows, out)) return true;
    if (leaf_detail::try_mutable_bytes<TimestampValue>(leaf, rows, out)) return true;
    if (leaf_detail::try_mutable_bytes<double>(leaf, rows, out)) return true;
    return false;
}

// Byte-width of one element of a fixed-length scalar leaf, or 0 if the column is not a
// supported fixed-length scalar. Used by decoders to reject a self-describing stream width
// that disagrees with the destination column's real element size (corruption/truncation
// guard: routing a decode by the stream byte alone could otherwise write past the buffer
// the destination is sized for).
inline size_t leaf_element_bytes(const Column* leaf) {
    size_t sz = 0;
    if (leaf_detail::try_element_bytes<uint8_t>(leaf, &sz)) return sz;
    if (leaf_detail::try_element_bytes<int32_t>(leaf, &sz)) return sz;
    if (leaf_detail::try_element_bytes<uint32_t>(leaf, &sz)) return sz;
    if (leaf_detail::try_element_bytes<DateValue>(leaf, &sz)) return sz;
    if (leaf_detail::try_element_bytes<int64_t>(leaf, &sz)) return sz;
    if (leaf_detail::try_element_bytes<uint64_t>(leaf, &sz)) return sz;
    if (leaf_detail::try_element_bytes<TimestampValue>(leaf, &sz)) return sz;
    if (leaf_detail::try_element_bytes<double>(leaf, &sz)) return sz;
    return 0;
}

} // namespace starrocks::spill
