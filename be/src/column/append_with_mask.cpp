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

#include "column/append_with_mask.h"

#include <cstring>
#include <type_traits>
#include <vector>

#include "base/simd/filter.h"
#include "base/simd/simd.h"
#include "column/adaptive_nullable_column.h"
#include "column/array_column.h"
#include "column/column.h"
#include "column/const_column.h"
#include "column/decimalv3_column.h"
#include "column/file_column.h"
#include "column/json_column.h"
#include "column/map_column.h"
#include "column/object_column.h"
#include "column/struct_column.h"
#include "column/variant_column.h"
#include "gutil/strings/fastmem.h"

namespace starrocks {

template <bool PositiveSelect>
AppendWithMaskVisitor<PositiveSelect>::AppendWithMaskVisitor(const Column& src, const uint8_t* mask, size_t count)
        : Base(this), _src(src), _mask(mask), _count(count) {}

template <bool PositiveSelect>
Status AppendWithMaskVisitor<PositiveSelect>::do_visit(NullableColumn* column) {
    if (_mask == nullptr || _count == 0) {
        return Status::OK();
    }

    size_t selected = count_selected();
    if (selected == 0) {
        return Status::OK();
    }

    auto* nullable_column = down_cast<NullableColumn*>(column);
    bool is_view_src =
            _src.is_view() || (_src.is_nullable() && down_cast<const NullableColumn*>(&_src)->data_column()->is_view());
    if (selected == _count && !is_view_src) {
        nullable_column->append(_src, 0, _count);
        return Status::OK();
    }

    size_t orig_size = nullable_column->null_column()->size();

    if (_src.only_null()) {
        nullable_column->append_nulls(selected);
        return Status::OK();
    }

    if (_src.is_nullable()) {
        const auto* src_column = down_cast<const NullableColumn*>(&_src);
        if (!src_column->has_null()) {
            nullable_column->null_column_raw_ptr()->resize(orig_size + selected);
            RETURN_IF_ERROR(apply(nullable_column->data_column_raw_ptr(), src_column->data_column().get()));
        } else {
            RETURN_IF_ERROR(apply(nullable_column->data_column_raw_ptr(), src_column->data_column().get()));
            RETURN_IF_ERROR(apply(nullable_column->null_column_raw_ptr(), src_column->null_column().get()));
            size_t appended = nullable_column->null_column()->size() - orig_size;
            if (appended != 0) {
                const auto null_data = nullable_column->null_column()->immutable_data();
                bool has_new_null =
                        SIMD::contain_nonzero(null_data, orig_size, appended) || nullable_column->has_null();
                nullable_column->set_has_null(has_new_null);
            }
        }
    } else {
        nullable_column->null_column_raw_ptr()->resize(orig_size + selected);
        RETURN_IF_ERROR(apply(nullable_column->data_column_raw_ptr(), &_src));
    }

    DCHECK_EQ(nullable_column->null_column()->size(), nullable_column->data_column()->size());
    return Status::OK();
}

template <bool PositiveSelect>
Status AppendWithMaskVisitor<PositiveSelect>::do_visit(BinaryColumn* column) {
    return append_binary(column);
}

template <bool PositiveSelect>
bool AppendWithMaskVisitor<PositiveSelect>::is_selected(uint8_t value) const {
    if constexpr (PositiveSelect) {
        return value != 0;
    } else {
        return value == 0;
    }
}

template <bool PositiveSelect>
size_t AppendWithMaskVisitor<PositiveSelect>::count_selected() const {
    if (_mask == nullptr) {
        return 0;
    }

    size_t selected = SIMD::count_nonzero(_mask, _count);
    if constexpr (PositiveSelect) {
        return selected;
    } else {
        return _count - selected;
    }
}

template <bool PositiveSelect>
template <typename T>
Status AppendWithMaskVisitor<PositiveSelect>::append_fixed_length(FixedLengthColumnBase<T>* column) {
    if (_mask == nullptr || _count == 0) {
        return Status::OK();
    }
    if (_src.is_view()) {
        append_fallback(column);
        return Status::OK();
    }

    auto* src_column = dynamic_cast<const FixedLengthColumnBase<T>*>(&_src);
    if (src_column == nullptr) {
        append_fallback(column);
        return Status::OK();
    }

    size_t selected = count_selected();
    if (selected == 0) {
        return Status::OK();
    }

    auto& data = column->get_data();
    size_t orig_size = data.size();
    const T* __restrict src_data = reinterpret_cast<const T*>(src_column->raw_data());

    if (selected == _count) {
        raw::stl_vector_resize_uninitialized(&data, orig_size + _count);
        auto* __restrict dest = data.data() + orig_size;
        std::memcpy(dest, src_data, _count * sizeof(T));
        return Status::OK();
    }

    if constexpr (PositiveSelect && sizeof(T) <= 32) {
        // Size destination buffer to orig_size + _count to prevent AVX-512 _mm512_storeu buffer overrun
        raw::stl_vector_resize_uninitialized(&data, orig_size + _count);
        auto* __restrict dest = data.data() + orig_size;
        SIMD::Filter::filter_range(dest, src_data, _mask, 0, _count);
        raw::stl_vector_resize_uninitialized(&data, orig_size + selected);
        return Status::OK();
    } else {
        raw::stl_vector_resize_uninitialized(&data, orig_size + selected);
        auto* __restrict dest = data.data() + orig_size;

        size_t i = 0;
        while (i < _count) {
            while (i < _count && !is_selected(_mask[i])) {
                ++i;
            }
            if (i >= _count) {
                break;
            }
            size_t run_start = i;
            while (i < _count && is_selected(_mask[i])) {
                ++i;
            }
            size_t run_len = i - run_start;
            std::memcpy(dest, src_data + run_start, run_len * sizeof(T));
            dest += run_len;
        }
        return Status::OK();
    }
}

template <bool PositiveSelect>
Status AppendWithMaskVisitor<PositiveSelect>::append_binary(BinaryColumn* column) {
    if (_mask == nullptr || _count == 0) {
        return Status::OK();
    }
    if (_src.is_view() || _src.is_binary_view()) {
        append_fallback(column);
        return Status::OK();
    }

    auto* src_column = dynamic_cast<const BinaryColumn*>(&_src);
    if (src_column == nullptr) {
        append_fallback(column);
        return Status::OK();
    }

    size_t selected = count_selected();
    if (selected == 0) {
        return Status::OK();
    }

    if (selected == _count) {
        column->append(*src_column, 0, _count);
        return Status::OK();
    }

    const auto& src_offsets = src_column->get_offset();
    const auto* src_bytes = src_column->raw_bytes();

    size_t total_bytes = 0;
    src_offsets.visit_storage([&](const auto& src_offsets_buf) {
        const auto* __restrict src_offsets_data = src_offsets_buf.data();
        size_t i = 0;
        while (i < _count) {
            while (i < _count && !is_selected(_mask[i])) {
                ++i;
            }
            if (i >= _count) {
                break;
            }
            size_t run_start = i;
            while (i < _count && is_selected(_mask[i])) {
                ++i;
            }
            size_t run_end = i;
            total_bytes += src_offsets_data[run_end] - src_offsets_data[run_start];
        }
    });

    auto& offsets = column->get_offset();
    auto& bytes = column->get_bytes();
    size_t old_rows = offsets.size() - 1;
    size_t old_bytes = bytes.size();
    offsets.resize_uninitialized(old_rows + selected + 1, old_bytes + total_bytes);
    bytes.resize(old_bytes + total_bytes);

    auto* dest_bytes = bytes.data() + old_bytes;

    using Offsets = BinaryColumn::Offsets;
    Offsets::visit_storage_pair(offsets, src_offsets, [&](auto& dst_offsets_buf, const auto& src_offsets_buf) {
        using DstValue = typename std::decay_t<decltype(dst_offsets_buf)>::value_type;
        const auto* __restrict src_offsets_data = src_offsets_buf.data();
        auto* __restrict dest_offsets = dst_offsets_buf.data() + old_rows + 1;

        uint64_t current_offset = old_bytes;
        dst_offsets_buf[old_rows] = static_cast<DstValue>(current_offset);
        size_t copied = 0;

        size_t i = 0;
        while (i < _count) {
            while (i < _count && !is_selected(_mask[i])) {
                ++i;
            }
            if (i >= _count) {
                break;
            }
            size_t run_start = i;
            while (i < _count && is_selected(_mask[i])) {
                ++i;
            }
            size_t run_end = i;
            size_t run_len = run_end - run_start;
            size_t run_bytes = src_offsets_data[run_end] - src_offsets_data[run_start];

            strings::memcpy_inlined(dest_bytes + copied, src_bytes + src_offsets_data[run_start], run_bytes);
            copied += run_bytes;

            const uint64_t run_start_offset = src_offsets_data[run_start];
            for (size_t j = 0; j < run_len; ++j) {
                dest_offsets[j] = static_cast<DstValue>(current_offset +
                                                        (src_offsets_data[run_start + 1 + j] - run_start_offset));
            }
            dest_offsets += run_len;
            current_offset += run_bytes;
        }
    });

    column->invalidate_slice_cache();
    return Status::OK();
}

template <bool PositiveSelect>
Status AppendWithMaskVisitor<PositiveSelect>::apply(Column* dst, const Column* src) {
    return append_with_mask<PositiveSelect>(dst, *src, _mask, _count);
}

template <bool PositiveSelect>
template <typename ColumnType>
void AppendWithMaskVisitor<PositiveSelect>::append_fallback(ColumnType* column) {
    if (_mask == nullptr || _count == 0) {
        return;
    }
    if (_src.is_view()) {
        std::vector<uint32_t> indexes;
        indexes.reserve(_count);
        for (size_t i = 0; i < _count; ++i) {
            if (is_selected(_mask[i])) {
                indexes.push_back(i);
            }
        }
        if (!indexes.empty()) {
            _src.append_selective_to(*column, indexes.data(), 0, indexes.size());
        }
        return;
    }
    for (size_t i = 0; i < _count; ++i) {
        if (is_selected(_mask[i])) {
            column->append(_src, i, 1);
        }
    }
}

template <bool PositiveSelect>
Status append_with_mask(Column* dst, const Column& src, const uint8_t* mask, size_t count) {
    if (dst == nullptr || count == 0) {
        return Status::OK();
    }
    AppendWithMaskVisitor<PositiveSelect> visitor(src, mask, count);
    return dst->accept_mutable(&visitor);
}

// Explicit instantiations

template class AppendWithMaskVisitor<true>;
template class AppendWithMaskVisitor<false>;

template Status append_with_mask<true>(Column*, const Column&, const uint8_t*, size_t);
template Status append_with_mask<false>(Column*, const Column&, const uint8_t*, size_t);

} // namespace starrocks
