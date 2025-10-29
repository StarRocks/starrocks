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

#include "storage/rowset/binary_plain_page.h"

#include "common/config.h"

#ifdef __SSE4_2__
#include <emmintrin.h>
#endif

#include <cstring>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "gutil/casts.h"
#include "storage/column_predicate.h"
#include "types/logical_type.h"

namespace starrocks {

template <LogicalType Type>
Status BinaryPlainPageDecoder<Type>::next_batch(size_t* count, Column* dst) {
    SparseRange<> read_range;
    uint32_t begin = current_index();
    read_range.add(Range<>(begin, begin + *count));
    RETURN_IF_ERROR(next_batch(read_range, dst));
    *count = current_index() - begin;
    return Status::OK();
}

template <LogicalType Type>
Status BinaryPlainPageDecoder<Type>::next_batch(const SparseRange<>& range, Column* dst) {
    DCHECK(_parsed);
    if (PREDICT_FALSE(_cur_idx >= _num_elems)) {
        return Status::OK();
    }

    size_t to_read = std::min(range.span_size(), _num_elems - _cur_idx);
    std::vector<Slice> strs;
    strs.reserve(to_read);
    SparseRangeIterator<> iter = range.new_iterator();
    if constexpr (Type == TYPE_CHAR) {
        while (to_read > 0) {
            _cur_idx = iter.begin();
            Range<> r = iter.next(to_read);
            size_t end = _cur_idx + r.span_size();
            for (; _cur_idx < end; _cur_idx++) {
                Slice s = string_at_index(_cur_idx);
                s.size = strnlen(s.data, s.size);
                strs.emplace_back(s);
            }
            to_read -= r.span_size();
        }
        if (dst->append_strings(strs)) {
            return Status::OK();
        }
    } else if constexpr (Type == TYPE_VARCHAR) {
        bool append_status = true;
        while (to_read > 0) {
            _cur_idx = iter.begin();
            Range<> r = iter.next(to_read);
            size_t end = _cur_idx + r.span_size();
            append_status &= append_range(_cur_idx, end, dst);
            to_read -= r.span_size();
            _cur_idx = end;
        }
        if (append_status) {
            return Status::OK();
        }
    } else {
        // other types
        while (to_read > 0) {
            _cur_idx = iter.begin();
            Range<> r = iter.next(to_read);
            size_t end = _cur_idx + r.span_size();
            for (; _cur_idx < end; _cur_idx++) {
                strs.emplace_back(string_at_index(_cur_idx));
            }
            to_read -= r.span_size();
        }
        if (range.size() == 1) {
            if (dst->append_continuous_strings(strs)) {
                return Status::OK();
            }
        } else if (dst->append_strings(strs)) {
            return Status::OK();
        }
    }
    return Status::InvalidArgument("Column::append_strings() not supported");
}

template <LogicalType Type>
bool BinaryPlainPageDecoder<Type>::append_range(uint32_t idx, uint32_t end, Column* dst) const {
    if constexpr (Type == TYPE_VARCHAR) {
        auto data_column = ColumnHelper::get_data_column(dst);
        auto& bytes = down_cast<BinaryColumn*>(data_column)->get_bytes();
        auto& offsets = down_cast<BinaryColumn*>(data_column)->get_offset();
        DCHECK_GE(offsets.size(), 1);

        uint32_t begin_offset = offsets.back();
        size_t current_offset_sz = offsets.size();
        offsets.resize(current_offset_sz + end - idx);

        const uint32_t page_data_offset = offset_uncheck(idx);
        // vectorized loop
        for (uint32_t i = idx; i < end - 1; i++) {
#if __BYTE_ORDER == __LITTLE_ENDIAN
            auto offset = _offsets_ptr[i + 1];
#else
            // direct call offset_uncheck() will break auto-vectorized
            // maybe we can remove this condition compile after we upgrade the toolchain
            auto offset = offset_uncheck(i + 1);
#endif
            uint32_t current_offset = offset - page_data_offset + begin_offset;
            offsets[current_offset_sz++] = current_offset;
        }

        for (uint32_t i = end - 1; i < end; i++) {
            uint32_t current_offset = offset(i + 1) - page_data_offset + begin_offset;
            offsets[current_offset_sz++] = current_offset;
        }

        size_t append_bytes_length = offsets.back() - begin_offset;
        size_t old_bytes_size = bytes.size();
        // try to reserve to avoid extra memcpy
        if (bytes.capacity() == 0 && end - idx == _num_elems) {
            // We assume each row is roughly the same size and then estimate the required buffer size.
            size_t chunk_size = config::vector_chunk_size;
            size_t read_rows = std::min<size_t>(end - idx, chunk_size);
            size_t reserve_length = config::data_page_size * 1.5 / read_rows * chunk_size;
            bytes.reserve(reserve_length);
        }

        bytes.resize(old_bytes_size + append_bytes_length);
        // TODO: need did some optimize for large memory copy
        memcpy(bytes.data() + old_bytes_size, _data.get_data() + offset(idx), append_bytes_length);

        if (dst->is_nullable()) {
            auto& null_data = down_cast<NullableColumn*>(dst)->null_column_data();
            size_t null_data_size = null_data.size();
            down_cast<NullableColumn*>(dst)->null_column_data().resize(null_data_size + end - idx, 0);
        }

#ifndef NDEBUG
        dst->check_or_die();
#endif
        return true;
    } else {
        DCHECK(false) << "unreachable path";
        return false;
    }
}

template <LogicalType Type>
void BinaryPlainPageDecoder<Type>::batch_string_at_index(Slice* dst, const int32_t* idx, size_t size) const {
    // On macOS ARM64 with ASAN, the _parsed_datas optimization causes issues
    // where cached Slices may point to freed memory. Disable the optimization
    // and always use string_at_index for safety.
#ifdef __APPLE__
    for (int i = 0; i < size; ++i) {
        dst[i] = string_at_index(idx[i]);
    }
#else
    if (_parsed_datas.has_value()) {
        const std::vector<Slice>& parsed_data = *_parsed_datas;
        static_assert(sizeof(Slice) == sizeof(__int128));
#ifdef __SSE4_2__
        const Slice* parsed_data_ptr = parsed_data.data();
#pragma GCC unroll 2
        for (int i = 0; i < size; ++i) {
            DCHECK_LT(idx[i], parsed_data.size());
            __m128i slice = _mm_loadu_si128((__m128i_u*)(parsed_data_ptr + idx[i]));
            _mm_storeu_si128((__m128i_u*)(dst + i), slice);
        }
#else
        for (int i = 0; i < size; ++i) {
            DCHECK_LT(idx[i], parsed_data.size());
            dst[i] = parsed_data[idx[i]];
        }
#endif
    } else {
        for (int i = 0; i < size; ++i) {
            dst[i] = string_at_index(idx[i]);
        }
    }
#endif
}

template <LogicalType Type>
Status BinaryPlainPageDecoder<Type>::next_batch_with_filter(
        Column* column, const SparseRange<>& range, const std::vector<const ColumnPredicate*>& compound_and_predicates,
        NullColumn* null, uint8_t* selection, uint16_t* selected_idx, bool* data_filtered) {
    // don't support nullable column right now
    DCHECK(null == nullptr);
    DCHECK(_parsed);
    if (PREDICT_FALSE(_cur_idx >= _num_elems)) {
        return Status::OK();
    }

    if constexpr (Type != TYPE_VARCHAR) {
        *data_filtered = false;
        return next_batch(range, column);
    } else {
        *data_filtered = true;

        size_t to_read = std::min(range.span_size(), _num_elems - _cur_idx);
        SparseRangeIterator<> iter = range.new_iterator();
        bool append_status = true;
        size_t index = 0;
        while (to_read > 0) {
            _cur_idx = iter.begin();
            Range<> r = iter.next(to_read);
            size_t length = r.span_size();
            size_t end = _cur_idx + length;
            append_status &= next_range_with_filter(_cur_idx, end, column, compound_and_predicates,
                                                    null != nullptr ? null->get_data().data() + index : nullptr,
                                                    selection + index, selected_idx + index);
            index += length;
            to_read -= length;
            _cur_idx = end;
        }
        if (append_status) {
            return Status::OK();
        } else {
            return Status::InternalError("BinaryPlainPageDecoder::next_batch_with_filter error!");
        }
    }
}

template <LogicalType Type>
bool BinaryPlainPageDecoder<Type>::next_range_with_filter(
        uint32_t idx, uint32_t end, Column* dst, const std::vector<const ColumnPredicate*>& compound_and_predicates,
        uint8_t* null, uint8_t* selection, uint16_t* selected_idx) {
    DCHECK(null == nullptr);
    if constexpr (Type == TYPE_VARCHAR) {
        uint32_t num_rows = end - idx;
        if (num_rows == 0) {
            return true;
        }

        BinaryColumn::Offsets temp_offsets;
        temp_offsets.resize(num_rows + 1);
        temp_offsets[0] = 0;

        const uint32_t page_data_offset = offset_uncheck(idx);
        for (uint32_t i = idx; i < end - 1; i++) {
#if __BYTE_ORDER == __LITTLE_ENDIAN
            auto offset = _offsets_ptr[i + 1];
#else
            // direct call offset_uncheck() will break auto-vectorized
            // maybe we can remove this condition compile after we upgrade the toolchain
            auto offset = offset_uncheck(i + 1);
#endif

            uint32_t current_offset = offset - page_data_offset;
            temp_offsets[i - idx + 1] = current_offset;
        }

        for (uint32_t i = end - 1; i < end; i++) {
            uint32_t current_offset = offset(i + 1) - page_data_offset;
            temp_offsets[i - idx + 1] = current_offset;
        }

        size_t data_length = temp_offsets.back();
        const void* data_ptr = _data.get_data() + page_data_offset;

        // todo: merge null column if necessary
        // Create a heap-allocated BinaryColumn to avoid stack object lifetime issues
        auto temp_column = BinaryColumn::create(data_ptr, data_length, std::move(temp_offsets));

        Status predicate_result = compound_and_predicates_evaluate(compound_and_predicates, temp_column.get(),
                                                                   selection, selected_idx, 0, num_rows);
        auto data_column = ColumnHelper::get_data_column(dst);
        auto& bytes = down_cast<BinaryColumn*>(data_column)->get_bytes();
        auto& offsets = down_cast<BinaryColumn*>(data_column)->get_offset();
        DCHECK_GE(offsets.size(), 1);

        uint32_t begin_offset = offsets.back();
        size_t original_offset_size = offsets.size();

        uint32_t selected_count = SIMD::count_nonzero(selection, num_rows);

        if (selected_count == 0) {
            return true;
        }

        // todo: optimize case when selected_count == num_rows
        offsets.reserve(original_offset_size + selected_count);

        auto& temp_offset_in_column = temp_column->get_offset();

        size_t total_bytes_to_append = 0;
        for (uint32_t i = 0; i < num_rows; i++) {
            if (selection[i]) {
                total_bytes_to_append += temp_offset_in_column[i + 1];
            }
        }

        bytes.reserve(bytes.size() + total_bytes_to_append);

        uint32_t current_offset = begin_offset;
        for (uint32_t i = 0; i < num_rows; i++) {
            if (selection[i]) {
                uint32_t row_start = page_data_offset + temp_offset_in_column[i];
                uint32_t row_end = page_data_offset + temp_offset_in_column[i + 1];
                uint32_t row_length = temp_offset_in_column[i + 1] - temp_offset_in_column[i];

                bytes.insert(bytes.end(), _data.get_data() + row_start, _data.get_data() + row_end);
                current_offset += row_length;
                offsets.push_back(current_offset);
            }
        }

#ifndef NDEBUG
        dst->check_or_die();
#endif
        return true;
    } else {
        DCHECK(false) << "unreachable path";
        return false;
    }
}

template <LogicalType Type>
Status BinaryPlainPageDecoder<Type>::read_by_rowids(const ordinal_t first_ordinal_in_page, const rowid_t* rowids,
                                                    size_t* count, Column* column) {
    DCHECK(_parsed);
    if (PREDICT_FALSE(*count == 0)) {
        return Status::OK();
    }
    size_t total = *count;
    static_assert(sizeof(Slice) == sizeof(int128_t));
    auto slices_data = std::make_unique_for_overwrite<uint8_t[]>(total * sizeof(Slice));
    Slice* slices = reinterpret_cast<Slice*>(slices_data.get());
    if constexpr (Type == TYPE_CHAR) {
        for (size_t i = 0; i < total; i++) {
            ordinal_t ord = rowids[i] - first_ordinal_in_page;
            if (UNLIKELY(ord >= _num_elems)) {
                total = i;
                break;
            }
            Slice element = string_at_index(ord);
            element.size = strnlen(element.data, element.size);
            slices[i] = element;
        }
    } else {
        for (size_t i = 0; i < total; i++) {
            ordinal_t ord = rowids[i] - first_ordinal_in_page;
            if (UNLIKELY(ord >= _num_elems)) {
                total = i;
                break;
            }
            slices[i] = string_at_index(ord);
        }
    }

    class SliceContainerAdaptor {
    public:
        using value_type = Slice;
        SliceContainerAdaptor(Slice* slices, size_t size) : _slices(slices), _size(size) {}

        Slice* data() const { return _slices; }
        size_t size() const { return _size; }

    private:
        Slice* _slices;
        size_t _size;
    };

    SliceContainerAdaptor adaptor(slices, total);
    if (column->append_strings(adaptor)) {
        *count = total;
        return Status::OK();
    }
    return Status::InvalidArgument("Column::append_strings() not supported");
}

template class BinaryPlainPageDecoder<TYPE_CHAR>;
template class BinaryPlainPageDecoder<TYPE_VARCHAR>;
template class BinaryPlainPageDecoder<TYPE_HLL>;
template class BinaryPlainPageDecoder<TYPE_OBJECT>;
template class BinaryPlainPageDecoder<TYPE_PERCENTILE>;
template class BinaryPlainPageDecoder<TYPE_JSON>;
template class BinaryPlainPageDecoder<TYPE_VARBINARY>;

} // namespace starrocks
