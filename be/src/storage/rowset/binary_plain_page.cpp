#include "storage/rowset/binary_plain_page.h"

#include <cstring>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "gutil/casts.h"
#include "types/logical_type.h"

namespace starrocks {

template <LogicalType Type>
Status BinaryPlainPageDecoder<Type>::next_batch(size_t* count, Column* dst) {
    SparseRange read_range;
    uint32_t begin = current_index();
    read_range.add(Range(begin, begin + *count));
    RETURN_IF_ERROR(next_batch(read_range, dst));
    *count = current_index() - begin;
    return Status::OK();
}

template <LogicalType Type>
Status BinaryPlainPageDecoder<Type>::next_batch(const SparseRange& range, Column* dst) {
    DCHECK(_parsed);
    if (PREDICT_FALSE(_cur_idx >= _num_elems)) {
        return Status::OK();
    }

    size_t to_read = std::min(range.span_size(), _num_elems - _cur_idx);
    std::vector<Slice> strs;
    strs.reserve(to_read);
    SparseRangeIterator iter = range.new_iterator();
    if constexpr (Type == TYPE_CHAR) {
        while (to_read > 0) {
            _cur_idx = iter.begin();
            Range r = iter.next(to_read);
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
            Range r = iter.next(to_read);
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
            Range r = iter.next(to_read);
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

template class BinaryPlainPageDecoder<TYPE_CHAR>;
template class BinaryPlainPageDecoder<TYPE_VARCHAR>;
template class BinaryPlainPageDecoder<TYPE_HLL>;
template class BinaryPlainPageDecoder<TYPE_OBJECT>;
template class BinaryPlainPageDecoder<TYPE_PERCENTILE>;
template class BinaryPlainPageDecoder<TYPE_JSON>;
template class BinaryPlainPageDecoder<TYPE_VARBINARY>;

} // namespace starrocks
