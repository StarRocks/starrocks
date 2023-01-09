#include "storage/rowset/binary_plain_page.h"

#include "column/binary_column.h"

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
    } else {
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

template class BinaryPlainPageDecoder<TYPE_CHAR>;
template class BinaryPlainPageDecoder<TYPE_VARCHAR>;
template class BinaryPlainPageDecoder<TYPE_HLL>;
template class BinaryPlainPageDecoder<TYPE_OBJECT>;
template class BinaryPlainPageDecoder<TYPE_PERCENTILE>;
template class BinaryPlainPageDecoder<TYPE_JSON>;
template class BinaryPlainPageDecoder<TYPE_VARBINARY>;

} // namespace starrocks
