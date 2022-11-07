#include "storage/rowset/binary_plain_page.h"

#include "column/binary_column.h"

namespace starrocks {

template <FieldType Type>
Status BinaryPlainPageDecoder<Type>::next_batch(size_t* count, vectorized::Column* dst) {
    vectorized::SparseRange read_range;
    uint32_t begin = current_index();
    read_range.add(vectorized::Range(begin, begin + *count));
    RETURN_IF_ERROR(next_batch(read_range, dst));
    *count = current_index() - begin;
    return Status::OK();
}

template <FieldType Type>
Status BinaryPlainPageDecoder<Type>::next_batch(const vectorized::SparseRange& range, vectorized::Column* dst) {
    DCHECK(_parsed);
    if (PREDICT_FALSE(_cur_idx >= _num_elems)) {
        return Status::OK();
    }

    size_t to_read = std::min(range.span_size(), _num_elems - _cur_idx);
    std::vector<Slice> strs;
    strs.reserve(to_read);
    vectorized::SparseRangeIterator iter = range.new_iterator();
    if constexpr (Type == OLAP_FIELD_TYPE_CHAR) {
        while (to_read > 0) {
            _cur_idx = iter.begin();
            vectorized::Range r = iter.next(to_read);
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
            vectorized::Range r = iter.next(to_read);
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

template class BinaryPlainPageDecoder<OLAP_FIELD_TYPE_CHAR>;
template class BinaryPlainPageDecoder<OLAP_FIELD_TYPE_VARCHAR>;
template class BinaryPlainPageDecoder<OLAP_FIELD_TYPE_HLL>;
template class BinaryPlainPageDecoder<OLAP_FIELD_TYPE_OBJECT>;
template class BinaryPlainPageDecoder<OLAP_FIELD_TYPE_PERCENTILE>;
template class BinaryPlainPageDecoder<OLAP_FIELD_TYPE_JSON>;

} // namespace starrocks
