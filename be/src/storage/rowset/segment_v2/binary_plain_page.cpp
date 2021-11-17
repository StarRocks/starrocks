#include "storage/rowset/segment_v2/binary_plain_page.h"

#include "column/binary_column.h"

namespace starrocks::segment_v2 {

template <FieldType Type>
Status BinaryPlainPageDecoder<Type>::next_batch(size_t* count, vectorized::Column* dst) {
    DCHECK(_parsed);
    if (PREDICT_FALSE(_cur_idx >= _num_elems)) {
        *count = 0;
        return Status::OK();
    }
    *count = std::min(*count, static_cast<size_t>(_num_elems - _cur_idx));
    std::vector<Slice> strs;
    strs.reserve(*count);
    size_t end = _cur_idx + *count;
    if constexpr (Type == OLAP_FIELD_TYPE_CHAR) {
        for (/**/; _cur_idx < end; _cur_idx++) {
            Slice s = string_at_index(_cur_idx);
            s.size = strnlen(s.data, s.size);
            strs.emplace_back(s);
        }
        if (dst->append_strings(strs)) {
            return Status::OK();
        }
    } else {
        for (/**/; _cur_idx < end; _cur_idx++) {
            strs.emplace_back(string_at_index(_cur_idx));
        }
        if (dst->append_continuous_strings(strs)) {
            return Status::OK();
        }
    }
    return Status::InvalidArgument("Column::append_strings() not supported");
}

template <FieldType Type>
Status BinaryPlainPageDecoder<Type>::next_batch(vectorized::SparseRange& range, vectorized::Column* dst) {
    DCHECK(_parsed);
    if (PREDICT_FALSE(_cur_idx >= _num_elems)) {
        return Status::OK();
    }
    /*
    if (_cur_idx != range.begin()) {
        LOG(INFO) << "binary plain page cur_idx is not equal begin of range";
        return Status::InternalError("binary plain page decoder cur_idx error");
    }
    */
    size_t nread = std::min(range.span_size(), _num_elems - _cur_idx);
    std::vector<Slice> strs;
    strs.reserve(nread);
    vectorized::SparseRangeIterator iter = range.new_iterator();
    if constexpr (Type == OLAP_FIELD_TYPE_CHAR) {
        while(iter.has_more() && _cur_idx < _num_elems) {
            //LOG(INFO) << "type char";
            _cur_idx = iter.begin();
            vectorized::Range r = iter.next(nread);
            size_t end = _cur_idx + r.span_size();
            for (; _cur_idx < end; _cur_idx++) {
                Slice s = string_at_index(_cur_idx);
                s.size = strnlen(s.data, s.size);
                strs.emplace_back(s);
            }
        }
        if (dst->append_strings(strs)) {
            return Status::OK();
        }
    } else {
        while(iter.has_more() && _cur_idx < _num_elems) {
            _cur_idx = iter.begin();
            vectorized::Range r = iter.next(nread);
            //LOG(INFO) << "type other" << ", _cur_idx is " << _cur_idx << ", range size is " << r.span_size();
            size_t end = _cur_idx + r.span_size();
            //LOG(INFO) << "_cur_idx is " << _cur_idx << ", end_idx is " << end;
            for (; _cur_idx < end; _cur_idx++) {
                strs.emplace_back(string_at_index(_cur_idx));
            }
        }
        if (dst->append_continuous_strings(strs)) {
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

} // namespace starrocks::segment_v2
