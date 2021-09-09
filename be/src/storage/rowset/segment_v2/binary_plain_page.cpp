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

template class BinaryPlainPageDecoder<OLAP_FIELD_TYPE_CHAR>;
template class BinaryPlainPageDecoder<OLAP_FIELD_TYPE_VARCHAR>;
template class BinaryPlainPageDecoder<OLAP_FIELD_TYPE_HLL>;
template class BinaryPlainPageDecoder<OLAP_FIELD_TYPE_OBJECT>;
template class BinaryPlainPageDecoder<OLAP_FIELD_TYPE_PERCENTILE>;

} // namespace starrocks::segment_v2
