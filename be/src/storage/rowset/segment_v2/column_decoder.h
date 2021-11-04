// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "storage/rowset/segment_v2/column_iterator.h"

namespace starrocks {
namespace segment_v2 {

// To handle DictDecode
class ColumnDecoder {
public:
    ColumnDecoder() : _iter(nullptr) {}
    explicit ColumnDecoder(ColumnIterator* iter) : _iter(iter) {}

    void set_iterator(ColumnIterator* iter) { _iter = iter; }

    Status decode_dict_codes(const vectorized::Column& codes, vectorized::Column* words) {
        DCHECK(_iter != nullptr);
        return _iter->decode_dict_codes(codes, words);
    }

    Status decode_values_by_rowid(const vectorized::Column& rowids, vectorized::Column* values) {
        DCHECK(_iter != nullptr);
        RETURN_IF_ERROR(_iter->fetch_values_by_rowid(rowids, values));
        return Status::OK();
    }

private:
    ColumnIterator* _iter;
};

} // namespace segment_v2
} // namespace starrocks
