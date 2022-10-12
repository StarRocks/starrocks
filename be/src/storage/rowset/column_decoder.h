// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <optional>

#include "column/binary_column.h"
#include "column/vectorized_fwd.h"
#include "runtime/global_dict/types.h"
#include "storage/rowset/column_iterator.h"

namespace starrocks {

// To handle DictDecode
class ColumnDecoder {
public:
    ColumnDecoder() = default;

    void set_iterator(ColumnIterator* iter) { _iter = iter; }

    Status decode_dict_codes(const vectorized::Column& codes, vectorized::Column* words) {
        DCHECK(_iter != nullptr);
        return _iter->decode_dict_codes(codes, words);
    }

    Status decode_values_by_rowid(const vectorized::Column& rowids, vectorized::Column* values) {
        DCHECK(_iter != nullptr);
        return _iter->fetch_values_by_rowid(rowids, values);
    }

    void set_all_page_dict_encoded(bool all_page_dict_encoded) { _all_page_dict_encoded = all_page_dict_encoded; }
    void set_global_dict(vectorized::GlobalDictMap* global_dict) { _global_dict = global_dict; }
    // check global dict is superset of local dict
    void check_global_dict();

    Status encode_to_global_id(vectorized::Column* datas, vectorized::Column* codes);
    bool need_force_encode_to_global_id() const {
        return (!_all_page_dict_encoded && _global_dict) || (!_code_convert_map.has_value() && _global_dict);
    }

    int16_t* code_convert_data() { return _code_convert_map.has_value() ? _code_convert_map->data() + 1 : nullptr; }

private:
    std::optional<std::vector<int16_t>> _code_convert_map;
    ColumnIterator* _iter = nullptr;
    vectorized::GlobalDictMap* _global_dict = nullptr;
    bool _all_page_dict_encoded = false;
};

} // namespace starrocks
