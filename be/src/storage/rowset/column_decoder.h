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

#include <optional>

#include "column/binary_column.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "runtime/global_dict/types.h"
#include "storage/rowset/column_iterator.h"

namespace starrocks {

// To handle DictDecode
class ColumnDecoder {
public:
    ColumnDecoder() = default;

    void set_iterator(ColumnIterator* iter) { _iter = iter; }

    Status decode_dict_codes(const Column& codes, Column* words) {
        DCHECK(_iter != nullptr);
        return _iter->decode_dict_codes(codes, words);
    }

    Status decode_values_by_rowid(const Column& rowids, Column* values) {
        DCHECK(_iter != nullptr);
        return _iter->fetch_values_by_rowid(rowids, values);
    }

    void set_all_page_dict_encoded(bool all_page_dict_encoded) { _all_page_dict_encoded = all_page_dict_encoded; }
    void set_global_dict(GlobalDictMap* global_dict) { _global_dict = global_dict; }
    // check global dict is superset of local dict
    void check_global_dict();

    // encode string column to global dictionary id
    Status encode_to_global_id(Column* datas, Column* codes);

    bool need_force_encode_to_global_id() const {
        return (!_all_page_dict_encoded && _global_dict) || (!_code_convert_map.has_value() && _global_dict);
    }

    int16_t* code_convert_data() { return _code_convert_map.has_value() ? _code_convert_map->data() + 1 : nullptr; }

private:
    Status _encode_string_to_global_id(Column* datas, Column* codes);

    Status _encode_array_to_global_id(Column* datas, Column* codes);

private:
    std::optional<std::vector<int16_t>> _code_convert_map;
    ColumnIterator* _iter = nullptr;
    GlobalDictMap* _global_dict = nullptr;
    bool _all_page_dict_encoded = false;
};

} // namespace starrocks
