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

#include "storage/rowset/column_decoder.h"

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/compiler_util.h"
#include "common/status.h"
#include "fmt/core.h"
#include "gutil/casts.h"
#include "storage/rowset/dictcode_column_iterator.h"

namespace starrocks {
Status ColumnDecoder::encode_to_global_id(Column* datas, Column* codes) {
    auto* data = datas;
    if (datas->is_nullable()) {
        auto* nullable_column = down_cast<NullableColumn*>(datas);
        data = nullable_column->data_column().get();
    }
    if (data->is_binary()) {
        return _encode_string_to_global_id(datas, codes);
    } else if (data->is_array()) {
        return _encode_array_to_global_id(datas, codes);
    }
    return Status::NotSupported("encode to global id not support type.");
}

Status ColumnDecoder::_encode_string_to_global_id(Column* datas, Column* codes) {
    const auto ed = _global_dict->end();
    size_t num_rows = datas->size();
    codes->resize(num_rows);
    if (datas->is_nullable()) {
        auto* nullable_column = down_cast<NullableColumn*>(datas);
        auto* binary_column = down_cast<BinaryColumn*>(nullable_column->data_column().get());
        auto* lowcard_nullcolumn = down_cast<NullableColumn*>(codes);
        auto* lowcard_datacolumn = down_cast<LowCardDictColumn*>(lowcard_nullcolumn->data_column().get());
        auto& lowcard_data = lowcard_datacolumn->get_data();
        const auto& null_data = nullable_column->null_column_data();
        for (int i = 0; i < num_rows; ++i) {
            if (null_data[i] == 0) {
                auto iter = _global_dict->find(binary_column->get_slice(i));
                if (LIKELY(iter != ed)) {
                    lowcard_data[i] = iter->second;
                } else {
                    // corner case:
                    // if this column is unique model, and it is also a value column (replace aggregate)
                    // the value won't found in global dict, then it will be replaced in other rowset value
                    lowcard_data[i] = 0;
                }
            }
        }
        // set null info to the the lowcardinality column
        lowcard_nullcolumn->set_has_null(nullable_column->has_null());
        lowcard_nullcolumn->null_column()->swap_column(*nullable_column->null_column());
    } else {
        auto* binary_column = down_cast<BinaryColumn*>(datas);
        auto* lowcard_column = down_cast<LowCardDictColumn*>(codes);
        auto& lowcard_data = lowcard_column->get_data();
        for (int i = 0; i < num_rows; ++i) {
            auto iter = _global_dict->find(binary_column->get_slice(i));
            if (LIKELY(iter != ed)) {
                lowcard_data[i] = iter->second;
            } else {
                // same reason as above
                lowcard_data[i] = 0;
            }
        }
    }
    return Status::OK();
}

Status ColumnDecoder::_encode_array_to_global_id(Column* datas, Column* codes) {
    if (datas->is_nullable()) {
        auto* nullable_column = down_cast<NullableColumn*>(datas);
        auto* lowcard_nullcolumn = down_cast<NullableColumn*>(codes);
        auto* array_column = down_cast<ArrayColumn*>(nullable_column->data_column().get());
        auto* lowcard_array_column = down_cast<ArrayColumn*>(lowcard_nullcolumn->data_column().get());

        lowcard_nullcolumn->set_has_null(nullable_column->has_null());
        lowcard_nullcolumn->null_column()->swap_column(*nullable_column->null_column());
        lowcard_array_column->offsets_column()->swap_column(*array_column->offsets_column());
        return _encode_string_to_global_id(array_column->elements_column().get(),
                                           lowcard_array_column->elements_column().get());
    } else {
        auto* array_column = down_cast<ArrayColumn*>(datas);
        auto* lowcard_array_column = down_cast<ArrayColumn*>(codes);
        lowcard_array_column->offsets_column()->swap_column(*array_column->offsets_column());
        return _encode_string_to_global_id(array_column->elements_column().get(),
                                           lowcard_array_column->elements_column().get());
    }
}

void ColumnDecoder::check_global_dict() {
    if (_global_dict && _all_page_dict_encoded) {
        std::vector<int16_t> code_convert_map;
        Status st = GlobalDictCodeColumnIterator::build_code_convert_map(_iter, _global_dict, &code_convert_map);
        if (st.ok()) {
            _code_convert_map = std::move(code_convert_map);
        } else {
            LOG(INFO) << st.to_string() << " will force the use of the global dictionary encoding";
        }
    }
}

} // namespace starrocks
