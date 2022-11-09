// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/rowset/column_decoder.h"

#include "column/binary_column.h"
#include "column/nullable_column.h"
#include "common/compiler_util.h"
#include "fmt/core.h"
#include "gutil/casts.h"
#include "storage/rowset/dictcode_column_iterator.h"

namespace starrocks {
Status ColumnDecoder::encode_to_global_id(vectorized::Column* datas, vectorized::Column* codes) {
    const auto ed = _global_dict->end();
    size_t num_rows = datas->size();
    codes->resize(num_rows);
    if (datas->is_nullable()) {
        auto* nullable_column = down_cast<vectorized::NullableColumn*>(datas);
        auto* binary_column = down_cast<vectorized::BinaryColumn*>(nullable_column->data_column().get());
        auto* lowcard_nullcolumn = down_cast<vectorized::NullableColumn*>(codes);
        auto* lowcard_datacolumn = down_cast<vectorized::LowCardDictColumn*>(lowcard_nullcolumn->data_column().get());
        auto& lowcard_data = lowcard_datacolumn->get_data();
        const auto& null_data = nullable_column->null_column_data();
        for (int i = 0; i < num_rows; ++i) {
            if (null_data[i] == 0) {
                auto iter = _global_dict->find(binary_column->get_slice(i));
                if (LIKELY(iter != ed)) {
                    lowcard_data[i] = iter->second;
                } else {
                    return Status::InternalError(fmt::format("Not Found string in global dict: {}",
                                                             binary_column->get_slice(i).to_string()));
                }
            }
        }
        // set null info to the the lowcardinality column
        lowcard_nullcolumn->set_has_null(nullable_column->has_null());
        lowcard_nullcolumn->null_column()->swap_column(*nullable_column->null_column());
    } else {
        auto* binary_column = down_cast<vectorized::BinaryColumn*>(datas);
        auto* lowcard_column = down_cast<vectorized::LowCardDictColumn*>(codes);
        auto& lowcard_data = lowcard_column->get_data();
        for (int i = 0; i < num_rows; ++i) {
            auto iter = _global_dict->find(binary_column->get_slice(i));
            if (LIKELY(iter != ed)) {
                lowcard_data[i] = iter->second;
            } else {
                return Status::InternalError(
                        fmt::format("Not Found string in global dict: {}", binary_column->get_slice(i).to_string()));
            }
        }
    }
    return Status::OK();
}

void ColumnDecoder::check_global_dict() {
    if (_global_dict && _all_page_dict_encoded) {
        std::vector<int16_t> code_convert_map;
        auto* scalar_iter = down_cast<ScalarColumnIterator*>(_iter);
        Status st = GlobalDictCodeColumnIterator::build_code_convert_map(scalar_iter, _global_dict, &code_convert_map);
        if (st.ok()) {
            _code_convert_map = std::move(code_convert_map);
        } else {
            LOG(INFO) << st.to_string() << " will force the use of the global dictionary encoding";
        }
    }
}

} // namespace starrocks
