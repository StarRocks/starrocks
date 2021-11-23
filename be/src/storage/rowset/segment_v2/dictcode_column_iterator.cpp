// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/rowset/segment_v2/dictcode_column_iterator.h"

#include "storage/rowset/segment_v2/scalar_column_iterator.h"

namespace starrocks::segment_v2 {

Status GlobalDictCodeColumnIterator::_build_to_global_dict() {
    DCHECK(_col_iter->all_page_dict_encoded());

    // we only have to build code mapping once
    if (_local_to_global_holder.size() > 0) {
        return Status::OK();
    }
    auto file_column_iter = down_cast<ScalarColumnIterator*>(_col_iter);
    int dict_size = file_column_iter->dict_size();

    auto column = vectorized::BinaryColumn::create();

    int dict_codes[dict_size];
    for (int i = 0; i < dict_size; ++i) {
        dict_codes[i] = i;
    }

    file_column_iter->decode_dict_codes(dict_codes, dict_size, column.get());

    _local_to_global_holder.resize(dict_size + 2);
    std::fill(_local_to_global_holder.begin(), _local_to_global_holder.end(), 0);
    _local_to_global = _local_to_global_holder.data() + 1;

    for (int i = 0; i < dict_size; ++i) {
        auto slice = column->get_slice(i);
        auto res = _global_dict->find(slice);
        if (res == _global_dict->end()) {
            if (slice.size > 0) {
                return Status::InternalError(fmt::format("not found slice:{} in global dict", slice.data));
            }
        } else {
            _local_to_global[dict_codes[i]] = res->second;
        }
    }
    return Status::OK();
}

void GlobalDictCodeColumnIterator::_init_local_dict_col() {
    _local_dict_code_col = std::make_unique<vectorized::Int32Column>();
    if (_opts.is_nullable) {
        _local_dict_code_col =
                vectorized::NullableColumn::create(std::move(_local_dict_code_col), vectorized::NullColumn::create());
    }
}

auto GlobalDictCodeColumnIterator::_get_local_dict_col_container(Column* column)
        -> const LowCardDictColumn::Container& {
    LowCardDictColumn* dict_column = nullptr;
    if (_opts.is_nullable) {
        auto nullable_column = down_cast<vectorized::NullableColumn*>(column);
        dict_column = down_cast<LowCardDictColumn*>(nullable_column->data_column().get());
        const auto& null_data = nullable_column->immutable_null_column_data();
        int row_sz = null_data.size();
        // TODO: If we can ensure that the null value of data is the default value,
        // then this loop can be removed
        for (int i = 0; i < row_sz; ++i) {
            dict_column->get_data()[i] = null_data[i] ? 0 : dict_column->get_data()[i];
        }
    } else {
        dict_column = down_cast<LowCardDictColumn*>(column);
    }
    return dict_column->get_data();
}

} // namespace starrocks::segment_v2