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

} // namespace starrocks::segment_v2