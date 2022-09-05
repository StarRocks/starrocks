// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "column/column.h"

#include "common/statusor.h"

namespace starrocks::vectorized {

void Column::serialize_batch_with_null_masks(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                             uint32_t max_one_row_size, uint8_t* null_masks, bool has_null) {
    uint32_t* sizes = slice_sizes.data();

    if (!has_null) {
        for (size_t i = 0; i < chunk_size; ++i) {
            memcpy(dst + i * max_one_row_size + sizes[i], &has_null, sizeof(bool));
            sizes[i] += sizeof(bool) + serialize(i, dst + i * max_one_row_size + sizes[i] + sizeof(bool));
        }
    } else {
        for (size_t i = 0; i < chunk_size; ++i) {
            memcpy(dst + i * max_one_row_size + sizes[i], null_masks + i, sizeof(bool));
            sizes[i] += sizeof(bool);

            if (!null_masks[i]) {
                sizes[i] += serialize(i, dst + i * max_one_row_size + sizes[i]);
            }
        }
    }
}

StatusOr<ColumnPtr> Column::downgrade_helper_func(ColumnPtr* col) {
    auto ret = (*col)->downgrade();
    if (!ret.ok()) {
        return ret;
    } else if (ret.value() == nullptr) {
        return nullptr;
    } else {
        (*col) = ret.value();
        return nullptr;
    }
}

StatusOr<ColumnPtr> Column::upgrade_helper_func(ColumnPtr* col) {
    auto ret = (*col)->upgrade_if_overflow();
    if (!ret.ok()) {
        return ret;
    } else if (ret.value() == nullptr) {
        return nullptr;
    } else {
        (*col) = ret.value();
        return nullptr;
    }
}

} // namespace starrocks::vectorized
