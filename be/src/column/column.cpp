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

#include "column/column.h"

#include "common/statusor.h"

namespace starrocks {

void Column::serialize_batch_with_null_masks(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                             uint32_t max_one_row_size, uint8_t* null_masks, bool has_null) {
    uint32_t* sizes = slice_sizes.data();

    if (!has_null) {
        for (size_t i = 0; i < chunk_size; ++i) {
            memcpy(dst + i * max_one_row_size + sizes[i], &has_null, sizeof(bool));
            sizes[i] += static_cast<uint32_t>(sizeof(bool)) +
                        serialize(i, dst + i * max_one_row_size + sizes[i] + sizeof(bool));
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

} // namespace starrocks
