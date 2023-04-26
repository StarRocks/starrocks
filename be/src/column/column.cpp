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

#include <fmt/format.h>

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

bool Column::empty_null_in_complex_column(const Filter& null_data, const std::vector<uint32_t>& offsets) {
    DCHECK(null_data.size() == this->size());
    if (!is_array() && !is_map()) {
        throw std::runtime_error("empty_null_in_complex_column() only works for array and map column.");
    }
    bool need_empty = false;
    size_t size = this->size();
    if (size + 1 != offsets.size()) {
        throw std::runtime_error(
                fmt::format("inputs offsets' size {} != the column's offsets' size {}.", offsets.size(), size + 1));
    }
    // TODO: optimize it using SIMD
    for (auto i = 0; i < size && !need_empty; ++i) {
        if (null_data[i] && offsets[i + 1] != offsets[i]) {
            need_empty = true;
        }
    }
    // TODO: copy too much may result in worse performance.
    if (need_empty) {
        auto new_column = clone_empty();
        uint32_t from = 0;
        uint32_t to = size;
        while (from < to) {
            uint32_t new_from = from + 1;
            while (new_from < to && null_data[from] == null_data[new_from]) {
                ++new_from;
            }
            if (null_data[from]) {
                new_column->append_default(new_from - from);
            } else {
                new_column->append(*this, from, new_from - from);
            }
            from = new_from;
        }

        swap_column(*new_column.get());
    }
    return need_empty;
}

} // namespace starrocks
