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

#include <cstddef>
#include <cstdint>

namespace starrocks {

// RowSourceMask stores a uint16_t data that represents the source (segment iterator in compaction) of each row
// and the aggregation state of the row.
//
// RowSourceMask is used to merge non-key columns without comparing keys in vertical compaction,
// and to improve compaction efficiency and reduce memory usage.
//
// The lower 15 bits represent the source number, so the count of sources must be be less than or equal to 0x7FFF.
// The higher 1 bit represents the aggregation flag, and is used in agg and uniq data model.
// Agg flag is 1 means that the key of the row is the same as the previous row.
struct RowSourceMask {
    RowSourceMask(uint16_t data) : data(data) {}

    RowSourceMask(uint16_t source_num, bool agg_flag) {
        set_source_num(source_num);
        set_agg_flag(agg_flag);
    }

    uint16_t get_source_num() const { return data & MASK_NUMBER; }

    bool get_agg_flag() const { return (data & MASK_FLAG) != 0; }

    void set_source_num(uint16_t source_num) { data = (data & MASK_FLAG) | (source_num & MASK_NUMBER); }

    void set_agg_flag(bool agg_flag) { data = agg_flag ? data | MASK_FLAG : data & ~MASK_FLAG; }

    uint16_t data = 0;

    static constexpr size_t MAX_SOURCES = 0x7FFF;
    static constexpr uint16_t MASK_NUMBER = 0x7FFF;
    static constexpr uint16_t MASK_FLAG = 0x8000;
};

} // namespace starrocks
