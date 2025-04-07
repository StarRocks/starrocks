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

#include "column/column.h"
#include "common/status.h"
#include "formats/parquet/encoding.h"
#include "util/slice.h"

namespace starrocks::parquet {

// DELTA_BINARY_PACKED decoder
template <typename T>
class DeltaBinaryPackedDecoder final : public Decoder {
public:
    DeltaBinaryPackedDecoder() = default;
    ~DeltaBinaryPackedDecoder() override = default;

    static Status decode(const std::string& buffer, Slice* value) {
        value->data = const_cast<char*>(buffer.data());
        value->size = buffer.size();
        return Status::OK();
    }

    Status set_data(const Slice& data) override {
        _data = data;
        _offset = 0;
        return Status::OK();
    }

    Status init_header() {}

    Status next_batch(size_t count, ColumnContentType content_type, Column* dst, const FilterData* filter) override {
        return Status::OK();
    }

    Status skip(size_t values_to_skip) override { return Status::OK(); }

private:
    uint32_t values_per_block_;
    uint32_t mini_blocks_per_block_;
    uint32_t values_per_mini_block_;
    uint32_t total_value_count_;

    uint32_t total_values_remaining_;
    // Remaining values in current mini block. If the current block is the last mini block,
    // values_remaining_current_mini_block_ may greater than total_values_remaining_.
    uint32_t values_remaining_current_mini_block_;

    // If the page doesn't contain any block, `first_block_initialized_` will
    // always be false. Otherwise, it will be true when first block initialized.
    bool first_block_initialized_;
    T min_delta_;
    uint32_t mini_block_idx_;
    std::string delta_bit_widths_;
    int delta_bit_width_;

    T last_value_;

    Slice _data;
    size_t _offset = 0;
};

} // namespace starrocks::parquet
