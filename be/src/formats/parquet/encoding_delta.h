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

#include <cstdint>

#include "column/column.h"
#include "common/status.h"
#include "formats/parquet/encoding.h"
#include "util/bit_stream_utils.h"
#include "util/slice.h"

namespace starrocks::parquet {

// DELTA_BINARY_PACKED decoder
template <typename T>
class DeltaBinaryPackedDecoder final : public Decoder {
public:
    static_assert(std::is_integral_v<T>, "T must be an integral type");
    using UT = std::make_unsigned_t<T>;
    DeltaBinaryPackedDecoder() = default;
    ~DeltaBinaryPackedDecoder() override = default;

    Status set_data(const Slice& data) override {
        _data = data;
        _offset = 0;
        _bit_reader.reset((uint8_t*)(data.data), data.size);
        _header_inited = false;
        return Status::OK();
    }

    Status InitHeader() {
        BitReader* decoder_ = &_bit_reader;
        if (_header_inited) {
            return Status::OK();
        }
        if (!decoder_->GetVlqInt(&values_per_block_) || !decoder_->GetVlqInt(&mini_blocks_per_block_) ||
            !decoder_->GetVlqInt(&total_value_count_) || !decoder_->GetZigZagVlqInt(&last_value_)) {
            return Status::Corruption("InitHeader failed");
        }

        if (values_per_block_ == 0) {
            return Status::Corruption("cannot have zero value per block");
        }
        if (values_per_block_ % 128 != 0) {
            return Status::Corruption("the number of values in a block must be multiple of 128, but it's " +
                                      std::to_string(values_per_block_));
        }
        if (mini_blocks_per_block_ == 0) {
            return Status::Corruption("cannot have zero miniblock per block");
        }
        values_per_mini_block_ = values_per_block_ / mini_blocks_per_block_;
        if (values_per_mini_block_ == 0) {
            throw Status::Corruption("cannot have zero value per miniblock");
        }
        if (values_per_mini_block_ % 32 != 0) {
            throw Status::Corruption("the number of values in a miniblock must be multiple of 32, but it's " +
                                     std::to_string(values_per_mini_block_));
        }

        total_values_remaining_ = total_value_count_;
        delta_bit_widths_.resize(mini_blocks_per_block_);
        first_block_initialized_ = false;
        values_remaining_current_mini_block_ = 0;
        _header_inited = true;
    }

    Status InitBlock() {
        BitReader* decoder_ = &_bit_reader;
        DCHECK_GT(total_values_remaining_, 0) << "InitBlock called at EOF";

        if (!decoder_->GetZigZagVlqInt(&min_delta_)) {
            return Status::Corruption("InitBlock EOF");
        }

        // read the bitwidth of each miniblock
        uint8_t* bit_width_data = (uint8_t*)delta_bit_widths_->data();
        for (uint32_t i = 0; i < mini_blocks_per_block_; ++i) {
            if (!decoder_->GetAligned<uint8_t>(1, bit_width_data + i)) {
                return Status::Corruption("Decode bit-width EOF");
            }
            // Note that non-conformant bitwidth entries are allowed by the Parquet spec
            // for extraneous miniblocks in the last block (GH-14923), so we check
            // the bitwidths when actually using them (see InitMiniBlock()).
        }
        mini_block_idx_ = 0;
        first_block_initialized_ = true;
        RETURN_IF_ERROR(InitMiniBlock(bit_width_data[0]));
        return Status::OK();
    }

    Status InitMiniBlock(int bit_width) {
        static constexpr int kMaxDeltaBitWidth = static_cast<int>(sizeof(T) * 8);
        if (PREDICT_FALSE(bit_width > kMaxDeltaBitWidth)) {
            return Status::Corruption("delta bit width larger than integer bit width");
        }
        delta_bit_width_ = bit_width;
        values_remaining_current_mini_block_ = values_per_mini_block_;
    }

    Status GetInternal(T* buffer, int max_values) {
        BitReader* decoder_ = &_bit_reader;
        max_values = static_cast<int>(std::min<int64_t>(max_values, total_values_remaining_));
        if (max_values == 0) {
            return Status::OK();
        }

        int i = 0;

        if (PREDICT_FALSE(!first_block_initialized_)) {
            // This is the first time we decode this data page, first output the
            // last value and initialize the first block.
            buffer[i++] = last_value_;
            if (PREDICT_FALSE(i == max_values)) {
                // When i reaches max_values here we have two different possibilities:
                // 1. total_value_count_ == 1, which means that the page may have only
                //    one value (encoded in the header), and we should not initialize
                //    any block, nor should we skip any padding bits below.
                // 2. total_value_count_ != 1, which means we should initialize the
                //    incoming block for subsequent reads.
                if (total_value_count_ != 1) {
                    RETURN_IF_ERROR(InitBlock());
                }
                total_values_remaining_ -= max_values;
                this->num_values_ -= max_values;
                return Status::OK();
            }
            RETURN_IF_ERROR(InitBlock());
        }

        DCHECK(first_block_initialized_);
        while (i < max_values) {
            // Ensure we have an initialized mini-block
            if (PREDICT_FALSE(values_remaining_current_mini_block_ == 0)) {
                ++mini_block_idx_;
                if (mini_block_idx_ < mini_blocks_per_block_) {
                    InitMiniBlock(delta_bit_widths_->data()[mini_block_idx_]);
                } else {
                    InitBlock();
                }
            }

            int values_decode = std::min(values_remaining_current_mini_block_, static_cast<uint32_t>(max_values - i));
            if (decoder_->GetBatch(delta_bit_width_, buffer + i, values_decode)) {
                return Status::Corruption("GetBatch failed");
            }
            for (int j = 0; j < values_decode; ++j) {
                // Addition between min_delta, packed int and last_value should be treated as
                // unsigned addition. Overflow is as expected.
                buffer[i + j] =
                        static_cast<UT>(min_delta_) + static_cast<UT>(buffer[i + j]) + static_cast<UT>(last_value_);
                last_value_ = buffer[i + j];
            }
            values_remaining_current_mini_block_ -= values_decode;
            i += values_decode;
        }
        total_values_remaining_ -= max_values;
        this->num_values_ -= max_values;

        if (PREDICT_FALSE(total_values_remaining_ == 0)) {
            uint32_t padding_bits = values_remaining_current_mini_block_ * delta_bit_width_;
            // skip the padding bits
            if (!decoder_->Advance(padding_bits)) {
                return Status::Corruption("Advance failed");
            }
            values_remaining_current_mini_block_ = 0;
        }
        return Status::OK();
    }

    Status next_batch(size_t count, ColumnContentType content_type, Column* dst, const FilterData* filter) override {
        count = std::min<uint32_t>(count, total_values_remaining_);
        dst->resize(count);
        // TODO: GetInternal
        return Status::OK();
    }

    Status skip(size_t values_to_skip) override { return Status::OK(); }

private:
    // ============
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

    // ============
    Slice _data;
    size_t _offset = 0;
    bool _header_inited = false;
    BitReader _bit_reader;
};

} // namespace starrocks::parquet
