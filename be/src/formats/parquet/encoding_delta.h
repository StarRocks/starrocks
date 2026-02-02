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

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>

#include "base/simd/delta_decode.h"
#include "base/string/slice.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "common/status.h"
#include "formats/parquet/encoding.h"
#include "util/bit_stream_utils.h"

namespace starrocks::parquet {

// ----------------------------------------------------------------------
// DELTA_BINARY_PACKED encoder

/// DeltaBitPackEncoder is an encoder for the DeltaBinary Packing format
/// as per the parquet spec. See:
/// https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-encoding-delta_binary_packed--5
///
/// Consists of a header followed by blocks of delta encoded values binary packed.
///
///  Format
///    [header] [block 1] [block 2] ... [block N]
///
///  Header
///    [block size] [number of mini blocks per block] [total value count] [first value]
///
///  Block
///    [min delta] [list of bitwidths of the mini blocks] [miniblocks]
///
/// Sets aside bytes at the start of the internal buffer where the header will be written,
/// and only writes the header when FlushValues is called before returning it.
///
/// To encode a block, we will:
///
/// 1. Compute the differences between consecutive elements. For the first element in the
/// block, use the last element in the previous block or, in the case of the first block,
/// use the first value of the whole sequence, stored in the header.
///
/// 2. Compute the frame of reference (the minimum of the deltas in the block). Subtract
/// this min delta from all deltas in the block. This guarantees that all values are
/// non-negative.
///
/// 3. Encode the frame of reference (min delta) as a zigzag ULEB128 int followed by the
/// bit widths of the mini blocks and the delta values (minus the min delta) bit packed
/// per mini block.
///
/// Supports only INT32 and INT64.

template <typename T>
class DeltaBinaryPackedEncoder final : public Encoder {
    // Maximum possible header size
    static constexpr uint32_t kMaxPageHeaderWriterSize = 32;
    static constexpr uint32_t kValuesPerBlock = std::is_same_v<int32_t, T> ? 128 : 256;
    static constexpr uint32_t kMiniBlocksPerBlock = 4;

public:
    using UT = std::make_unsigned_t<T>;

    DeltaBinaryPackedEncoder(const uint32_t values_per_block = kValuesPerBlock,
                             const uint32_t mini_blocks_per_block = kMiniBlocksPerBlock)
            : values_per_block_(values_per_block),
              mini_blocks_per_block_(mini_blocks_per_block),
              values_per_mini_block_(values_per_block / mini_blocks_per_block),
              deltas_(values_per_block),
              bits_buffer_((kMiniBlocksPerBlock + values_per_block) * sizeof(T)),
              bit_writer_(&bits_buffer_) {
        DCHECK(values_per_block_ % 128 == 0) << "the number of values in a block must be multiple of 128, but it's "
                                             << std::to_string(values_per_block_);
        DCHECK(values_per_mini_block_ % 32 == 0)
                << "the number of values in a miniblock must be multiple of 32, but it's "
                << std::to_string(values_per_mini_block_);
        DCHECK(values_per_block % mini_blocks_per_block == 0)
                << "the number of values per block % number of miniblocks per block must be 0, "
                   "but it's "
                << std::to_string(values_per_block % mini_blocks_per_block);
        // Reserve enough space at the beginning of the buffer for largest possible header.
        sink_.advance(kMaxPageHeaderWriterSize);
    }

    ~DeltaBinaryPackedEncoder() override = default;

    Slice build() override {
        FlushValues();
        return Slice(sink_.data() + sink_offset_, sink_.size() - sink_offset_);
    }

    Status append(const uint8_t* vals, size_t count) override {
        sink_offset_ = -1; // more values.
        Put(reinterpret_cast<const T*>(vals), static_cast<int>(count));
        return Status::OK();
    }

    std::string to_string() const override { return fmt::format("DeltaBinaryPackedEncoder<{}>", typeid(T).name()); }

private:
    const uint32_t values_per_block_;
    const uint32_t mini_blocks_per_block_;
    const uint32_t values_per_mini_block_;
    uint32_t values_current_block_{0};
    uint32_t total_value_count_{0};
    T first_value_{0};
    T current_value_{0};

    std::vector<T> deltas_;
    faststring sink_;
    faststring bits_buffer_;
    BitWriter bit_writer_;
    int sink_offset_{-1};

private:
    void Put(const T* src, int num_values) {
        if (num_values == 0) {
            return;
        }

        int idx = 0;
        if (total_value_count_ == 0) {
            current_value_ = src[0];
            first_value_ = current_value_;
            idx = 1;
        }
        total_value_count_ += num_values;

        while (idx < num_values) {
            T value = src[idx];
            // Calculate deltas. The possible overflow is handled by use of unsigned integers
            // making subtraction operations well-defined and correct even in case of overflow.
            // Encoded integers will wrap back around on decoding.
            // See http://en.wikipedia.org/wiki/Modular_arithmetic#Integers_modulo_n
            deltas_[values_current_block_] = static_cast<T>(static_cast<UT>(value) - static_cast<UT>(current_value_));
            current_value_ = value;
            idx++;
            values_current_block_++;
            if (values_current_block_ == values_per_block_) {
                FlushBlock();
            }
        }
    }

    void FlushBlock() {
        if (values_current_block_ == 0) {
            return;
        }

        // Calculate the frame of reference for this miniblock. This value will be subtracted
        // from all deltas to guarantee all deltas are positive for encoding.
        const T min_delta = *std::min_element(deltas_.begin(), deltas_.begin() + values_current_block_);
        bit_writer_.PutZigZagVlqInt(min_delta);

        // Call to GetNextBytePtr reserves mini_blocks_per_block_ bytes of space to write
        // bit widths of miniblocks as they become known during the encoding.
        uint8_t* bit_width_data = bit_writer_.GetNextBytePtr(mini_blocks_per_block_);
        DCHECK(bit_width_data != nullptr);

        const uint32_t num_miniblocks = static_cast<uint32_t>(
                std::ceil(static_cast<double>(values_current_block_) / static_cast<double>(values_per_mini_block_)));
        for (uint32_t i = 0; i < num_miniblocks; i++) {
            const uint32_t values_current_mini_block = std::min(values_per_mini_block_, values_current_block_);

            const uint32_t start = i * values_per_mini_block_;
            const T max_delta =
                    *std::max_element(deltas_.begin() + start, deltas_.begin() + start + values_current_mini_block);

            // The minimum number of bits required to write any of values in deltas_ vector.
            // See overflow comment above.
            const auto bit_width = bit_width_data[i] =
                    BitUtil::NumRequiredBits(static_cast<UT>(max_delta) - static_cast<UT>(min_delta));

            for (uint32_t j = start; j < start + values_current_mini_block; j++) {
                // Convert delta to frame of reference. See overflow comment above.
                const UT value = static_cast<UT>(deltas_[j]) - static_cast<UT>(min_delta);
                bit_writer_.PutValue(value, bit_width);
            }
            // If there are not enough values to fill the last mini block, we pad the mini block
            // with zeroes so that its length is the number of values in a full mini block
            // multiplied by the bit width.
            for (uint32_t j = values_current_mini_block; j < values_per_mini_block_; j++) {
                bit_writer_.PutValue(0, bit_width);
            }
            values_current_block_ -= values_current_mini_block;
        }

        // If, in the last block, less than <number of miniblocks in a block> miniblocks are
        // needed to store the values, the bytes storing the bit widths of the unneeded
        // miniblocks are still present, their value should be zero, but readers must accept
        // arbitrary values as well.
        for (uint32_t i = num_miniblocks; i < mini_blocks_per_block_; i++) {
            bit_width_data[i] = 0;
        }
        DCHECK_EQ(values_current_block_, 0);

        bit_writer_.Flush();
        DCHECK_EQ((void*)bit_writer_.buffer(), (void*)(&bits_buffer_));
        DCHECK_EQ(bits_buffer_.size(), bit_writer_.bytes_written());
        sink_.append(bits_buffer_.data(), bits_buffer_.size());
        bit_writer_.Clear();
    }

    void FlushValues() {
        // already flushed.
        if (sink_offset_ != -1) {
            return;
        }

        if (values_current_block_ > 0) {
            FlushBlock();
        }

        faststring header_buffer;
        BitWriter header_writer(&header_buffer);
        header_writer.PutVlqInt(values_per_block_);
        header_writer.PutVlqInt(mini_blocks_per_block_);
        header_writer.PutVlqInt(total_value_count_);
        header_writer.PutZigZagVlqInt(static_cast<T>(first_value_));
        header_writer.Flush();

        // We reserved enough space at the beginning of the buffer for largest possible header
        // and data was written immediately after. We now write the header data immediately
        // before the end of reserved space.
        int actual_header_size = header_writer.bytes_written();
        sink_offset_ = kMaxPageHeaderWriterSize - actual_header_size;
        std::memcpy(sink_.data() + sink_offset_, header_buffer.data(), actual_header_size);
    }
};

// DELTA_BINARY_PACKED decoder
template <typename T>
class DeltaBinaryPackedDecoder final : public Decoder {
public:
    static_assert(std::is_integral_v<T>, "T must be an integral type");
    static_assert(sizeof(T) == 4 || sizeof(T) == 8, "T must be 4 or 8 bytes");
    using UT = std::make_unsigned_t<T>;
    DeltaBinaryPackedDecoder() = default;
    ~DeltaBinaryPackedDecoder() override = default;

    std::string to_string() const override { return fmt::format("DeltaBinaryPackedDecoder<{}>", typeid(T).name()); }

    Status set_data(const Slice& data) override {
        _data = data;
        _offset = 0;
        _bit_reader.reset((uint8_t*)(data.data), data.size);
        RETURN_IF_ERROR(InitHeader());
        return Status::OK();
    }

    Status next_batch(size_t count, ColumnContentType content_type, Column* dst, const FilterData* filter) override {
        if (count > total_values_remaining_) {
            return Status::InvalidArgument("not enough values to read");
        }
        size_t cur_size = dst->size();
        dst->resize(count + cur_size);
        T* data = reinterpret_cast<T*>(dst->mutable_raw_data()) + cur_size;
        RETURN_IF_ERROR(GetInternal(data, count));
        return Status::OK();
    }

    Status next_batch(size_t count, uint8_t* dst) override {
        if (count > total_values_remaining_) {
            return Status::InvalidArgument("not enough values to read");
        }
        T* data = reinterpret_cast<T*>(dst);
        RETURN_IF_ERROR(GetInternal(data, count));
        return Status::OK();
    }

    Status skip(size_t values_to_skip) override {
        if (values_to_skip > total_values_remaining_) {
            return Status::InvalidArgument("not enough values to skip");
        }
        _skip_buffer.resize(values_to_skip);
        RETURN_IF_ERROR(GetInternal(_skip_buffer.data(), values_to_skip));
        return Status::OK();
    }

    uint32_t total_values_count() { return total_value_count_; }

    uint32_t bytes_left() const { return _bit_reader.bytes_left(); }

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

    bool fixed_values_ = true;
    int values_remaining_current_block_ = 0;

    T last_value_;

    // ============
    Slice _data;
    size_t _offset = 0;
    BitReader _bit_reader;
    std::vector<T> _skip_buffer;

private:
    Status InitHeader() {
        BitReader* decoder_ = &_bit_reader;
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
        fixed_values_ = true;
        return Status::OK();
    }

    Status InitBlock() {
        BitReader* decoder_ = &_bit_reader;
        DCHECK_GT(total_values_remaining_, 0) << "InitBlock called at EOF";

        if (!decoder_->GetZigZagVlqInt(&min_delta_)) {
            return Status::Corruption("InitBlock EOF");
        }

        fixed_values_ = true;
        fixed_values_ &= (min_delta_ == 0);

        // read the bitwidth of each miniblock
        uint8_t* bit_width_data = (uint8_t*)delta_bit_widths_.data();
        for (uint32_t i = 0; i < mini_blocks_per_block_; ++i) {
            if (!decoder_->GetAligned<uint8_t>(1, bit_width_data + i)) {
                return Status::Corruption("Decode bit-width EOF");
            }
            // Note that non-conformant bitwidth entries are allowed by the Parquet spec
            // for extraneous miniblocks in the last block (GH-14923), so we check
            // the bitwidths when actually using them (see InitMiniBlock()).
            fixed_values_ &= (bit_width_data[i] == 0);
        }
        values_remaining_current_block_ = values_per_block_;
        first_block_initialized_ = true;
        mini_block_idx_ = 0;
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
        return Status::OK();
    }

    Status GetInternal(T* buffer, int max_values) {
        BitReader* decoder_ = &_bit_reader;
        max_values = std::min<int>(max_values, total_values_remaining_);
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
                return Status::OK();
            }
            RETURN_IF_ERROR(InitBlock());
        }
        DCHECK(first_block_initialized_);

        while (i < max_values) {
            // optimization for fixed values. Remember `fixed_values` only applies in a single block.
            // if current block is fixed values and we can fill more values
            if (fixed_values_ && values_remaining_current_block_ > 0) {
                int values_decode = std::min(values_remaining_current_block_, max_values - i);
                std::fill(buffer + i, buffer + i + values_decode, last_value_);
                i += values_decode;
                values_remaining_current_block_ -= values_decode;

                int values_used_current_block = (values_per_block_ - values_remaining_current_block_);
                mini_block_idx_ = values_used_current_block / values_per_mini_block_;
                values_remaining_current_mini_block_ = values_used_current_block % values_per_mini_block_;
                continue;
            }

            // Ensure we have an initialized mini-block
            if (PREDICT_FALSE(values_remaining_current_mini_block_ == 0)) {
                ++mini_block_idx_;
                if (mini_block_idx_ < mini_blocks_per_block_) {
                    RETURN_IF_ERROR(InitMiniBlock(delta_bit_widths_.data()[mini_block_idx_]));
                } else {
                    RETURN_IF_ERROR(InitBlock());
                    if (fixed_values_) {
                        continue;
                    }
                }
            }

            int values_decode = std::min(values_remaining_current_mini_block_, static_cast<uint32_t>(max_values - i));
            if (!decoder_->GetBatch(delta_bit_width_, buffer + i, values_decode)) {
                return Status::Corruption("GetBatch failed");
            }
            // original version using chain addition.
            // for (int j = 0; j < values_decode; ++j) {
            //     // Addition between min_delta, packed int and last_value should be treated as
            //     // unsigned addition. Overflow is as expected.
            //     buffer[i + j] =
            //             static_cast<UT>(min_delta_) + static_cast<UT>(buffer[i + j]) + static_cast<UT>(last_value_);
            //     last_value_ = buffer[i + j];
            // }

            // fprintf(stderr, "delta_bit_width_ = %d, values_decode = %d, mini_delta = %d, last_value = %d\n",
            //         delta_bit_width_, values_decode, min_delta_, last_value_);

            // fixed values.
            if (PREDICT_FALSE(min_delta_ == 0 && delta_bit_width_ == 0)) {
                std::fill(buffer + i, buffer + i + values_decode, last_value_);
            } else if constexpr (sizeof(T) == 4) {
                delta_decode_chain_int32(buffer + i, values_decode, min_delta_, last_value_);
            } else if constexpr (sizeof(T) == 8) {
                delta_decode_chain_int64(buffer + i, values_decode, min_delta_, last_value_);
            }
            values_remaining_current_mini_block_ -= values_decode;
            i += values_decode;
        }
        total_values_remaining_ -= max_values;

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
};

// ----------------------------------------------------------------------
// DELTA_LENGTH_BYTE_ARRAY encoder

class DeltaLengthByteArrayEncoder final : public Encoder {
public:
    DeltaLengthByteArrayEncoder() = default;
    ~DeltaLengthByteArrayEncoder() override = default;

    Slice build() override {
        FlushValues();
        return Slice(sink_.data(), sink_.size());
    }

    Status append(const uint8_t* vals, size_t count) override {
        sink_sealed_ = false;
        RETURN_IF_ERROR(Put(reinterpret_cast<const Slice*>(vals), count));
        return Status::OK();
    }

    std::string to_string() const override { return "DeltaLengthByteArrayEncoder"; }

private:
    faststring string_buffer_;
    DeltaBinaryPackedEncoder<int> length_encoder_;

    bool sink_sealed_ = false;
    faststring sink_;

private:
    Status Put(const Slice* src, int num_values) {
        if (num_values == 0) {
            return Status::OK();
        }

        constexpr int kBatchSize = 256;
        std::array<int32_t, kBatchSize> lengths;
        int64_t total_increment_size = 0;
        for (int idx = 0; idx < num_values; idx += kBatchSize) {
            const int batch_size = std::min(kBatchSize, num_values - idx);
            for (int j = 0; j < batch_size; ++j) {
                const int32_t len = src[idx + j].size;
                total_increment_size += len;
                lengths[j] = len;
            }
            RETURN_IF_ERROR(length_encoder_.append((const uint8_t*)lengths.data(), batch_size));
        }
        if (total_increment_size > std::numeric_limits<int32_t>::max()) {
            return Status::Corruption("total increment size overflow in DELTA_LENGTH_BYTE_ARRAY");
        }
        if (string_buffer_.length() + total_increment_size > std::numeric_limits<int32_t>::max()) {
            return Status::Corruption("excess expansion in DELTA_LENGTH_BYTE_ARRAY");
        }
        string_buffer_.reserve(string_buffer_.length() + total_increment_size);
        for (int idx = 0; idx < num_values; idx++) {
            string_buffer_.append(src[idx].data, src[idx].size);
        }
        return Status::OK();
    }

    void FlushValues() {
        if (sink_sealed_) {
            return;
        }
        Slice encoded_lengths = length_encoder_.build();
        sink_.clear();
        sink_.reserve(encoded_lengths.size + string_buffer_.size());
        sink_.append(encoded_lengths.data, encoded_lengths.size);
        sink_.append(string_buffer_.data(), string_buffer_.size());
        sink_sealed_ = true;
    }
};

// ----------------------------------------------------------------------
// DELTA_LENGTH_BYTE_ARRAY decoder
template <tparquet::Type::type PT>
class DeltaLengthByteArrayDecoder : public Decoder {
public:
    DeltaLengthByteArrayDecoder() = default;
    ~DeltaLengthByteArrayDecoder() override = default;

    std::string to_string() const override { return "DeltaLengthByteArrayDecoder"; }

    Status set_data(const Slice& data) override {
        RETURN_IF_ERROR(len_decoder_.set_data(data));
        RETURN_IF_ERROR(DecodeLengths());
        data_ = (const uint8_t*)data.data;
        len_ = data.size;
        bytes_offset_ = (len_ - len_decoder_.bytes_left());
        return Status::OK();
    }

    Status skip(size_t values_to_skip) override {
        if (values_to_skip > num_valid_values_) {
            return Status::InvalidArgument("not enough values to skip");
        }
        RETURN_IF_ERROR(Skip(static_cast<int>(values_to_skip)));
        return Status::OK();
    }

    Status next_batch(size_t count, ColumnContentType content_type, Column* dst, const FilterData* filter) override {
        if (count > num_valid_values_) {
            return Status::InvalidArgument("not enough values to read");
        }
        slice_buffer_.reserve(count);
        RETURN_IF_ERROR(Decode(slice_buffer_.data(), static_cast<int>(count)));

        if (dst->is_nullable()) {
            down_cast<NullableColumn*>(dst)->null_column_raw_ptr()->append_default(count);
        }
        auto* binary_column = ColumnHelper::get_binary_column(dst);
        binary_column->append_continuous_strings(slice_buffer_.data(), count);
        return Status::OK();
    }

    Status next_batch(size_t count, uint8_t* dst) override {
        if (count > num_valid_values_) {
            return Status::InvalidArgument("not enough values to read");
        }
        Slice* data = reinterpret_cast<Slice*>(dst);
        RETURN_IF_ERROR(Decode(data, count));
        return Status::OK();
    }

    void set_type_length(int32_t type_length) override { type_length_ = type_length; }

private:
    const uint8_t* data_ = nullptr;
    uint32_t len_ = 0;
    uint32_t bytes_offset_ = 0;
    std::vector<Slice> slice_buffer_;

    DeltaBinaryPackedDecoder<int32_t> len_decoder_;
    int num_valid_values_{0};
    uint32_t length_idx_{0};
    std::vector<int32_t> buffered_length_;
    int type_length_;

private:
    // Decode all the encoded lengths. The decoder_ will be at the start of the encoded data after that.
    Status DecodeLengths() {
        // get the number of encoded lengths
        int num_length = len_decoder_.total_values_count();
        buffered_length_.resize(num_length);
        // call len_decoder_.Decode to decode all the lengths.
        // all the lengths are buffered in buffered_length_.
        RETURN_IF_ERROR(len_decoder_.next_batch(num_length, reinterpret_cast<uint8_t*>(buffered_length_.data())));
        length_idx_ = 0;
        num_valid_values_ = num_length;
        return Status::OK();
    }

    Status Skip(int max_values) {
        max_values = std::min(max_values, num_valid_values_);
        if (max_values == 0) {
            return Status::OK();
        }
        int64_t data_size = 0;
        const int32_t* length_ptr = buffered_length_.data() + length_idx_;
        if constexpr (PT == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
            data_size += max_values * type_length_;
        } else {
            if (contains_negative_value(length_ptr, max_values)) {
                return Status::Corruption("negative string delta length");
            }
            for (int i = 0; i < max_values; ++i) {
                data_size += length_ptr[i];
            }
        }
        if (data_size > std::numeric_limits<int32_t>::max()) {
            return Status::Corruption("data size overflow in DELTA_LENGTH_BYTE_ARRAY");
        }
        length_idx_ += max_values;
        num_valid_values_ -= max_values;
        bytes_offset_ += data_size;
        if (PREDICT_FALSE(bytes_offset_ > len_)) {
            return Status::Corruption("bytes offset exceeds data size in DELTA_LENGTH_BYTE_ARRAY");
        }
        return Status::OK();
    }

    Status Decode(Slice* buffer, int max_values) {
        // Decode up to `max_values` strings into an internal buffer
        // and reference them into `buffer`.
        max_values = std::min(max_values, num_valid_values_);
        if (max_values == 0) {
            return Status::OK();
        }

        int64_t data_size = 0;
        const uint8_t* data_ptr = data_ + bytes_offset_;
        const int32_t* length_ptr = buffered_length_.data() + length_idx_;
        if (contains_negative_value(length_ptr, max_values)) {
            return Status::Corruption("negative string delta length");
        }
        for (int i = 0; i < max_values; ++i) {
            int32_t len = length_ptr[i];
            buffer[i].data = (char*)data_ptr;
            buffer[i].size = len;
            data_ptr += len;
            data_size += len;
        }
        if (data_size > std::numeric_limits<int32_t>::max()) {
            return Status::Corruption("data size overflow in DELTA_LENGTH_BYTE_ARRAY");
        }
        length_idx_ += max_values;
        num_valid_values_ -= max_values;
        bytes_offset_ += data_size;
        if (PREDICT_FALSE(bytes_offset_ > len_)) {
            return Status::Corruption("bytes offset exceeds data size in DELTA_LENGTH_BYTE_ARRAY");
        }
        return Status::OK();
    }
};

// ----------------------------------------------------------------------
// DELTA_BYTE_ARRAY encoder
/// Delta Byte Array encoding also known as incremental encoding or front compression:
/// for each element in a sequence of strings, store the prefix length of the previous
/// entry plus the suffix.
///
/// This is stored as a sequence of delta-encoded prefix lengths (DELTA_BINARY_PACKED),
/// followed by the suffixes encoded as delta length byte arrays
/// (DELTA_LENGTH_BYTE_ARRAY).

class DeltaByteArrayEncoder : public Encoder {
public:
    DeltaByteArrayEncoder() = default;
    ~DeltaByteArrayEncoder() override = default;

    std::string to_string() const override { return "DeltaByteArrayEncoder"; }

    Slice build() override {
        FlushValues();
        return Slice(sink_.data(), sink_.size());
    }

    Status append(const uint8_t* vals, size_t count) override {
        sink_sealed_ = false;
        RETURN_IF_ERROR(Put(reinterpret_cast<const Slice*>(vals), count));
        return Status::OK();
    }

private:
    DeltaBinaryPackedEncoder<int32_t> prefix_length_encoder_;
    DeltaLengthByteArrayEncoder suffix_encoder_;
    std::string last_value_ = "";

    bool sink_sealed_ = false;
    faststring sink_;

private:
    Status Put(const Slice* src, int num_values) {
        if (num_values == 0) {
            return Status::OK();
        }

        std::string_view last_value_view = last_value_;
        constexpr int kBatchSize = 256;
        std::array<int32_t, kBatchSize> prefix_lengths;
        std::array<Slice, kBatchSize> suffixes;

        for (int i = 0; i < num_values; i += kBatchSize) {
            const int batch_size = std::min(kBatchSize, num_values - i);

            for (int j = 0; j < batch_size; ++j) {
                const int idx = i + j;
                const auto view = src[idx];
                const auto len = static_cast<const uint32_t>(view.size);

                uint32_t common_prefix_length = 0;
                const uint32_t maximum_common_prefix_length =
                        std::min(len, static_cast<uint32_t>(last_value_view.length()));
                while (common_prefix_length < maximum_common_prefix_length) {
                    if (last_value_view[common_prefix_length] != view[common_prefix_length]) {
                        break;
                    }
                    common_prefix_length++;
                }

                last_value_view = view;
                prefix_lengths[j] = common_prefix_length;
                const uint32_t suffix_length = len - common_prefix_length;
                const uint8_t* suffix_ptr = (const uint8_t*)src[idx].data + common_prefix_length;

                // Convert to ByteArray, so it can be passed to the suffix_encoder_.
                const Slice suffix = {reinterpret_cast<const char*>(suffix_ptr), suffix_length};
                suffixes[j] = suffix;
            }

            RETURN_IF_ERROR(suffix_encoder_.append(reinterpret_cast<const uint8_t*>(suffixes.data()), batch_size));
            RETURN_IF_ERROR(
                    prefix_length_encoder_.append(reinterpret_cast<const uint8_t*>(prefix_lengths.data()), batch_size));
        }
        last_value_ = last_value_view;
        return Status::OK();
    }

    void FlushValues() {
        if (sink_sealed_) {
            return;
        }
        Slice prefix_length_data = prefix_length_encoder_.build();
        // todo(yanz): could optimize to avoid an extra memcpy
        Slice suffix_data = suffix_encoder_.build();
        sink_.clear();
        sink_.reserve(prefix_length_data.size + suffix_data.size);
        sink_.append(prefix_length_data.data, prefix_length_data.size);
        sink_.append(suffix_data.data, suffix_data.size);
        sink_sealed_ = true;
    }
};

// ----------------------------------------------------------------------
// DELTA_BYTE_ARRAY decoder
class DeltaByteArrayDecoder : public Decoder {
public:
    DeltaByteArrayDecoder() = default;
    ~DeltaByteArrayDecoder() override = default;

    std::string to_string() const override { return "DeltaByteArrayDecoder"; }

private:
    DeltaBinaryPackedDecoder<int32_t> prefix_len_decoder_;
    DeltaLengthByteArrayDecoder<tparquet::Type::BYTE_ARRAY> suffix_decoder_;
    std::string last_value_;
    // string buffer for last value in previous page
    // std::string last_value_in_previous_page_;
    int num_valid_values_{0};
    uint32_t prefix_len_offset_{0};
    faststring buffered_prefix_length_;
    // buffer for decoded strings, which gurantees the lifetime of the decoded strings
    // until the next call of Decode.
    faststring buffered_data_;
    std::vector<Slice> slice_buffer_;

public:
    Status set_data(const Slice& data) override {
        RETURN_IF_ERROR(prefix_len_decoder_.set_data(data));
        // get the number of encoded prefix lengths
        int num_prefix = prefix_len_decoder_.total_values_count();
        // call prefix_len_decoder_.Decode to decode all the prefix lengths.
        // all the prefix lengths are buffered in buffered_prefix_length_.
        buffered_prefix_length_.resize(num_prefix * sizeof(int32_t));
        RETURN_IF_ERROR(prefix_len_decoder_.next_batch(num_prefix, buffered_prefix_length_.data()));
        prefix_len_offset_ = 0;
        num_valid_values_ = num_prefix;

        int bytes_left = prefix_len_decoder_.bytes_left();
        // If len < bytes_left, prefix_len_decoder.Decode will throw exception.
        DCHECK_GE(data.size, bytes_left);
        int suffix_begins = data.size - bytes_left;
        // at this time, the decoder_ will be at the start of the encoded suffix data.
        RETURN_IF_ERROR(suffix_decoder_.set_data(Slice(data.data + suffix_begins, bytes_left)));

        // TODO: read corrupted files written with bug(PARQUET-246). last_value_ should be set
        // to last_value_in_previous_page_ when decoding a new page(except the first page)
        last_value_.clear();
        return Status::OK();
    }

    Status skip(size_t values_to_skip) override {
        if (values_to_skip > num_valid_values_) {
            return Status::InvalidArgument("not enough values to skip");
        }
        slice_buffer_.reserve(values_to_skip);
        RETURN_IF_ERROR(GetInternal(slice_buffer_.data(), static_cast<int>(values_to_skip)));
        return Status::OK();
    }

    Status next_batch(size_t count, ColumnContentType content_type, Column* dst, const FilterData* filter) override {
        if (count > num_valid_values_) {
            return Status::InvalidArgument("not enough values to read");
        }
        slice_buffer_.reserve(count);
        RETURN_IF_ERROR(GetInternal(slice_buffer_.data(), static_cast<int>(count)));

        if (dst->is_nullable()) {
            down_cast<NullableColumn*>(dst)->null_column_raw_ptr()->append_default(count);
        }
        auto* binary_column = ColumnHelper::get_binary_column(dst);
        binary_column->append_continuous_strings(slice_buffer_.data(), count);
        return Status::OK();
    }

    Status next_batch(size_t count, uint8_t* dst) override {
        if (count > num_valid_values_) {
            return Status::InvalidArgument("not enough values to read");
        }
        Slice* data = reinterpret_cast<Slice*>(dst);
        RETURN_IF_ERROR(GetInternal(data, count));
        return Status::OK();
    }

protected:
    template <bool is_first_run>
    static Status BuildBufferInternal(const int32_t* prefix_len_ptr, int i, Slice* buffer, std::string_view* prefix,
                                      uint8_t** __restrict__ data_ptr) {
        if (PREDICT_FALSE(static_cast<size_t>(prefix_len_ptr[i]) > prefix->length())) {
            return Status::Corruption("prefix length too large in DELTA_BYTE_ARRAY");
        }

        DCHECK_EQ(is_first_run, i == 0);
        // Both prefix and suffix are non-empty, so we need to decode the string
        // into `data_ptr`.
        // 1. Copy the prefix
        memcpy(*data_ptr, prefix->data(), prefix_len_ptr[i]);
        // 2. Copy the suffix.
        memcpy(*data_ptr + prefix_len_ptr[i], buffer[i].data, buffer[i].size);
        // 3. Point buffer[i] to the decoded string.
        buffer[i].data = (char*)*data_ptr;
        buffer[i].size += prefix_len_ptr[i];
        *data_ptr += buffer[i].size;
        *prefix = std::string_view{buffer[i]};
        return Status::OK();
    }

    Status GetInternal(Slice* buffer, int max_values) {
        // Decode up to `max_values` strings into an internal buffer
        // and reference them into `buffer`.
        max_values = std::min(max_values, num_valid_values_);
        if (max_values == 0) {
            return Status::OK();
        }

        RETURN_IF_ERROR(suffix_decoder_.next_batch(max_values, reinterpret_cast<uint8_t*>(buffer)));
        int64_t data_size = 0;
        const int32_t* prefix_len_ptr = (const int32_t*)buffered_prefix_length_.data() + prefix_len_offset_;
        if (contains_negative_value(prefix_len_ptr, max_values)) {
            return Status::Corruption("negative prefix length in DELTA_BYTE_ARRAY");
        }
        for (int i = 0; i < max_values; ++i) {
            data_size += prefix_len_ptr[i] + buffer[i].size;
        }
        if (data_size > std::numeric_limits<int32_t>::max()) {
            return Status::Corruption("excess expansion in DELTA_BYTE_ARRAY");
        }
        buffered_data_.resize(data_size);
        std::string_view prefix{last_value_};
        uint8_t* data_ptr = buffered_data_.data();
        if (max_values > 0) {
            RETURN_IF_ERROR(BuildBufferInternal</*is_first_run=*/true>(prefix_len_ptr, 0, buffer, &prefix, &data_ptr));
        }
        for (int i = 1; i < max_values; ++i) {
            RETURN_IF_ERROR(BuildBufferInternal</*is_first_run=*/false>(prefix_len_ptr, i, buffer, &prefix, &data_ptr));
        }
        DCHECK_EQ(data_ptr - buffered_data_.data(), data_size);
        prefix_len_offset_ += max_values;
        num_valid_values_ -= max_values;
        last_value_ = std::string(prefix);
        return Status::OK();
    }
};

} // namespace starrocks::parquet