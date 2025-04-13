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
#include "column/column_helper.h"
#include "common/status.h"
#include "formats/parquet/encoding.h"
#include "util/bit_stream_utils.h"
#include "util/slice.h"

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
    using UT = std::make_unsigned_t<T>;
    DeltaBinaryPackedDecoder() = default;
    ~DeltaBinaryPackedDecoder() override = default;

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
        dst->resize_uninitialized(count + cur_size);
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
        constexpr int kMaxSkipBufferSize = 128;
        _skip_buffer.resize(kMaxSkipBufferSize);
        while (values_to_skip > 0) {
            size_t to_read = std::min<size_t>(values_to_skip, kMaxSkipBufferSize);
            RETURN_IF_ERROR(GetInternal(_skip_buffer.data(), to_read));
            values_to_skip -= to_read;
        }
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
        return Status::OK();
    }

    Status InitBlock() {
        BitReader* decoder_ = &_bit_reader;
        DCHECK_GT(total_values_remaining_, 0) << "InitBlock called at EOF";

        if (!decoder_->GetZigZagVlqInt(&min_delta_)) {
            return Status::Corruption("InitBlock EOF");
        }

        // read the bitwidth of each miniblock
        uint8_t* bit_width_data = (uint8_t*)delta_bit_widths_.data();
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
        return Status::OK();
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
                    RETURN_IF_ERROR(InitMiniBlock(delta_bit_widths_.data()[mini_block_idx_]));
                } else {
                    RETURN_IF_ERROR(InitBlock());
                }
            }

            int values_decode = std::min(values_remaining_current_mini_block_, static_cast<uint32_t>(max_values - i));
            if (!decoder_->GetBatch(delta_bit_width_, buffer + i, values_decode)) {
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

    Slice build() override {
        FlushValues();
        return Slice(packed_data_.data(), packed_data_.size());
    }

    Status append(const uint8_t* vals, size_t count) override {
        packed_ = false;
        RETURN_IF_ERROR(Put(reinterpret_cast<const Slice*>(vals), count));
        return Status::OK();
    }

private:
    faststring sink_;
    DeltaBinaryPackedEncoder<int> length_encoder_;

    bool packed_ = false;
    faststring packed_data_;

private:
    Status Put(const Slice* src, int num_values) {
        if (num_values == 0) {
            return Status::OK();
        }

        constexpr int kBatchSize = 256;
        std::array<int32_t, kBatchSize> lengths;
        uint32_t total_increment_size = 0;
        for (int idx = 0; idx < num_values; idx += kBatchSize) {
            const int batch_size = std::min(kBatchSize, num_values - idx);
            for (int j = 0; j < batch_size; ++j) {
                const int32_t len = src[idx + j].size;
                total_increment_size += len;
                lengths[j] = len;
            }
            RETURN_IF_ERROR(length_encoder_.append((const uint8_t*)lengths.data(), batch_size));
        }
        if (sink_.length() + total_increment_size > std::numeric_limits<int32_t>::max()) {
            return Status::Corruption("excess expansion in DELTA_LENGTH_BYTE_ARRAY");
        }
        sink_.reserve(total_increment_size);
        for (int idx = 0; idx < num_values; idx++) {
            sink_.append(src[idx].data, src[idx].size);
        }
        return Status::OK();
    }

    void FlushValues() {
        if (packed_) {
            return;
        }
        Slice encoded_lengths = length_encoder_.build();
        packed_data_.clear();
        packed_data_.reserve(encoded_lengths.size + sink_.size());
        packed_data_.append(encoded_lengths.data, encoded_lengths.size);
        packed_data_.append(sink_.data(), sink_.size());
        packed_ = true;
    }
};

// ----------------------------------------------------------------------
// DELTA_LENGTH_BYTE_ARRAY decoder

class DeltaLengthByteArrayDecoder : public Decoder {
public:
    DeltaLengthByteArrayDecoder() = default;
    ~DeltaLengthByteArrayDecoder() override = default;

    Status set_data(const Slice& data) override {
        RETURN_IF_ERROR(len_decoder_.set_data(data));
        RETURN_IF_ERROR(DecodeLengths());
        data_ = (const uint8_t*)data.data;
        len_ = data.size;
        bytes_offset_ = (len_ - len_decoder_.bytes_left());
        return Status::OK();
    }

    Status skip(size_t values_to_skip) override { return Skip(static_cast<int>(values_to_skip)); }

    Status next_batch(size_t count, ColumnContentType content_type, Column* dst, const FilterData* filter) override {
        if ((count + length_idx_) > num_valid_values_) {
            return Status::InvalidArgument("not enough values to read");
        }
        slice_buffer_.reserve(count);
        RETURN_IF_ERROR(Decode(slice_buffer_.data(), static_cast<int>(count)));

        if (dst->is_nullable()) {
            down_cast<NullableColumn*>(dst)->mutable_null_column()->append_default(count);
        }
        auto* binary_column = ColumnHelper::get_binary_column(dst);
        binary_column->append_strings(slice_buffer_.data(), count);
        return Status::OK();
    }

    Status next_batch(size_t count, uint8_t* dst) override {
        Slice* data = reinterpret_cast<Slice*>(dst);
        RETURN_IF_ERROR(Decode(data, count));
        return Status::OK();
    }

private:
    const uint8_t* data_ = nullptr;
    uint32_t len_ = 0;
    uint32_t bytes_offset_ = 0;
    std::vector<Slice> slice_buffer_;

    DeltaBinaryPackedDecoder<int32_t> len_decoder_;
    int num_valid_values_{0};
    uint32_t length_idx_{0};
    std::vector<int32_t> buffered_length_;

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

    Status Skip(int count) {
        if ((count + length_idx_) > num_valid_values_) {
            return Status::InvalidArgument("not enough values to skip");
        }
        int32_t data_size = 0;
        const int32_t* length_ptr = buffered_length_.data() + length_idx_;
        for (int i = 0; i < count; ++i) {
            int32_t len = length_ptr[i];
            if (PREDICT_FALSE(len < 0)) {
                return Status::Corruption("negative string delta length");
            }
            data_size += len;
        }
        length_idx_ += count;
        bytes_offset_ += data_size;
        return Status::OK();
    }

    Status Decode(Slice* buffer, int count) {
        // Decode up to `max_values` strings into an internal buffer
        // and reference them into `buffer`.
        if ((count + length_idx_) > num_valid_values_) {
            return Status::InvalidArgument("not enough values to read");
        }
        if (count == 0) {
            return Status::OK();
        }

        int32_t data_size = 0;
        const int32_t* length_ptr = buffered_length_.data() + length_idx_;
        for (int i = 0; i < count; ++i) {
            int32_t len = length_ptr[i];
            if (PREDICT_FALSE(len < 0)) {
                return Status::Corruption("negative string delta length");
            }
            buffer[i].size = len;
            data_size += len;
        }
        length_idx_ += count;
        const uint8_t* data_ptr = data_ + bytes_offset_;
        for (int i = 0; i < count; ++i) {
            buffer[i].data = (char*)data_ptr;
            data_ptr += buffer[i].size;
        }
        bytes_offset_ += data_size;
        num_valid_values_ -= count;
        return Status::OK();
    }
};

} // namespace starrocks::parquet