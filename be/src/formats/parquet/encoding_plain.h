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

#include <cstring>

#include "column/column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "formats/parquet/encoding.h"
#include "gutil/strings/substitute.h"
#include "types/int256.h"
#include "util/bit_stream_utils.h"
#include "util/bit_util.h"
#include "util/coding.h"
#include "util/faststring.h"
#include "util/raw_container.h"
#include "util/slice.h"

#ifdef __AVX2__
#include <immintrin.h>
#endif

namespace starrocks::parquet {

static constexpr int kBooleanBitPackedBitWidth = 1;

template <typename T>
class PlainEncoder final : public Encoder {
public:
    PlainEncoder() = default;
    ~PlainEncoder() override = default;

    Status append(const uint8_t* vals, size_t count) override {
        size_t copy_bytes = count * SIZE_OF_TYPE;
        _buffer.append(vals, copy_bytes);
        return Status::OK();
    }

    Slice build() override { return {_buffer.data(), _buffer.size()}; }

private:
    enum { SIZE_OF_TYPE = sizeof(T) };

    faststring _buffer;
};

template <>
class PlainEncoder<Slice> final : public Encoder {
public:
    PlainEncoder() = default;
    ~PlainEncoder() override = default;

    Status append(const uint8_t* vals, size_t count) override {
        const auto* slices = (const Slice*)vals;
        for (int i = 0; i < count; ++i) {
            put_fixed32_le(&_buffer, static_cast<uint32_t>(slices[i].size));
            _buffer.append(slices[i].data, slices[i].size);
        }
        return Status::OK();
    }

    Slice build() override { return {_buffer.data(), _buffer.size()}; }

private:
    faststring _buffer;
};

template <>
class PlainEncoder<bool> final : public Encoder {
public:
    PlainEncoder() : _bit_writer(&_buffer) {}
    ~PlainEncoder() override = default;

    Status append(const uint8_t* vals, size_t count) override {
        // TODO(@DorianZheng) Plain boolean encoder is using by UT currently, optimize it in the future.
        for (int i = 0; i < count; i++) {
            _bit_writer.PutValue(vals[i], kBooleanBitPackedBitWidth);
        }
        return Status::OK();
    }

    Slice build() override {
        _bit_writer.Flush();
        return {_buffer.data(), _buffer.size()};
    }

private:
    faststring _buffer;
    BitWriter _bit_writer;
};

template <typename T>
class PlainDecoder final : public Decoder {
public:
    PlainDecoder() = default;
    ~PlainDecoder() override = default;

    static Status decode(const std::string& buffer, T* value) {
        int byte_size = sizeof(T);
        memcpy(value, buffer.c_str(), byte_size);
        return Status::OK();
    }

    Status set_data(const Slice& data) override {
        _data = data;
        _offset = 0;
        return Status::OK();
    }

    Status next_batch(size_t count, ColumnContentType content_type, Column* dst, const FilterData* filter) override {
        size_t max_fetch = count * SIZE_OF_TYPE;
        if (max_fetch + _offset > _data.size) {
            return Status::InternalError(strings::Substitute(
                    "going to read out-of-bounds data, offset=$0,count=$1,size=$2", _offset, count, _data.size));
        }
        auto n = dst->append_numbers(_data.data + _offset, max_fetch);
        DCHECK_EQ(count, n);
        _offset += max_fetch;
        return Status::OK();
    }

    Status skip(size_t values_to_skip) override {
        size_t fetch_size = values_to_skip * SIZE_OF_TYPE;
        if (fetch_size + _offset > _data.size) {
            return Status::InternalError(strings::Substitute(
                    "going to skip out-of-bounds data, offset=$0,skip=$1,size=$2", _offset, fetch_size, _data.size));
        }
        _offset += fetch_size;
        return Status::OK();
    }

    Status next_batch(size_t count, uint8_t* dst) override {
        size_t max_fetch = count * SIZE_OF_TYPE;
        if (max_fetch + _offset > _data.size) {
            return Status::InternalError(strings::Substitute(
                    "going to read out-of-bounds data, offset=$0,count=$1,size=$2", _offset, count, _data.size));
        }
        memcpy(dst, _data.data + _offset, max_fetch);
        _offset += max_fetch;
        return Status::OK();
    }

private:
    enum { SIZE_OF_TYPE = sizeof(T) };

    Slice _data;
    size_t _offset = 0;
};

template <>
class PlainDecoder<Slice> final : public Decoder {
public:
    PlainDecoder() = default;
    ~PlainDecoder() override = default;

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

    Status next_batch_with_nulls(size_t count, const NullInfos& null_infos, ColumnContentType content_type, Column* dst,
                                 const FilterData* filter) override {
        const uint8_t* __restrict is_nulls = null_infos.nulls_data();
        // fill null data
        DCHECK(dst->is_nullable());
        size_t null_cnt = null_infos.num_nulls;
        if (dst->is_nullable()) {
            NullColumn* null_column = down_cast<NullableColumn*>(dst)->mutable_null_column();
            auto& null_data = null_column->get_data();
            size_t prev_num_rows = null_data.size();
            raw::stl_vector_resize_uninitialized(&null_data, count + prev_num_rows);
            uint8_t* __restrict__ dst_nulls = null_data.data() + prev_num_rows;
            memcpy(dst_nulls, is_nulls, count);
            down_cast<NullableColumn*>(dst)->set_has_null(null_cnt > 0);
        }

        size_t max_size = 0;
        size_t read_count = count - null_cnt;
        uint32_t lengths[read_count + 1];
        char* datas[read_count + 1];
        size_t i = 0;
        size_t cursor = _offset;
        //
        for (i = 0; (i < read_count) & (_offset < _data.size); ++i) {
            uint32_t length = decode_fixed32_le(reinterpret_cast<const uint8_t*>(_data.data) + cursor);
            cursor += sizeof(int32_t);
            datas[i] = _data.data + cursor;
            cursor += length;
            lengths[i] = length;
            max_size = max_size > length ? max_size : length;
        }

        _offset = cursor;
        if (i < read_count) {
            return Status::InternalError(fmt::format("error: null_cnt {} i:{} read_cnt:{}", null_cnt, i, read_count));
        }
        // fill offset data
        auto* binary_column = ColumnHelper::get_binary_column(dst);
        {
            auto& offsets = binary_column->get_offset();
            auto& bytes = binary_column->get_bytes();
            size_t prev_offsets = offsets.size();
            raw::stl_vector_resize_uninitialized(&offsets, count + prev_offsets);
            size_t offset = bytes.size();
            size_t cnt = 0;
            for (size_t i = 0; i < count; ++i) {
                offset += is_nulls[i] ? 0 : lengths[cnt++];
                offsets[prev_offsets + i] = offset;
            }

            binary_column->get_bytes().reserve(offset);
        }

        if (read_count == 0) {
            return Status::OK();
        }

        // fill bytes data
        max_size = std::max(BitUtil::next_power_of_two(max_size), 8L);
        if (datas[read_count - 1] - _data.data + max_size <= _data.size) {
            binary_column->append_bytes_overflow(datas, lengths, read_count, max_size);
            DCHECK_EQ(binary_column->get_bytes().size(), binary_column->get_offset().back());
        } else {
            binary_column->append_bytes(datas, lengths, read_count);
            DCHECK_EQ(binary_column->get_bytes().size(), binary_column->get_offset().back());
        }

        return Status::OK();
    }

    Status next_batch(size_t count, ColumnContentType content_type, Column* dst, const FilterData* filter) override {
        size_t num_decoded = 0;
        if (dst->is_nullable()) {
            down_cast<NullableColumn*>(dst)->mutable_null_column()->append_default(count);
        }

#define CHECK_DECODING_BOUND                                                                                  \
    if (UNLIKELY(num_decoded < count || _offset > _data.size)) {                                              \
        return Status::InternalError(strings::Substitute(                                                     \
                "going to read out-of-bounds data, offset=$0,count=$1,size=$2", _offset, count, _data.size)); \
    }

        if (filter) {
            auto* binary_column = ColumnHelper::get_binary_column(dst);
            while (num_decoded < count && _offset < _data.size) {
                uint32_t length = decode_fixed32_le(reinterpret_cast<const uint8_t*>(_data.data) + _offset);
                _offset += sizeof(int32_t);
                if (filter[num_decoded]) {
                    binary_column->append(Slice(_data.data + _offset, length));
                } else {
                    binary_column->append_default();
                }
                _offset += length;
                num_decoded++;
            }
            CHECK_DECODING_BOUND
        } else {
            auto slices_data = std::make_unique_for_overwrite<uint8_t[]>(count * sizeof(Slice));
            Slice* slices = reinterpret_cast<Slice*>(slices_data.get());
            size_t max_size = 0;
            while (num_decoded < count && _offset < _data.size) {
                uint32_t length = decode_fixed32_le(reinterpret_cast<const uint8_t*>(_data.data) + _offset);
                _offset += sizeof(int32_t);
                slices[num_decoded] = {_data.data + _offset, length};
                _offset += length;
                max_size = std::max<size_t>(max_size, length);
                num_decoded++;
            }
            CHECK_DECODING_BOUND
            bool ret = false;
            // when last slices offset + max_size > _data.size, there is overflow on reading
            max_size = std::max(BitUtil::next_power_of_two(max_size), 8L);
            if (slices[count - 1].data - _data.data + max_size <= _data.size) {
                ret = ColumnHelper::get_binary_column(dst)->append_strings_overflow(slices, num_decoded, max_size);
            } else {
                ret = ColumnHelper::get_binary_column(dst)->append_strings(slices, num_decoded);
            }

            if (UNLIKELY(!ret)) {
                return Status::InternalError("PlainDecoder append strings to column failed");
            }
        }

        return Status::OK();
    }

    Status skip(size_t values_to_skip) override {
        size_t num_decoded = 0;
        while (num_decoded < values_to_skip && _offset < _data.size) {
            uint32_t length = decode_fixed32_le(reinterpret_cast<const uint8_t*>(_data.data) + _offset);
            _offset += sizeof(int32_t);
            _offset += length;
            num_decoded++;
        }
        // unlikely happened
        if (UNLIKELY(num_decoded < values_to_skip || _offset > _data.size)) {
            return Status::InternalError(
                    strings::Substitute("going to skip out-of-bounds data, offset=$0,size=$1", _offset, _data.size));
        }
        return Status::OK();
    }

    Status next_batch(size_t count, uint8_t* dst) override {
        auto* slices = reinterpret_cast<Slice*>(dst);

        size_t num_decoded = 0;
        while (num_decoded < count && _offset < _data.size) {
            uint32_t length = decode_fixed32_le(reinterpret_cast<const uint8_t*>(_data.data) + _offset);
            _offset += sizeof(int32_t);
            slices[num_decoded] = Slice(_data.data + _offset, length);
            _offset += length;
            num_decoded++;
        }
        // never happend
        if (num_decoded < count || _offset > _data.size) {
            return Status::InternalError(strings::Substitute(
                    "going to read out-of-bounds data, offset=$0,count=$1,size=$2", _offset, count, _data.size));
        }
        return Status::OK();
    }

private:
    Slice _data;
    size_t _offset = 0;
};

// plain encoding for boolean type is stored as `Bit Packed`, `LSB` first format
// more details refer to: https://github.com/apache/parquet-format/blob/master/Encodings.md#PLAIN
template <>
class PlainDecoder<bool> final : public Decoder {
public:
    PlainDecoder() = default;
    ~PlainDecoder() override = default;

    Status set_data(const Slice& data) override {
        _batched_bit_reader.reset(reinterpret_cast<const uint8_t*>(data.data), data.size);
        _decoded_values_buffer.reset();
        return Status::OK();
    }

    Status next_batch(size_t count, ColumnContentType content_type, Column* dst, const FilterData* filter) override {
        auto original_size = dst->size();
        dst->resize(original_size + count);
        auto num_unpacked_values = unpack_batch(count, dst->mutable_raw_data() + original_size);
        if (num_unpacked_values < count) {
            return Status::InternalError(strings::Substitute(
                    "going to read out-of-bounds data, count=$0,num_unpacked_values=$1", count, num_unpacked_values));
        }
        return Status::OK();
    }

    Status skip(size_t values_to_skip) override {
        //TODO(Smith) still heavy work load
        std::vector<uint8_t> tmp;
        tmp.reserve(values_to_skip);
        return next_batch(values_to_skip, tmp.data());
    }

    Status next_batch(size_t count, uint8_t* dst) override {
        auto num_unpacked_values = unpack_batch(count, dst);
        if (num_unpacked_values < count) {
            return Status::InternalError(strings::Substitute(
                    "going to read out-of-bounds data, count=$0,num_unpacked_values=$1", count, num_unpacked_values));
        }
        return Status::OK();
    }

private:
    static const int kBitPackedBatchSize = 32;
    static const int kBitPackedDefaultValue = 8;

    BatchedBitReader _batched_bit_reader;

    std::unique_ptr<uint8_t[]> _decoded_values_buffer;
    std::size_t _decoded_buffer_size;
    std::size_t _decoded_values_size;
    std::size_t _decoded_values_offset;

    std::size_t read_decoded_values(std::size_t num_values, uint8_t* v) {
        if (_decoded_values_buffer != nullptr && _decoded_values_offset < _decoded_values_size) {
            auto read_count = std::min(num_values, _decoded_values_size - _decoded_values_offset);
            memcpy(v, &_decoded_values_buffer[_decoded_values_offset], read_count * sizeof(uint8_t));
            _decoded_values_offset += read_count;
            return read_count;
        } else {
            return 0;
        }
    }

    void unpack_round_up_num_values(int bit_width, std::size_t num_values) {
        auto round_up_num_values = BitUtil::round_up_numi32(static_cast<uint32_t>(num_values)) << 5;
        if (_decoded_values_buffer == nullptr || _decoded_buffer_size < round_up_num_values) {
            _decoded_values_buffer = std::make_unique<uint8_t[]>(round_up_num_values);
            _decoded_buffer_size = round_up_num_values;
        }
        _decoded_values_size = _batched_bit_reader.unpack_batch(bit_width, static_cast<int>(round_up_num_values),
                                                                _decoded_values_buffer.get());
        _decoded_values_offset = 0;
    }

    // BatchedBitReader::unpack_batch may drop trailing bits, for safety, we should decode 32*n value a batch
    // and buffer it.
    std::size_t unpack_batch(std::size_t num_values, uint8_t* v) {
        // if _decoded_values_buffer has remaining unread values, read it first
        auto read_count = read_decoded_values(num_values, v);
        if (read_count == num_values) {
            return num_values;
        }
        num_values -= read_count;

        // If 'num_values' is a multiple of 32 or 'bit_width' * 'num_values' must be a multiple of 8.
        // We will not drop any trailing bits.
        if (num_values % kBitPackedBatchSize == 0 ||
            (num_values * kBooleanBitPackedBitWidth) % kBitPackedDefaultValue == 0) {
            read_count += _batched_bit_reader.unpack_batch(kBooleanBitPackedBitWidth, static_cast<int>(num_values),
                                                           v + read_count);
        } else {
            // Else, read round up number to 32 of values and buffer it
            unpack_round_up_num_values(kBooleanBitPackedBitWidth, num_values);
            read_count += read_decoded_values(num_values, v + read_count);
        }

        return read_count;
    }
};

// Fixed length byte array encoder and decoder
class FLBAPlainEncoder final : public Encoder {
public:
    FLBAPlainEncoder() = default;
    ~FLBAPlainEncoder() override = default;

    Status append(const uint8_t* vals, size_t count) override {
        if (count == 0) return Status::OK();

        const auto* slices = (const Slice*)vals;
        _buffer.reserve(_buffer.size() + count * slices[0].size);
        for (int i = 0; i < count; ++i) {
            DCHECK_EQ(slices[0].size, slices[i].size);
            _buffer.append(slices[i].data, slices[i].size);
        }
        return Status::OK();
    }

    Slice build() override { return {_buffer.data(), _buffer.size()}; }

private:
    faststring _buffer;
};

class FLBAPlainDecoder final : public Decoder {
public:
    FLBAPlainDecoder() = default;
    ~FLBAPlainDecoder() override = default;

    static Status decode(const std::string& buffer, Slice* value) {
        value->data = const_cast<char*>(buffer.data());
        value->size = buffer.size();
        return Status::OK();
    }

    void set_type_length(int32_t type_length) override { _type_length = type_length; }

    Status set_data(const Slice& data) override {
        _data = data;
        _offset = 0;
        return Status::OK();
    }

    Status next_batch(size_t count, ColumnContentType content_type, Column* dst, const FilterData* filter) override {
        if (_offset + _type_length * count > _data.size) {
            return Status::InternalError(strings::Substitute(
                    "going to read out-of-bounds data, offset=$0,count=$1,size=$2", _offset, count, _data.size));
        }
        auto ret = dst->append_continuous_fixed_length_strings(_data.data + _offset, count, _type_length);
        if (UNLIKELY(!ret)) {
            return Status::InternalError("FLBAPlainDecoder append strings to column failed");
        }
        _offset += count * _type_length;
        return Status::OK();
    }

    Status next_batch_with_nulls(size_t count, const NullInfos& null_infos, ColumnContentType content_type, Column* dst,
                                 const FilterData* filter) override {
        if (null_infos.num_ranges <= 1) {
            return Decoder::next_batch_with_nulls(count, null_infos, content_type, dst, filter);
        }

        const uint8_t* __restrict is_nulls = null_infos.nulls_data();
        DCHECK(dst->is_nullable());
        auto* nullable_column = down_cast<NullableColumn*>(dst);
        // fill null columns
        _next_null_column(count, null_infos, nullable_column);
        // fill data columns
        auto* binary_column = down_cast<BinaryColumn*>(nullable_column->mutable_data_column());
        size_t null_cnt = null_infos.num_nulls;
        size_t read_count = count - null_cnt;

        auto& bytes = binary_column->get_bytes();
        size_t offset = bytes.size();
        {
            // fill byte columns
            size_t prev_bytes_size = bytes.size();
            size_t read_bytes_size = _type_length * read_count;
            if (_offset + read_bytes_size > _data.size) {
                return Status::InternalError(strings::Substitute(
                        "going to read out-of-bounds data, offset=$0,count=$1,size=$2", _offset, count, _data.size));
            }

            bytes.resize(prev_bytes_size + read_bytes_size);
            memcpy(bytes.data() + prev_bytes_size, _data.data + _offset, read_bytes_size);
            _offset += read_bytes_size;
        }
        auto& offsets = binary_column->get_offset();
        size_t prev_offsets = offsets.size();
        raw::stl_vector_resize_uninitialized(&offsets, count + prev_offsets);
        {
            // fill offset columns
            for (size_t i = 0; i < count; ++i) {
                offset += is_nulls[i] ? 0 : _type_length;
                offsets[prev_offsets + i] = offset;
            }
        }
        DCHECK_EQ(binary_column->get_bytes().size(), binary_column->get_offset().back());

        return Status::OK();
    }

    Status skip(size_t values_to_skip) override {
        if (_offset + _type_length * values_to_skip > _data.size) {
            return Status::InternalError(
                    strings::Substitute("going to skip out-of-bounds data, offset=$0,skip=$1,size=$2", _offset,
                                        _type_length * values_to_skip, _data.size));
        }
        _offset += _type_length * values_to_skip;
        return Status::OK();
    }

    Status next_batch(size_t count, uint8_t* dst) override {
        if (_offset + _type_length * count > _data.size) {
            return Status::InternalError(strings::Substitute(
                    "going to read out-of-bounds data, offset=$0,count=$1,size=$2", _offset, count, _data.size));
        }

        auto* slices = reinterpret_cast<Slice*>(dst);
        size_t i = 0;

#ifdef __AVX2__
        static_assert(sizeof(Slice) == sizeof(int128_t));
        __m256i fixed_length = _mm256_set1_epi64x(_type_length);
        __m256i inc = _mm256_set1_epi64x(_type_length * 4);
        __m256i offsets = _mm256_setr_epi64x(0, _type_length, _type_length * 2, _type_length * 3);
        __m256i cur = _mm256_set1_epi64x((uint64_t)(_data.data + _offset));
        cur = _mm256_add_epi64(cur, offsets);
        for (; i + 4 <= count; i += 4) {
            // mix two i64 to i128
            __m256i lo = __builtin_shufflevector(cur, fixed_length, 0, 4, 1, 4);
            __m256i hi = __builtin_shufflevector(cur, fixed_length, 2, 4, 3, 4);

            _mm256_storeu_si256(reinterpret_cast<__m256i*>(&slices[i]), lo);
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(&slices[i + 2]), hi);

            cur = _mm256_add_epi64(cur, inc);
        }
        _offset += i * _type_length;

#endif

        for (; i < count; ++i) {
            slices[i] = Slice(_data.data + _offset, _type_length);
            _offset += _type_length;
        }

        return Status::OK();
    }

private:
    Slice _data;
    size_t _type_length;
    size_t _offset = 0;
};

} // namespace starrocks::parquet
