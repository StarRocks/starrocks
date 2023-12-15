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
#include "gutil/strings/substitute.h"
#include "util/bit_stream_utils.h"
#include "util/coding.h"
#include "util/faststring.h"
#include "util/slice.h"

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

    Status next_batch(size_t count, ColumnContentType content_type, Column* dst) override {
        size_t max_fetch = count * SIZE_OF_TYPE;
        if (max_fetch + _offset > _data.size) {
            return Status::InternalError(strings::Substitute(
                    "going to read out-of-bounds data, offset=$0,count=$1,size=$2", _offset, count, _data.size));
        }
        auto n = dst->append_numbers(_data.data + _offset, max_fetch);
        CHECK_EQ(count, n);
        _offset += max_fetch;
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

    Status next_batch(size_t count, ColumnContentType content_type, Column* dst) override {
        std::vector<Slice> slices;
        slices.reserve(count);

        size_t num_decoded = 0;
        while (num_decoded < count && _offset < _data.size) {
            uint32_t length = decode_fixed32_le(reinterpret_cast<const uint8_t*>(_data.data) + _offset);
            _offset += sizeof(int32_t);
            slices.emplace_back(_data.data + _offset, length);
            _offset += length;
            num_decoded++;
        }
        // never happend
        if (num_decoded < count || _offset > _data.size) {
            return Status::InternalError(strings::Substitute(
                    "going to read out-of-bounds data, offset=$0,count=$1,size=$2", _offset, count, _data.size));
        }
        auto ret = dst->append_strings(slices);
        if (UNLIKELY(!ret)) {
            return Status::InternalError("PlainDecoder append strings to column failed");
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

    Status next_batch(size_t count, ColumnContentType content_type, Column* dst) override {
        auto original_size = dst->size();
        dst->resize(original_size + count);
        auto num_unpacked_values = unpack_batch(count, dst->mutable_raw_data() + original_size);
        if (num_unpacked_values < count) {
            return Status::InternalError(strings::Substitute(
                    "going to read out-of-bounds data, count=$0,num_unpacked_values=$1", count, num_unpacked_values));
        }
        return Status::OK();
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

    void set_type_legth(int32_t type_length) override { _type_length = type_length; }

    Status set_data(const Slice& data) override {
        _data = data;
        _offset = 0;
        return Status::OK();
    }

    Status next_batch(size_t count, ColumnContentType content_type, Column* dst) override {
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

    Status next_batch(size_t count, uint8_t* dst) override {
        if (_offset + _type_length * count > _data.size) {
            return Status::InternalError(strings::Substitute(
                    "going to read out-of-bounds data, offset=$0,count=$1,size=$2", _offset, count, _data.size));
        }

        auto* slices = reinterpret_cast<Slice*>(dst);
        for (int i = 0; i < count; ++i) {
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
