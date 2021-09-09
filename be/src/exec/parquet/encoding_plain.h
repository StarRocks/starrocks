// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/column.h"
#include "common/status.h"
#include "exec/parquet/encoding.h"
#include "gutil/strings/substitute.h"
#include "util/coding.h"
#include "util/faststring.h"
#include "util/slice.h"

namespace starrocks::parquet {

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

    Slice build() override { return Slice(_buffer.data(), _buffer.size()); }

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
        const Slice* slices = (const Slice*)vals;
        for (int i = 0; i < count; ++i) {
            put_fixed32_le(&_buffer, slices[i].size);
            _buffer.append(slices[i].data, slices[i].size);
        }
        return Status::OK();
    }

    Slice build() override { return Slice(_buffer.data(), _buffer.size()); }

private:
    faststring _buffer;
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

    Status next_batch(size_t count, ColumnContentType content_type, vectorized::Column* dst) override {
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

    Status next_batch(size_t count, ColumnContentType content_type, vectorized::Column* dst) override {
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
        dst->append_strings(slices);
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

// Fixed length byte array encoder and decoder
class FLBAPlainEncoder final : public Encoder {
public:
    FLBAPlainEncoder() = default;
    ~FLBAPlainEncoder() override = default;

    Status append(const uint8_t* vals, size_t count) override {
        if (count == 0) return Status::OK();

        const Slice* slices = (const Slice*)vals;
        _buffer.reserve(_buffer.size() + count * slices[0].size);
        for (int i = 0; i < count; ++i) {
            DCHECK_EQ(slices[0].size, slices[i].size);
            _buffer.append(slices[i].data, slices[i].size);
        }
        return Status::OK();
    }

    Slice build() override { return Slice(_buffer.data(), _buffer.size()); }

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

    Status next_batch(size_t count, ColumnContentType content_type, vectorized::Column* dst) override {
        if (_offset + _type_length * count > _data.size) {
            return Status::InternalError(strings::Substitute(
                    "going to read out-of-bounds data, offset=$0,count=$1,size=$2", _offset, count, _data.size));
        }
        std::vector<Slice> slices;
        slices.resize(count);
        for (int i = 0; i < count; ++i) {
            slices[i] = Slice(_data.data + _offset, _type_length);
            _offset += _type_length;
        }

        dst->append_strings(slices);
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
