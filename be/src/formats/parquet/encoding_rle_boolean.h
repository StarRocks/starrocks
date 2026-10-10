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
#include <string>

#include "base/bit/rle_encoding.h"
#include "base/coding.h"
#include "base/string/faststring.h"
#include "base/string/slice.h"
#include "column/column.h"
#include "column/raw_data_visitor.h"
#include "common/compiler_util.h"
#include "common/status.h"
#include "formats/parquet/encoding.h"
#include "gutil/strings/substitute.h"

namespace starrocks::parquet {

// BOOLEAN values encoded as RLE (parquet-format Encodings.md, "RLE / Bit-Packing Hybrid"):
//
//   rle-bit-packed-hybrid: <length> <encoded-data>
//   length := byte count of <encoded-data>, 4 bytes little endian
//
// with a bit width of 1. parquet-mr emits this for every boolean column when the writer version is
// PARQUET_2_0, and arrow/pyarrow emit it for data_page_version=2.0, so any v2 file with a boolean
// column needs it. Repetition/definition levels use the same hybrid codec, so the decoder is a thin
// adapter over RleDecoder.
//
// The format pads the last literal group to 8 values, so like PlainDecoder<bool> this decoder cannot
// tell a read that runs at most 7 values past the end from a valid one; callers pass the page's
// num_values, as they do for every other encoding.
class RleBooleanDecoder final : public Decoder {
public:
    RleBooleanDecoder() = default;
    ~RleBooleanDecoder() override = default;

    std::string to_string() const override { return "RleBooleanDecoder"; }

    Status set_data(const Slice& data) override {
        if (UNLIKELY(data.size < kLengthPrefixBytes)) {
            return Status::Corruption(strings::Substitute(
                    "RLE boolean page is shorter than its 4-byte length prefix, size=$0", data.size));
        }
        const auto* buf = reinterpret_cast<const uint8_t*>(data.data);
        uint32_t num_bytes = decode_fixed32_le(buf);
        if (UNLIKELY(num_bytes > data.size - kLengthPrefixBytes)) {
            return Status::Corruption(
                    strings::Substitute("RLE boolean page length prefix exceeds page data, length=$0, available=$1",
                                        num_bytes, data.size - kLengthPrefixBytes));
        }
        _rle_decoder = RleDecoder<uint8_t>(buf + kLengthPrefixBytes, static_cast<int>(num_bytes), kBitWidth);
        return Status::OK();
    }

    Status next_batch(size_t count, ColumnContentType content_type, Column* dst, const FilterData* filter) override {
        size_t original_size = dst->size();
        dst->resize(original_size + count);
        MutableRawDataVisitor visitor;
        RETURN_IF_ERROR(dst->accept_mutable(&visitor));
        return _decode(count, visitor.result() + original_size);
    }

    Status skip(size_t values_to_skip) override {
        if (UNLIKELY(!_rle_decoder.Skip(values_to_skip))) {
            return Status::InternalError(
                    strings::Substitute("going to skip out-of-bounds data, skip=$0", values_to_skip));
        }
        return Status::OK();
    }

    Status next_batch(size_t count, uint8_t* dst) override { return _decode(count, dst); }

private:
    static constexpr size_t kLengthPrefixBytes = 4;
    static constexpr int kBitWidth = 1;

    Status _decode(size_t count, uint8_t* dst) {
        if (UNLIKELY(!_rle_decoder.GetBatch(dst, count))) {
            return Status::InternalError(strings::Substitute("going to read out-of-bounds data, count=$0", count));
        }
        return Status::OK();
    }

    RleDecoder<uint8_t> _rle_decoder;
};

class RleBooleanEncoder final : public Encoder {
public:
    RleBooleanEncoder() : _rle_encoder(&_payload, kBitWidth) {}
    ~RleBooleanEncoder() override = default;

    Status append(const uint8_t* vals, size_t count) override {
        if (UNLIKELY(_built)) {
            return Status::InternalError("RleBooleanEncoder: append after build");
        }
        for (size_t i = 0; i < count; ++i) {
            _rle_encoder.Put(vals[i] != 0 ? 1 : 0);
        }
        return Status::OK();
    }

    // build() may be called more than once; the stream is finalized on the first call.
    Slice build() override {
        if (!_built) {
            int payload_len = _rle_encoder.Flush();
            _out.clear();
            put_fixed32_le(&_out, static_cast<uint32_t>(payload_len));
            _out.append(_payload.data(), payload_len);
            _built = true;
        }
        return {_out.data(), _out.size()};
    }

    std::string to_string() const override { return "RleBooleanEncoder"; }

private:
    static constexpr int kBitWidth = 1;

    faststring _payload;
    RleEncoder<uint8_t> _rle_encoder;
    faststring _out;
    bool _built = false;
};

} // namespace starrocks::parquet
