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

#include "formats/parquet/level_codec.h"

#include "util/bit_util.h"
#include "util/coding.h"

namespace starrocks::parquet {

Status LevelDecoder::parse(tparquet::Encoding::type encoding, level_t max_level, uint32_t num_levels, Slice* slice) {
    _encoding = encoding;
    _bit_width = BitUtil::log2(max_level + 1);
    _num_levels = num_levels;
    switch (encoding) {
    case tparquet::Encoding::RLE: {
        if (slice->size < 4) {
            return Status::InternalError("");
        }

        auto* data = (uint8_t*)slice->data;
        uint32_t num_bytes = decode_fixed32_le(data);
        if (num_bytes > slice->size - 4) {
            return Status::InternalError("");
        }
        _rle_decoder = RleDecoder<level_t>(data + 4, num_bytes, _bit_width);

        slice->data += 4 + num_bytes;
        slice->size -= 4 + num_bytes;
        break;
    }
    case tparquet::Encoding::BIT_PACKED: {
        uint32_t num_bits = num_levels * _bit_width;
        uint32_t num_bytes = BitUtil::RoundUpNumBytes(num_bits);
        if (num_bytes > slice->size) {
            return Status::Corruption("");
        }
        _bit_packed_decoder = BitReader((uint8_t*)slice->data, num_bytes);

        slice->data += num_bytes;
        slice->size -= num_bytes;
        break;
    }
    default:
        return Status::InternalError("not supported encoding");
    }
    return Status::OK();
}

} // namespace starrocks::parquet
