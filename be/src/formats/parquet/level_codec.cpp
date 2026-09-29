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

#include "base/bit/bit_stream_utils.inline.h"
#include "base/bit/bit_util.h"
#include "base/coding.h"
#include "base/container/raw_container.h"
#include "base/simd/simd.h"
#include "base/string/slice.h"
#include "formats/parquet/utils.h"

namespace starrocks::parquet {

Status LevelDecoder::parse(tparquet::Encoding::type encoding, level_t max_level, uint32_t num_levels, Slice* slice) {
    _encoding = encoding;
    _bit_width = BitUtil::log2(max_level + 1);
    _num_levels = num_levels;
    // new page, invalid cached decode
    _levels_decoded = _levels_parsed;
    switch (encoding) {
    case tparquet::Encoding::RLE: {
        if (slice->size < 4) {
            return Status::Corruption("");
        }

        auto* data = (uint8_t*)slice->data;
        uint32_t num_bytes = decode_fixed32_le(data);
        if (num_bytes > slice->size - 4) {
            return Status::Corruption("");
        }
        _rle_decoder = RleDecoder<level_t>(data + 4, num_bytes, _bit_width);
        slice->remove_prefix(4 + num_bytes);
        break;
    }
    case tparquet::Encoding::BIT_PACKED: {
        uint32_t num_bits = num_levels * _bit_width;
        uint32_t num_bytes = BitUtil::RoundUpNumBytes(num_bits);
        if (num_bytes > slice->size) {
            return Status::Corruption("");
        }
        _bit_packed_decoder = BitReader((uint8_t*)slice->data, num_bytes);
        slice->remove_prefix(num_bytes);
        break;
    }
    default:
        return Status::InternalError("not supported encoding");
    }
    return Status::OK();
}

Status LevelDecoder::parse_v2(uint32_t num_bytes, level_t max_level, uint32_t num_levels, Slice* slice) {
    _encoding = tparquet::Encoding::RLE;
    _bit_width = BitUtil::log2(max_level + 1);
    _num_levels = num_levels;
    // new page, invalid cached decode
    _levels_decoded = _levels_parsed;
    auto* data = (uint8_t*)slice->data;
    if (num_bytes > slice->size) {
        return Status::Corruption("");
    }
    _rle_decoder = RleDecoder<level_t>(data, num_bytes, _bit_width);
    slice->remove_prefix(num_bytes);
    return Status::OK();
}

bool LevelDecoder::decode_nulls(size_t n, NullInfos* infos) {
    DCHECK(can_decode_nulls());
    SCOPED_RAW_TIMER(_timer);
    infos->reset_with_capacity(n);
    uint8_t* __restrict flags = infos->nulls_data();
    // The flags first hold the levels (0 or 1), taken from the buffer and then from the RLE stream.
    size_t buffered = std::min(n, _levels_decoded - _levels_parsed);
    for (size_t i = 0; i < buffered; ++i) {
        flags[i] = static_cast<uint8_t>(_levels[_levels_parsed + i]);
    }
    _levels_parsed += buffered;
    size_t rest = n - buffered;
    if (rest > 0) {
        // The buffer is used up (_levels_parsed == _levels_decoded), so reading on from the stream keeps the
        // buffered and stream positions in step.
        if (rest > _num_levels || !_rle_decoder.GetBitBatchAsBytes(flags + buffered, rest)) {
            return false;
        }
        _num_levels -= rest;
    }
    for (size_t i = 0; i < n; ++i) {
        flags[i] = flags[i] == 0;
    }
    count_nulls_and_ranges(flags, n, infos);
    return true;
}

bool LevelDecoder::skip_count_non_null(size_t n, size_t* num_non_null) {
    DCHECK(can_decode_nulls());
    SCOPED_RAW_TIMER(_timer);
    size_t buffered = std::min(n, _levels_decoded - _levels_parsed);
    size_t count = 0;
    for (size_t i = 0; i < buffered; ++i) {
        count += _levels[_levels_parsed + i] != 0;
    }
    _levels_parsed += buffered;
    size_t rest = n - buffered;
    if (rest > _num_levels) {
        return false;
    }
    constexpr size_t kChunk = 4096;
    while (rest > 0) {
        size_t chunk = std::min(rest, kChunk);
        raw::stl_vector_resize_uninitialized(&_skip_levels, chunk);
        if (!_rle_decoder.GetBitBatchAsBytes(_skip_levels.data(), chunk)) {
            return false;
        }
        count += SIMD::count_nonzero(_skip_levels.data(), chunk);
        _num_levels -= chunk;
        rest -= chunk;
    }
    *num_non_null = count;
    return true;
}

size_t LevelDecoder::_get_level_to_decode_batch_size(size_t row_num) {
    constexpr size_t min_level_batch_size = 4096;
    size_t levels_remaining = _levels_decoded - _levels_parsed;
    if (row_num <= levels_remaining) {
        return 0;
    }

    size_t levels_to_decode = std::max(min_level_batch_size, row_num - levels_remaining);
    levels_to_decode = std::min(levels_to_decode, static_cast<size_t>(_num_levels));
    return levels_to_decode;
}

} // namespace starrocks::parquet
