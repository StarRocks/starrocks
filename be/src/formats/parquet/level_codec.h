// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <cstdint>

#include "common/status.h"
#include "formats/parquet/types.h"
#include "gen_cpp/parquet_types.h"
#include "util/bit_stream_utils.h"
#include "util/rle_encoding.h"

namespace starrocks {
class Slice;
}

namespace starrocks::parquet {

class LevelDecoder {
public:
    LevelDecoder() = default;
    ~LevelDecoder() = default;

    // Decode will try to decode data in slice, only some of the input slice will used.
    // This function will changed slice to undecoded part.
    // For example:
    //     input 1000 length data, and decoder digest 100 bytes. slice will be set to
    //     the last 900.
    Status parse(tparquet::Encoding::type encoding, level_t max_level, uint32_t num_levels, Slice* slice);

    // Try to decode n levels into levels;
    size_t decode_batch(size_t n, level_t* levels) {
        if (_encoding == tparquet::Encoding::RLE) {
            // NOTE(zc): Because RLE can only record elements that are multiples of 8,
            // it must be ensured that the incoming parameters cannot exceed the boundary.
            n = std::min((size_t)_num_levels, n);
            auto num_decoded = _rle_decoder.GetBatch(levels, n);
            _num_levels -= num_decoded;
            return num_decoded;
        } else if (_encoding == tparquet::Encoding::BIT_PACKED) {
        }
        return 0;
    }

    size_t next_repeated_count() {
        DCHECK_EQ(_encoding, tparquet::Encoding::RLE);
        return _rle_decoder.repeated_count();
    }

    level_t get_repeated_value(size_t count) { return _rle_decoder.get_repeated_value(count); }

private:
    tparquet::Encoding::type _encoding;
    level_t _bit_width = 0;
    [[maybe_unused]] level_t _max_level = 0;
    uint32_t _num_levels = 0;
    RleDecoder<level_t> _rle_decoder;
    BitReader _bit_packed_decoder;
};

} // namespace starrocks::parquet
