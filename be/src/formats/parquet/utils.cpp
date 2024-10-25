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

#include "formats/parquet/utils.h"

#include <glog/logging.h>

namespace starrocks::parquet {

CompressionTypePB convert_compression_codec(tparquet::CompressionCodec::type codec) {
    switch (codec) {
    case tparquet::CompressionCodec::UNCOMPRESSED:
        return NO_COMPRESSION;
    case tparquet::CompressionCodec::SNAPPY:
        return SNAPPY;
    // parquet-mr uses hadoop-lz4. more details refers to https://issues.apache.org/jira/browse/PARQUET-1878
    case tparquet::CompressionCodec::LZ4:
        return LZ4_HADOOP;
    case tparquet::CompressionCodec::ZSTD:
        return ZSTD;
    case tparquet::CompressionCodec::GZIP:
        return GZIP;
    case tparquet::CompressionCodec::LZO:
        return LZO;
    case tparquet::CompressionCodec::BROTLI:
        return BROTLI;
    case tparquet::CompressionCodec::LZ4_RAW:
        return LZ4;
    default:
        return UNKNOWN_COMPRESSION;
    }
}

int decimal_precision_to_byte_count_inner(int precision) {
    return std::ceil((std::log(std::pow(10, precision) - 1) / std::log(2) + 1) / 8);
}

int decimal_precision_to_byte_count(int precision) {
    DCHECK(precision > 0 && precision <= 38);
    static std::array<int, 39> table = {
            0,
            decimal_precision_to_byte_count_inner(1),
            decimal_precision_to_byte_count_inner(2),
            decimal_precision_to_byte_count_inner(3),
            decimal_precision_to_byte_count_inner(4),
            decimal_precision_to_byte_count_inner(5),
            decimal_precision_to_byte_count_inner(6),
            decimal_precision_to_byte_count_inner(7),
            decimal_precision_to_byte_count_inner(8),
            decimal_precision_to_byte_count_inner(9),
            decimal_precision_to_byte_count_inner(10),
            decimal_precision_to_byte_count_inner(11),
            decimal_precision_to_byte_count_inner(12),
            decimal_precision_to_byte_count_inner(13),
            decimal_precision_to_byte_count_inner(14),
            decimal_precision_to_byte_count_inner(15),
            decimal_precision_to_byte_count_inner(16),
            decimal_precision_to_byte_count_inner(17),
            decimal_precision_to_byte_count_inner(18),
            decimal_precision_to_byte_count_inner(19),
            decimal_precision_to_byte_count_inner(20),
            decimal_precision_to_byte_count_inner(21),
            decimal_precision_to_byte_count_inner(22),
            decimal_precision_to_byte_count_inner(23),
            decimal_precision_to_byte_count_inner(24),
            decimal_precision_to_byte_count_inner(25),
            decimal_precision_to_byte_count_inner(26),
            decimal_precision_to_byte_count_inner(27),
            decimal_precision_to_byte_count_inner(28),
            decimal_precision_to_byte_count_inner(29),
            decimal_precision_to_byte_count_inner(30),
            decimal_precision_to_byte_count_inner(31),
            decimal_precision_to_byte_count_inner(32),
            decimal_precision_to_byte_count_inner(33),
            decimal_precision_to_byte_count_inner(34),
            decimal_precision_to_byte_count_inner(35),
            decimal_precision_to_byte_count_inner(36),
            decimal_precision_to_byte_count_inner(37),
            decimal_precision_to_byte_count_inner(38),
    };

    return table[precision];
}

} // namespace starrocks::parquet
