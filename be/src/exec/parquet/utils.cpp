// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/parquet/utils.h"

namespace starrocks::parquet {

CompressionTypePB convert_compression_codec(tparquet::CompressionCodec::type codec) {
    switch (codec) {
    case tparquet::CompressionCodec::UNCOMPRESSED:
        return NO_COMPRESSION;
    case tparquet::CompressionCodec::SNAPPY:
        return SNAPPY;
    case tparquet::CompressionCodec::LZ4:
        return LZ4;
    case tparquet::CompressionCodec::ZSTD:
        return ZSTD;
    case tparquet::CompressionCodec::GZIP:
        return GZIP;
    case tparquet::CompressionCodec::LZO:
        return LZO;
    case tparquet::CompressionCodec::BROTLI:
        return BROTLI;
    default:
        return UNKNOWN_COMPRESSION;
    }
    return UNKNOWN_COMPRESSION;
}

} // namespace starrocks::parquet
