// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "formats/parquet/utils.h"

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
    default:
        return UNKNOWN_COMPRESSION;
    }
    return UNKNOWN_COMPRESSION;
}

} // namespace starrocks::parquet
