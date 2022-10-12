// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "gen_cpp/parquet_types.h"
#include "gen_cpp/types.pb.h"

namespace starrocks::parquet {

CompressionTypePB convert_compression_codec(tparquet::CompressionCodec::type parquet_codec);

enum ColumnContentType { VALUE, DICT_CODE };

} // namespace starrocks::parquet
