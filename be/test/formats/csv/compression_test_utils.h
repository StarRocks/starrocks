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

#include <gtest/gtest.h>
#include <zlib.h>

#include <string>

#include "gen_cpp/segment.pb.h"
#include "util/compression/block_compression.h"
#include "util/slice.h"

namespace starrocks::test {

// Helper function to decompress gzip data using zlib directly
inline std::string decompress_gzip(const std::string& compressed_data) {
    z_stream z_strm = {nullptr};
    z_strm.zalloc = Z_NULL;
    z_strm.zfree = Z_NULL;
    z_strm.opaque = Z_NULL;

    // MAX_WBITS + 16 for gzip format
    int ret = inflateInit2(&z_strm, MAX_WBITS + 16);
    EXPECT_EQ(ret, Z_OK);

    z_strm.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(compressed_data.data()));
    z_strm.avail_in = compressed_data.size();

    size_t max_uncompressed_size = compressed_data.size() * 100; // Conservative estimate
    std::string uncompressed_data;
    uncompressed_data.resize(max_uncompressed_size);

    z_strm.next_out = reinterpret_cast<Bytef*>(uncompressed_data.data());
    z_strm.avail_out = max_uncompressed_size;

    ret = inflate(&z_strm, Z_FINISH);
    EXPECT_TRUE(ret == Z_OK || ret == Z_STREAM_END);

    size_t actual_size = z_strm.total_out;
    inflateEnd(&z_strm);

    uncompressed_data.resize(actual_size);
    return uncompressed_data;
}

// Helper function to decompress data with specified compression type (non-GZIP)
inline std::string decompress_data(const std::string& compressed_data, CompressionTypePB compression_type) {
    const BlockCompressionCodec* codec;
    Status st = get_block_compression_codec(compression_type, &codec);
    EXPECT_TRUE(st.ok());

    Slice input(compressed_data.data(), compressed_data.size());
    size_t max_uncompressed_size = compressed_data.size() * 100; // Conservative estimate
    std::string uncompressed_data;
    uncompressed_data.resize(max_uncompressed_size);
    Slice output(uncompressed_data.data(), max_uncompressed_size);

    st = codec->decompress(input, &output);
    EXPECT_TRUE(st.ok());

    uncompressed_data.resize(output.size);
    return uncompressed_data;
}

} // namespace starrocks::test
