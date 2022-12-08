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

#include "io/compressed_input_stream.h"

#include <gtest/gtest.h>

#include <memory>

#include "io/string_input_stream.h"
#include "testutil/assert.h"
#include "util/compression/block_compression.h"
#include "util/compression/stream_compression.h"
#include "util/random.h"

namespace starrocks::io {

class CompressedInputStreamTest : public ::testing::Test {
protected:
    struct TestCase {
        Slice data;
        size_t read_buff_len;
        size_t compressed_buff_len;
    };

    static std::string random_string(int len) {
        static starrocks::Random rand(20200722);
        std::string s;
        s.reserve(len);
        for (int i = 0; i < len; i++) {
            s.push_back('a' + (rand.Next() % ('z' - 'a' + 1)));
        }
        return s;
    }

    std::shared_ptr<InputStream> LZ4F_compress_to_file(const Slice& content) {
        const BlockCompressionCodec* codec = nullptr;
        CHECK(get_block_compression_codec(LZ4_FRAME, &codec).ok());
        size_t max_compressed_len = codec->max_compressed_len(content.size);
        std::string compressed_data(max_compressed_len, '\0');
        Slice buff(compressed_data);
        CHECK(codec->compress(content, &buff).ok());
        compressed_data.resize(buff.size);
        return std::shared_ptr<InputStream>(new StringInputStream(std::move(compressed_data)));
    }

    std::shared_ptr<StreamCompression> LZ4F_decompressor() {
        std::unique_ptr<StreamCompression> dec;
        CHECK(StreamCompression::create_decompressor(CompressionTypePB::LZ4_FRAME, &dec).ok());
        return std::shared_ptr<StreamCompression>(dec.release());
    }

    void test(const TestCase& t) {
        auto f = std::make_shared<CompressedInputStream>(LZ4F_compress_to_file(t.data), LZ4F_decompressor(),
                                                         t.compressed_buff_len);
        std::string decompressed_data;
        std::string own_buff(t.read_buff_len, '\0');
        decompressed_data.reserve(t.data.size);

        ASSIGN_OR_ABORT(auto nread, f->read(own_buff.data(), own_buff.size()));
        while (nread > 0) {
            decompressed_data.append(own_buff.data(), nread);
            ASSIGN_OR_ABORT(nread, f->read(own_buff.data(), own_buff.size()));
        }
        ASSERT_EQ(t.data, decompressed_data);
    }
};

// NOLINTNEXTLINE
TEST_F(CompressedInputStreamTest, test_LZ4F) {
    const size_t K1 = 1024;
    const size_t M1 = 1024 * 1024;
    const std::string STR_1K = random_string(K1);
    const std::string STR_1M = random_string(M1);
    const std::string STR_10M = random_string(10 * M1);
    const std::string STR_100M = random_string(100 * M1);

    // clang-format off
    TestCase cases[] = {
            {"StarRocks", 1, M1},
            {STR_10M, K1, 2 * K1},
            {STR_100M, M1, M1},
    };
    // clang-format on

    for (const auto& t : cases) {
        test(t);
    }
}

} // namespace starrocks::io
