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
#include <lz4/lz4frame.h>

#include <memory>

#include "base/testutil/assert.h"
#include "fs/fs_posix.h"
#include "io/string_input_stream.h"
#include "io_test_base.h"
#include "util/compression/block_compression.h"
#include "util/compression/stream_decompressor.h"
namespace starrocks::io {

class CompressedInputStreamTest : public ::testing::Test {
protected:
    struct TestCase {
        Slice data;
        size_t read_buff_len;
        size_t compressed_buff_len;
    };

    struct ReadContext {
        size_t read_buffer_size = 1024;
        size_t decompressor_buffer_size = 8 * 1024 * 1024;
    };

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

    std::shared_ptr<StreamDecompressor> LZ4F_decompressor() {
        auto dec = StreamDecompressor::create_decompressor(CompressionTypePB::LZ4_FRAME);
        CHECK(dec.ok());
        return std::shared_ptr<StreamDecompressor>(std::move(dec).value().release());
    }

    void test_lz4f_cases(const TestCase& t) {
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

    void read_compressed_file_ctx(CompressionTypePB type, const char* path, std::string& out, const ReadContext& ctx) {
        auto fs = new_fs_posix();
        auto st = fs->new_random_access_file(path);
        ASSERT_TRUE(st.ok()) << st.status().message();
        auto file = std::move(st.value());

        using DecompressorPtr = std::shared_ptr<StreamDecompressor>;
        auto dec = StreamDecompressor::create_decompressor(type);
        ASSERT_TRUE(dec.ok());

        auto compressed_input_stream = std::make_shared<io::CompressedInputStream>(
                file->stream(), DecompressorPtr(std::move(dec).value().release()), ctx.decompressor_buffer_size);

        std::vector<char> vec_buf(ctx.read_buffer_size + 1);
        char* buf = vec_buf.data();

        for (;;) {
            auto st = compressed_input_stream->read(buf, ctx.read_buffer_size);
            ASSERT_TRUE(st.ok()) << st.status().message();
            uint64_t sz = st.value();
            if (sz == 0) break;
            buf[sz] = 0;
            out += buf;
        }
    }

    void read_compressed_file(CompressionTypePB type, const char* path, std::string& out) {
        ReadContext ctx;
        read_compressed_file_ctx(type, path, out, ctx);
    }

    std::string gen_normal_frame();
    std::string gen_empty_frame();
};

std::string CompressedInputStreamTest::gen_normal_frame() {
    char src[9] = {};
    size_t compressed_len = LZ4F_compressFrameBound(sizeof(src), nullptr);
    std::unique_ptr<char[]> compressed_buf(new char[compressed_len]);

    LZ4F_preferences_t pref = LZ4F_INIT_PREFERENCES;
    pref.frameInfo.contentSize = sizeof(src);
    size_t compressed_size = LZ4F_compressFrame(compressed_buf.get(), compressed_len, src, sizeof(src), &pref);
    EXPECT_EQ(LZ4F_isError(compressed_size), 0);
    return std::string(compressed_buf.get(), compressed_size);
}

std::string CompressedInputStreamTest::gen_empty_frame() {
    size_t compressed_len = LZ4F_compressFrameBound(0, nullptr);
    std::unique_ptr<char[]> compressed_buf(new char[compressed_len]);

    size_t compressed_size = LZ4F_compressFrame(compressed_buf.get(), compressed_len, nullptr, 0, nullptr);
    EXPECT_EQ(LZ4F_isError(compressed_size), 0);
    return std::string(compressed_buf.get(), compressed_size);
}

TEST_F(CompressedInputStreamTest, test_lz4_bug_1268_1) {
    // read partial data from compressed stream
    std::string compressed_str1 = gen_normal_frame();
    auto input_stream = std::make_shared<StringInputStream>(compressed_str1);
    auto f = std::make_shared<CompressedInputStream>(input_stream, LZ4F_decompressor(), 15);
    std::string decompressed_data(1024, '\0');
    ASSERT_OK(f->read(decompressed_data.data(), 5));
    f.reset();

    // read another compressed data
    std::string compressed_str2 = gen_empty_frame();
    Slice compressed_slice2(compressed_str2);
    std::string decompressed_str2;
    decompressed_str2.resize(8192);
    Slice decompressed_slice2(decompressed_str2);
    const BlockCompressionCodec* codec = nullptr;
    EXPECT_OK(get_block_compression_codec(LZ4_FRAME, &codec));
    EXPECT_OK(codec->decompress(compressed_slice2, &decompressed_slice2));
}

TEST_F(CompressedInputStreamTest, test_lz4_bug_1268_2) {
    // read partial data from compressed stream
    std::string compressed_str1 = gen_normal_frame();
    auto input_stream = std::make_shared<StringInputStream>(compressed_str1);
    auto f = std::make_shared<CompressedInputStream>(input_stream, LZ4F_decompressor(), 9);
    std::string decompressed_data(1024, '\0');
    ASSERT_OK(f->read(decompressed_data.data(), 5));
    f.reset();

    // read empty frame
    std::string empty_frame = gen_empty_frame();
    const BlockCompressionCodec* codec = nullptr;
    EXPECT_OK(get_block_compression_codec(LZ4_FRAME, &codec));
    Slice compressed_slice(empty_frame);
    std::string decompressed_str;
    decompressed_str.resize(1024);
    Slice decompressed_slice2(decompressed_str);
    ASSERT_OK(codec->decompress(compressed_slice, &decompressed_slice2));
}

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
        test_lz4f_cases(t);
    }
}

TEST_F(CompressedInputStreamTest, test_LZO0) {
    const char* path = "be/test/exec/test_data/csv_scanner/decompress_test0.csv.lzo";
    std::string out;
    read_compressed_file(CompressionTypePB::LZO, path, out);
    std::string expected = R"(Alice,1
Bob,2
CharlieX,3
)";
    std::cout << out << "\n";
    ASSERT_EQ(out, expected);
}

TEST_F(CompressedInputStreamTest, test_LZO1) {
    const char* path = "be/test/exec/test_data/csv_scanner/decompress_test1.csv.lzo";

    std::string head = R"(0,1
1,2
2,3
3,4
4,5
5,6
6,)";

    std::string tail = R"(9998
99998,99999
99999,100000
)";

    std::vector<size_t> decompressor_buffer_sizes = {
            1024, 2048, 4096, 128 * 1024, 256 * 1024, 1024 * 1024, 2 * 1024 * 1024, 8 * 1024 * 1024};
    std::vector<size_t> read_buffer_sizes = {
            32, 1024, 2048, 4096, 128 * 1024, 256 * 1024, 1024 * 1024, 2 * 1024 * 1024, 8 * 1024 * 1024};
    for (size_t decompressor_buffer_size : decompressor_buffer_sizes) {
        for (size_t read_buffer_size : read_buffer_sizes) {
            std::cout << "test lzo1: read_buffer_size: " << read_buffer_size
                      << ", decompressor_buffer_size: " << decompressor_buffer_size << std::endl;
            std::string out;
            ReadContext ctx{.read_buffer_size = read_buffer_size, .decompressor_buffer_size = decompressor_buffer_size};
            read_compressed_file_ctx(CompressionTypePB::LZO, path, out, ctx);
            ASSERT_EQ(out.size(), 1177785);
            ASSERT_EQ(out.substr(0, head.size()), head);
            ASSERT_EQ(out.substr(out.size() - tail.size(), tail.size()), tail);
        }
    }
}

TEST_F(CompressedInputStreamTest, test_Snappy0) {
    const char* path = "be/test/exec/test_data/csv_scanner/decompress_test0.csv.snappy";
    std::string out;
    read_compressed_file(CompressionTypePB::SNAPPY, path, out);
    std::string expected = R"(Alice,1
Bob,2
CharlieX,3
)";
    std::cout << out << "\n";
    ASSERT_EQ(out, expected);
}

TEST_F(CompressedInputStreamTest, test_Snappy1) {
    const char* path = "be/test/exec/test_data/csv_scanner/decompress_test1.csv.snappy";

    std::string head = R"(0,1
1,2
2,3
3,4
4,5
5,6
6,)";

    std::string tail = R"(9998
99998,99999
99999,100000
)";

    std::vector<size_t> buffer_sizes = {
            1024, 2048, 4096, 128 * 1024, 256 * 1024, 1024 * 1024, 2 * 1024 * 1024, 8 * 1024 * 1024};
    for (size_t read_buffer_size : buffer_sizes) {
        std::string out;
        ReadContext ctx{.read_buffer_size = read_buffer_size};
        read_compressed_file_ctx(CompressionTypePB::SNAPPY, path, out, ctx);
        ASSERT_EQ(out.size(), 1177785);
        ASSERT_EQ(out.substr(0, head.size()), head);
        ASSERT_EQ(out.substr(out.size() - tail.size(), tail.size()), tail);
    }
}

} // namespace starrocks::io
