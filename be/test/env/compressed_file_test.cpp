// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "env/compressed_file.h"

#include <gtest/gtest.h>

#include <memory>

#include "env/env_memory.h"
#include "exec/decompressor.h"
#include "util/block_compression.h"
#include "util/random.h"

namespace starrocks {

class CompressedSequentialFileTest : public ::testing::Test {
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

    std::shared_ptr<SequentialFile> LZ4F_compress_to_file(const Slice& content) {
        const BlockCompressionCodec* codec = nullptr;
        CHECK(get_block_compression_codec(LZ4_FRAME, &codec).ok());
        size_t max_compressed_len = codec->max_compressed_len(content.size);
        std::string compressed_data(max_compressed_len, '\0');
        Slice buff(compressed_data);
        CHECK(codec->compress(content, &buff).ok());
        compressed_data.resize(buff.size);
        return std::shared_ptr<SequentialFile>(new StringSequentialFile(std::move(compressed_data)));
    }

    std::shared_ptr<Decompressor> LZ4F_decompressor() {
        std::unique_ptr<Decompressor> dec;
        CHECK(Decompressor::create_decompressor(CompressionTypePB::LZ4_FRAME, &dec).ok());
        return std::shared_ptr<Decompressor>(dec.release());
    }

    void test(const TestCase& t) {
        auto f = std::make_shared<CompressedSequentialFile>(LZ4F_compress_to_file(t.data), LZ4F_decompressor(),
                                                            t.compressed_buff_len);
        std::string decompressed_data;
        std::string own_buff(t.read_buff_len, '\0');
        decompressed_data.reserve(t.data.size);

        Slice buff(own_buff);

        Status st = f->read(&buff);
        while (st.ok() && buff.size > 0) {
            decompressed_data.append(buff.data, buff.size);
            buff = Slice(own_buff);
            st = f->read(&buff);
        }
        ASSERT_TRUE(st.ok()) << st.to_string();
        ASSERT_EQ(t.data, decompressed_data);
    }
};

// NOLINTNEXTLINE
TEST_F(CompressedSequentialFileTest, test_LZ4F) {
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

} // namespace starrocks
