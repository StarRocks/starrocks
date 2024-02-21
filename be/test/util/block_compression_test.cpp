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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/util/block_compression_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "util/compression/block_compression.h"

#include <gtest/gtest.h>

#include <iostream>
#include <thread>

#include "gen_cpp/segment.pb.h"
#include "util/faststring.h"
#include "util/random.h"
#include "util/raw_container.h"

namespace starrocks {

static std::string random_string(int len) {
    static starrocks::Random rand(20200722);
    std::string s;
    s.reserve(len * 5);
    for (int i = 0; i < len; i++) {
        char c = 'a' + rand.Next() % ('z' - 'a' + 1);
        std::string tmp_str =
                std::to_string(c) + std::to_string(c) + std::to_string(c) + std::to_string(c) + std::to_string(c);
        s.append(tmp_str);
    }
    return s;
}

class BlockCompressionTest : public testing::Test {
public:
    BlockCompressionTest() = default;
    ~BlockCompressionTest() override = default;
};

static std::string generate_str(size_t len) {
    static char charset[] =
            "0123456789"
            "abcdefghijklmnopqrstuvwxyz"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    std::string result;
    result.resize(len);
    for (int i = 0; i < len; ++i) {
        result[i] = charset[rand() % sizeof(charset)];
    }
    return result;
}

void test_single_slice(starrocks::CompressionTypePB type) {
    const BlockCompressionCodec* codec = nullptr;
    auto st = get_block_compression_codec(type, &codec);
    ASSERT_TRUE(st.ok());

    size_t test_sizes[] = {0, 1, 10, 1000, 1000000};
    for (auto size : test_sizes) {
        auto orig = generate_str(size);
        size_t max_len = codec->max_compressed_len(size);
        std::string compressed;
        compressed.resize(max_len);
        {
            Slice compressed_slice(compressed);
            st = codec->compress(orig, &compressed_slice);
            ASSERT_TRUE(st.ok());

            std::string uncompressed;
            uncompressed.resize(size);
            {
                Slice uncompressed_slice(uncompressed);
                st = codec->decompress(compressed_slice, &uncompressed_slice);
                ASSERT_TRUE(st.ok());

                ASSERT_STREQ(orig.c_str(), uncompressed.c_str());
            }

            if (type == starrocks::CompressionTypePB::LZ4) {
                Slice uncompressed_slice(uncompressed);
                const BlockCompressionCodec* lz4_hadoop_codec = nullptr;
                st = get_block_compression_codec(starrocks::CompressionTypePB::LZ4_HADOOP, &lz4_hadoop_codec);
                ASSERT_TRUE(st.ok());
                st = lz4_hadoop_codec->decompress(compressed_slice, &uncompressed_slice);
                ASSERT_TRUE(st.ok());

                ASSERT_STREQ(orig.c_str(), uncompressed.c_str());
            }

            // buffer not enough for decompress
            // snappy has no return value if given buffer is not enough
            // NOTE: For ZLIB, we even get OK with a insufficient output
            // when uncompressed size is 1
            if ((type == starrocks::CompressionTypePB::ZLIB && uncompressed.size() > 1) &&
                type != starrocks::CompressionTypePB::SNAPPY && uncompressed.size() > 0) {
                Slice uncompressed_slice(uncompressed);
                uncompressed_slice.size -= 1;
                st = codec->decompress(compressed_slice, &uncompressed_slice);
                ASSERT_FALSE(st.ok());
            }
            // corrupt compressed data
            // we use inflate for gzip decompressor, it will return Z_OK for this case
            if (type != starrocks::CompressionTypePB::SNAPPY && type != starrocks::CompressionTypePB::GZIP) {
                Slice uncompressed_slice(uncompressed);
                compressed_slice.size -= 1;
                st = codec->decompress(compressed_slice, &uncompressed_slice);
                ASSERT_FALSE(st.ok());
                compressed_slice.size += 1;
            }
        }
        // buffer not enough for compress
        if (type != starrocks::CompressionTypePB::SNAPPY && size > 0) {
            Slice compressed_slice(compressed);
            compressed_slice.size = 1;
            st = codec->compress(orig, &compressed_slice);
            ASSERT_FALSE(st.ok());
        }
    }
}

TEST_F(BlockCompressionTest, single) {
    test_single_slice(starrocks::CompressionTypePB::ZSTD);
    test_single_slice(starrocks::CompressionTypePB::SNAPPY);
    test_single_slice(starrocks::CompressionTypePB::ZLIB);
    test_single_slice(starrocks::CompressionTypePB::LZ4);
    test_single_slice(starrocks::CompressionTypePB::LZ4_FRAME);
    test_single_slice(starrocks::CompressionTypePB::GZIP);
    test_single_slice(starrocks::CompressionTypePB::LZ4_HADOOP);
}

void test_multi_slices(starrocks::CompressionTypePB type) {
    const BlockCompressionCodec* codec = nullptr;
    auto st = get_block_compression_codec(type, &codec);
    ASSERT_TRUE(st.ok());

    size_t test_sizes[] = {0, 1, 10, 1000, 1000000};
    std::vector<std::string> orig_strs;
    for (auto size : test_sizes) {
        orig_strs.emplace_back(generate_str(size));
    }
    std::vector<Slice> orig_slices;
    std::string orig;
    for (auto& str : orig_strs) {
        orig_slices.emplace_back(str);
        orig.append(str);
    }

    size_t total_size = orig.size();
    size_t max_len = codec->max_compressed_len(total_size);

    std::string compressed;
    compressed.resize(max_len);
    {
        Slice compressed_slice(compressed);
        st = codec->compress(orig_slices, &compressed_slice);
        ASSERT_TRUE(st.ok());

        std::string uncompressed;
        uncompressed.resize(total_size);
        // normal case
        {
            Slice uncompressed_slice(uncompressed);
            st = codec->decompress(compressed_slice, &uncompressed_slice);
            ASSERT_TRUE(st.ok());

            ASSERT_STREQ(orig.c_str(), uncompressed.c_str());
        }

        if (type == starrocks::CompressionTypePB::LZ4) {
            Slice uncompressed_slice(uncompressed);
            const BlockCompressionCodec* lz4_hadoop_codec = nullptr;
            st = get_block_compression_codec(starrocks::CompressionTypePB::LZ4_HADOOP, &lz4_hadoop_codec);
            ASSERT_TRUE(st.ok());
            st = lz4_hadoop_codec->decompress(compressed_slice, &uncompressed_slice);
            ASSERT_TRUE(st.ok());

            ASSERT_STREQ(orig.c_str(), uncompressed.c_str());
        }
    }

    // buffer not enough failed
    if (type != starrocks::CompressionTypePB::SNAPPY) {
        Slice compressed_slice(compressed);
        compressed_slice.size = 10;
        st = codec->compress(orig, &compressed_slice);
        ASSERT_FALSE(st.ok());
    }
}

TEST_F(BlockCompressionTest, multi) {
    test_multi_slices(starrocks::CompressionTypePB::SNAPPY);
    test_multi_slices(starrocks::CompressionTypePB::ZLIB);
    test_multi_slices(starrocks::CompressionTypePB::LZ4);
    test_multi_slices(starrocks::CompressionTypePB::LZ4_FRAME);
    test_multi_slices(starrocks::CompressionTypePB::ZSTD);
    test_multi_slices(starrocks::CompressionTypePB::GZIP);
}

TEST_F(BlockCompressionTest, test_issue_10721) {
    std::string str = random_string(1024);
    const BlockCompressionCodec* codec = nullptr;
    auto st = get_block_compression_codec(starrocks::CompressionTypePB::ZSTD, &codec);
    ASSERT_TRUE(st.ok());

    Slice orig_slice = str;
    size_t total_size = str.size();
    faststring compressed;
    Slice compressed_slice;
    st = codec->compress(orig_slice, &compressed_slice, true, total_size, &compressed, nullptr);
    compressed.shrink_to_fit();
    ASSERT_TRUE(st.ok());
}

static const size_t kBenchmarkCompressionTimes = 1000;
static const size_t kBenchmarkCompressionConcurrentThreads = 32;
static const size_t kBenchmarkCompressionMultiSliceNum = 2;
static const size_t str_length = 1024 * 64;

void benchmark_single_slice_compression(starrocks::CompressionTypePB type, std::string& str) {
    const BlockCompressionCodec* codec = nullptr;
    auto st = get_block_compression_codec(type, &codec);
    ASSERT_TRUE(st.ok());

    const std::string& orig = str;
    Slice orig_slices(orig);

    size_t total_size = orig.size();
    size_t max_len = codec->max_compressed_len(total_size);

    for (int i = 0; i < kBenchmarkCompressionTimes; i++) {
        std::string compressed;
        compressed.resize(max_len);
        Slice compressed_slice(compressed);
        st = codec->compress(orig_slices, &compressed_slice);
        ASSERT_TRUE(st.ok());
        compressed.resize(compressed_slice.size);
    }
}

void benchmark_compression(starrocks::CompressionTypePB type, std::string& str) {
    const BlockCompressionCodec* codec = nullptr;
    auto st = get_block_compression_codec(type, &codec);
    ASSERT_TRUE(st.ok());

    std::vector<std::string> orig_strs;
    for (int i = 0; i < kBenchmarkCompressionMultiSliceNum; i++) {
        orig_strs.emplace_back(str);
    }
    std::vector<Slice> orig_slices;
    std::string orig;
    for (auto& str : orig_strs) {
        orig_slices.emplace_back(str);
        orig.append(str);
    }

    size_t total_size = orig.size();
    size_t max_len = codec->max_compressed_len(total_size);

    for (int i = 0; i < kBenchmarkCompressionTimes; i++) {
        std::string compressed;
        compressed.resize(max_len);
        Slice compressed_slice(compressed);
        st = codec->compress(orig_slices, &compressed_slice);
        ASSERT_TRUE(st.ok());
        compressed.resize(compressed_slice.size);
        compressed.shrink_to_fit();
    }
}

void benchmark_compression_buffer(starrocks::CompressionTypePB type, std::string& str) {
    const BlockCompressionCodec* codec = nullptr;
    auto st = get_block_compression_codec(type, &codec);
    ASSERT_TRUE(st.ok());

    std::vector<std::string> orig_strs;
    for (int i = 0; i < kBenchmarkCompressionMultiSliceNum; i++) {
        orig_strs.emplace_back(str);
    }
    std::vector<Slice> orig_slices;
    std::string orig;
    for (auto& str : orig_strs) {
        orig_slices.emplace_back(str);
        orig.append(str);
    }

    size_t total_size = orig.size();
    for (int i = 0; i < kBenchmarkCompressionTimes; i++) {
        faststring compressed;
        Slice compressed_slice;
        st = codec->compress(orig_slices, &compressed_slice, true, total_size, &compressed, nullptr);
        compressed.shrink_to_fit();
        ASSERT_TRUE(st.ok());
    }
}

void benchmark_decompression(starrocks::CompressionTypePB type, std::string& str) {
    const BlockCompressionCodec* codec = nullptr;
    auto st = get_block_compression_codec(type, &codec);
    ASSERT_TRUE(st.ok());

    std::vector<std::string> orig_strs;
    for (int i = 0; i < kBenchmarkCompressionMultiSliceNum; i++) {
        orig_strs.emplace_back(str);
    }
    std::vector<Slice> orig_slices;
    std::string orig;
    for (auto& str : orig_strs) {
        orig_slices.emplace_back(str);
        orig.append(str);
    }

    size_t total_size = orig.size();
    size_t max_len = codec->max_compressed_len(total_size);

    std::string compressed;
    compressed.resize(max_len);
    Slice compressed_slice(compressed);
    st = codec->compress(orig_slices, &compressed_slice);
    ASSERT_TRUE(st.ok());

    for (int i = 0; i < kBenchmarkCompressionTimes; i++) {
        std::string uncompressed;
        uncompressed.resize(total_size);
        // normal case
        {
            Slice uncompressed_slice(uncompressed);
            st = codec->decompress(compressed_slice, &uncompressed_slice);
            ASSERT_TRUE(st.ok());

            ASSERT_STREQ(orig.c_str(), uncompressed.c_str());
        }
    }
}

TEST_F(BlockCompressionTest, LZ4F_compression_LARGE_PAGE_TEST) {
    std::string str = random_string(1024 * 5);
    CompressionTypePB type = starrocks::CompressionTypePB::LZ4_FRAME;

    const BlockCompressionCodec* codec = nullptr;
    auto st = get_block_compression_codec(type, &codec);
    ASSERT_TRUE(st.ok());

    std::vector<std::string> orig_strs;
    for (int i = 0; i < kBenchmarkCompressionMultiSliceNum; i++) {
        orig_strs.emplace_back(str);
    }
    std::vector<Slice> orig_slices;
    std::string orig;
    for (auto& str : orig_strs) {
        orig_slices.emplace_back(str);
        orig.append(str);
    }

    size_t total_size = orig.size();
    raw::RawString compressed;
    Slice compressed_slice;
    st = codec->compress(orig_slices, &compressed_slice, true, total_size, nullptr, &compressed);
    ASSERT_TRUE(st.ok());
}

//#define LZ4_BENCHMARK
//#define LZ4F_BENCHMARK
//#define ZSTD_BENCHMARK

#ifdef LZ4_BENCHMARK
TEST_F(BlockCompressionTest, LZ4_benchmark_single_slice_compression) {
    std::string str = random_string(str_length);
    benchmark_single_slice_compression(starrocks::CompressionTypePB::LZ4, str);
}

TEST_F(BlockCompressionTest, LZ4_benchmark_compression) {
    std::string str = random_string(str_length);
    benchmark_compression(starrocks::CompressionTypePB::LZ4, str);
}

TEST_F(BlockCompressionTest, LZ4_benchmark_compression_buffer) {
    std::string str = random_string(str_length);
    benchmark_compression_buffer(starrocks::CompressionTypePB::LZ4, str);
}

TEST_F(BlockCompressionTest, LZ4_benchmark_decompression) {
    std::string str = random_string(str_length);
    benchmark_decompression(starrocks::CompressionTypePB::LZ4, str);
}

TEST_F(BlockCompressionTest, MultiThread_LZ4_benchmark_compression) {
    std::string str = random_string(str_length);
    std::vector<std::shared_ptr<std::thread>> threads;
    for (int i = 0; i < kBenchmarkCompressionConcurrentThreads; i++) {
        threads.push_back(std::shared_ptr<std::thread>(
                new std::thread([this, &str] { benchmark_compression(starrocks::CompressionTypePB::LZ4, str); })));
    }
    for (int i = 0; i < threads.size(); ++i) {
        threads[i]->join();
    }
}

TEST_F(BlockCompressionTest, MultiThread_LZ4_benchmark_compression_buffer) {
    std::string str = random_string(str_length);
    std::vector<std::shared_ptr<std::thread>> threads;
    for (int i = 0; i < kBenchmarkCompressionConcurrentThreads; i++) {
        threads.push_back(std::shared_ptr<std::thread>(new std::thread(
                [this, &str] { benchmark_compression_buffer(starrocks::CompressionTypePB::LZ4, str); })));
    }
    for (int i = 0; i < threads.size(); ++i) {
        threads[i]->join();
    }
}

TEST_F(BlockCompressionTest, MultiThread_LZ4_benchmark_decompression) {
    std::string str = random_string(str_length);
    std::vector<std::shared_ptr<std::thread>> threads;
    for (int i = 0; i < kBenchmarkCompressionConcurrentThreads; i++) {
        threads.push_back(std::shared_ptr<std::thread>(
                new std::thread([this, &str] { benchmark_decompression(starrocks::CompressionTypePB::LZ4, str); })));
    }
    for (int i = 0; i < threads.size(); ++i) {
        threads[i]->join();
    }
}
#endif

#ifdef LZ4F_BENCHMARK
TEST_F(BlockCompressionTest, LZ4F_benchmark_single_slice_compression) {
    std::string str = random_string(str_length);
    benchmark_single_slice_compression(starrocks::CompressionTypePB::LZ4_FRAME, str);
}

TEST_F(BlockCompressionTest, LZ4F_benchmark_compression) {
    std::string str = random_string(str_length);
    benchmark_compression(starrocks::CompressionTypePB::LZ4_FRAME, str);
}

TEST_F(BlockCompressionTest, LZ4F_benchmark_compression_buffer) {
    std::string str = random_string(str_length);
    benchmark_compression_buffer(starrocks::CompressionTypePB::LZ4_FRAME, str);
}

TEST_F(BlockCompressionTest, LZ4F_benchmark_decompression) {
    std::string str = random_string(str_length);
    benchmark_decompression(starrocks::CompressionTypePB::LZ4_FRAME, str);
}

TEST_F(BlockCompressionTest, MultiThread_LZ4F_benchmark_compression) {
    std::string str = random_string(str_length);
    std::vector<std::shared_ptr<std::thread>> threads;
    for (int i = 0; i < kBenchmarkCompressionConcurrentThreads; i++) {
        threads.push_back(std::shared_ptr<std::thread>(new std::thread(
                [this, &str] { benchmark_compression(starrocks::CompressionTypePB::LZ4_FRAME, str); })));
    }
    for (int i = 0; i < threads.size(); ++i) {
        threads[i]->join();
    }
}

TEST_F(BlockCompressionTest, MultiThread_LZ4F_benchmark_compression_buffer) {
    std::string str = random_string(str_length);
    std::vector<std::shared_ptr<std::thread>> threads;
    for (int i = 0; i < kBenchmarkCompressionConcurrentThreads; i++) {
        threads.push_back(std::shared_ptr<std::thread>(new std::thread(
                [this, &str] { benchmark_compression_buffer(starrocks::CompressionTypePB::LZ4_FRAME, str); })));
    }
    for (int i = 0; i < threads.size(); ++i) {
        threads[i]->join();
    }
}

TEST_F(BlockCompressionTest, MultiThread_LZ4F_benchmark_decompression) {
    std::string str = random_string(str_length);
    std::vector<std::shared_ptr<std::thread>> threads;
    for (int i = 0; i < kBenchmarkCompressionConcurrentThreads; i++) {
        threads.push_back(std::shared_ptr<std::thread>(new std::thread(
                [this, &str] { benchmark_decompression(starrocks::CompressionTypePB::LZ4_FRAME, str); })));
    }
    for (int i = 0; i < threads.size(); ++i) {
        threads[i]->join();
    }
}
#endif

#ifdef ZSTD_BENCHMARK
TEST_F(BlockCompressionTest, ZSTD_benchmark_single_slice_compression) {
    std::string str = random_string(str_length);
    benchmark_single_slice_compression(starrocks::CompressionTypePB::ZSTD, str);
}

TEST_F(BlockCompressionTest, ZSTD_benchmark_compression) {
    std::string str = random_string(str_length);
    benchmark_compression(starrocks::CompressionTypePB::ZSTD, str);
}

TEST_F(BlockCompressionTest, ZSTD_benchmark_compression_buffer) {
    std::string str = random_string(str_length);
    benchmark_compression_buffer(starrocks::CompressionTypePB::ZSTD, str);
}

TEST_F(BlockCompressionTest, ZSTD_benchmark_compression_decompression) {
    std::string str = random_string(str_length);
    benchmark_decompression(starrocks::CompressionTypePB::ZSTD, str);
}

TEST_F(BlockCompressionTest, MultiThread_ZSTD_benchmark_compression) {
    std::string str = random_string(str_length);
    std::vector<std::shared_ptr<std::thread>> threads;
    for (int i = 0; i < kBenchmarkCompressionConcurrentThreads; i++) {
        threads.push_back(std::shared_ptr<std::thread>(
                new std::thread([this, &str] { benchmark_compression(starrocks::CompressionTypePB::ZSTD, str); })));
    }
    for (int i = 0; i < threads.size(); ++i) {
        threads[i]->join();
    }
}

TEST_F(BlockCompressionTest, MultiThread_ZSTD_benchmark_compression_buffer) {
    std::string str = random_string(str_length);
    std::vector<std::shared_ptr<std::thread>> threads;
    for (int i = 0; i < kBenchmarkCompressionConcurrentThreads; i++) {
        threads.push_back(std::shared_ptr<std::thread>(new std::thread(
                [this, &str] { benchmark_compression_buffer(starrocks::CompressionTypePB::ZSTD, str); })));
    }
    for (int i = 0; i < threads.size(); ++i) {
        threads[i]->join();
    }
}

TEST_F(BlockCompressionTest, MultiThread_ZSTD_benchmark_decompression) {
    std::string str = random_string(str_length);
    std::vector<std::shared_ptr<std::thread>> threads;
    for (int i = 0; i < kBenchmarkCompressionConcurrentThreads; i++) {
        threads.push_back(std::shared_ptr<std::thread>(
                new std::thread([this, &str] { benchmark_decompression(starrocks::CompressionTypePB::ZSTD, str); })));
    }
    for (int i = 0; i < threads.size(); ++i) {
        threads[i]->join();
    }
}
#endif
} // namespace starrocks
