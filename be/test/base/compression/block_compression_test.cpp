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

#include "base/compression/block_compression.h"

#include <gtest/gtest.h>

#include <iostream>
#include <thread>

#include "base/compression/compression_context_pool_singletons.h"
#include "base/compression/zstd_dict.h"
#include "base/container/raw_container.h"
#include "base/random/random.h"
#include "base/string/faststring.h"
#include "gen_cpp/segment.pb.h"

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

// A ColumnMetaPB that never set compression_level reads back 0 (segment.proto declares no default
// sentinel). ZSTD must fall back to its default level rather than handing back a null codec, which
// callers such as PageIO interpret as "write the page body uncompressed".
TEST_F(BlockCompressionTest, zstd_unset_compression_level_is_not_no_compression) {
    for (int level : {0, -1, -5, 23, 100}) {
        const BlockCompressionCodec* codec = nullptr;
        ASSERT_TRUE(get_block_compression_codec(starrocks::CompressionTypePB::ZSTD, &codec, level).ok())
                << "level=" << level;
        ASSERT_NE(nullptr, codec) << "level=" << level;
    }

    // In-range levels still get the level-specific instance.
    for (int level : {1, 3, 22}) {
        const BlockCompressionCodec* codec = nullptr;
        ASSERT_TRUE(get_block_compression_codec(starrocks::CompressionTypePB::ZSTD, &codec, level).ok())
                << "level=" << level;
        ASSERT_NE(nullptr, codec) << "level=" << level;
    }

    // NO_COMPRESSION is the only type allowed to yield a null codec.
    const BlockCompressionCodec* none_codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(starrocks::CompressionTypePB::NO_COMPRESSION, &none_codec).ok());
    ASSERT_EQ(nullptr, none_codec);
}

// A page written with an unset level must round-trip as genuinely compressed data.
TEST_F(BlockCompressionTest, zstd_unset_compression_level_actually_compresses) {
    const BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(starrocks::CompressionTypePB::ZSTD, &codec, 0).ok());
    ASSERT_NE(nullptr, codec);

    // Highly repetitive input, so a working codec must shrink it substantially.
    std::string orig(64 * 1024, 'a');
    std::string compressed;
    compressed.resize(codec->max_compressed_len(orig.size()));
    Slice compressed_slice(compressed);
    ASSERT_TRUE(codec->compress(orig, &compressed_slice).ok());
    ASSERT_LT(compressed_slice.get_size(), orig.size() / 10);

    std::string uncompressed;
    uncompressed.resize(orig.size());
    Slice uncompressed_slice(uncompressed);
    ASSERT_TRUE(codec->decompress(compressed_slice, &uncompressed_slice).ok());
    ASSERT_EQ(orig, uncompressed_slice.to_string());
}

TEST_F(BlockCompressionTest, lz4_explicit_options) {
    const BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(starrocks::CompressionTypePB::LZ4, &codec).ok());

    std::string orig = random_string(1024);
    std::string compressed;
    compressed.resize(codec->max_compressed_len(orig.size()));

    BlockCompressionOptions options;
    options.lz4_acceleration = 4;

    Slice compressed_slice(compressed);
    ASSERT_TRUE(codec->compress(orig, &compressed_slice, options).ok());

    std::string uncompressed;
    uncompressed.resize(orig.size());
    Slice uncompressed_slice(uncompressed);
    ASSERT_TRUE(codec->decompress(compressed_slice, &uncompressed_slice).ok());
    ASSERT_EQ(orig, uncompressed);
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
[[maybe_unused]] static const size_t kBenchmarkCompressionConcurrentThreads = 32;
static const size_t kBenchmarkCompressionMultiSliceNum = 2;
[[maybe_unused]] static const size_t str_length = 1024 * 64;

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

TEST_F(BlockCompressionTest, test_multi_thread_get_ctx) {
    for (int j = 0; j < 10; j++) {
        std::vector<std::thread> workers;
        for (int cnt = 0; cnt < 30; cnt++) {
            workers.emplace_back([]() {
                for (uint64_t i = 1; i < 1000; i++) {
                    StatusOr<compression::LZ4F_CCtx_Pool::Ref> ref = compression::getLZ4F_CCtx();
                }
            });
        }
        for (auto& worker : workers) {
            worker.join();
        }
    }
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

// ===================== compression dict compression-dict codec tests =====================

// Roundtrip: a body compressed referencing a CDict decodes correctly with the
// matching DDict built from the same sample.
TEST_F(BlockCompressionTest, zstd_compression_dict_roundtrip) {
    const BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(CompressionTypePB::ZSTD, &codec).ok());

    std::string sample;
    for (int i = 0; i < 200; i++) {
        sample += R"({"role":"user","parts":[{"type":"text","content":"hello world )";
        sample += std::to_string(i);
        sample += R"("}]})";
    }
    auto cdict_or = compression::ZstdCDict::create(Slice(sample), /*level=*/3);
    ASSERT_TRUE(cdict_or.ok());
    auto ddict_or = compression::ZstdDDict::create(Slice(sample));
    ASSERT_TRUE(ddict_or.ok());

    std::string body;
    for (int i = 0; i < 500; i++) {
        body += R"({"role":"assistant","parts":[{"type":"text","content":"hello world )";
        body += std::to_string(i % 50);
        body += R"("}]})";
    }

    std::string compressed(codec->max_compressed_len(body.size()), '\0');
    Slice cslice(compressed);
    std::vector<Slice> in{Slice(body)};
    ASSERT_TRUE(codec->compress(in, &cslice, /*use_compression_buffer=*/false, body.size(), nullptr, nullptr,
                                cdict_or.value().get())
                        .ok());
    compressed.resize(cslice.size);

    std::string out(body.size(), '\0');
    Slice oslice(out);
    ASSERT_TRUE(codec->decompress(Slice(compressed), &oslice, ddict_or.value().get()).ok());
    ASSERT_EQ(body.size(), oslice.size);
    ASSERT_EQ(body, std::string(oslice.data, oslice.size));
}

// Reset-safety: a dict-bearing borrow must not leak its sticky refCDict to the
// next borrower. Interleave dict/no-dict compresses; the no-dict output must
// always equal a clean no-dict reference (the reset-on-return rule that
// prevents a dictionary leaking from one borrower to the next).
TEST_F(BlockCompressionTest, dict_reset_safety_no_leak_to_next_borrow) {
    const BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(CompressionTypePB::ZSTD, &codec).ok());
    std::string sample(4096, 'x');
    auto cdict_or = compression::ZstdCDict::create(Slice(sample), /*level=*/3);
    ASSERT_TRUE(cdict_or.ok());

    std::string body = generate_str(8000);
    auto nodict_compress = [&](std::string* out) {
        out->resize(codec->max_compressed_len(body.size()));
        Slice o(*out);
        EXPECT_TRUE(codec->compress(body, &o).ok());
        out->resize(o.size);
    };
    std::string ref;
    nodict_compress(&ref);

    for (int i = 0; i < 50; i++) {
        std::string dictc(codec->max_compressed_len(body.size()), '\0');
        Slice dc(dictc);
        std::vector<Slice> in{Slice(body)};
        ASSERT_TRUE(codec->compress(in, &dc, /*use_compression_buffer=*/false, body.size(), nullptr, nullptr,
                                    cdict_or.value().get())
                            .ok());
        std::string again;
        nodict_compress(&again);
        ASSERT_EQ(ref, again) << "no-dict compress diverged after a dict compress at iter " << i;
    }
}

// I5: a no-dict frame (dictID=0) decodes identically whether or not a
// raw-content DDict is referenced. This is what makes mixed dict/no-dict pages
// (raw pages, value-dict pages) in one compression dict column safe on the read path.
TEST_F(BlockCompressionTest, nodict_frame_decodes_under_ddict) {
    const BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(CompressionTypePB::ZSTD, &codec).ok());

    std::string sample(2048, 'q');
    auto ddict_or = compression::ZstdDDict::create(Slice(sample));
    ASSERT_TRUE(ddict_or.ok());

    std::string body = generate_str(5000);
    std::string compressed(codec->max_compressed_len(body.size()), '\0');
    Slice cslice(compressed);
    ASSERT_TRUE(codec->compress(body, &cslice).ok()); // no-dict frame
    compressed.resize(cslice.size);

    std::string with_dict(body.size(), '\0');
    Slice wd(with_dict);
    ASSERT_TRUE(codec->decompress(Slice(compressed), &wd, ddict_or.value().get()).ok());

    std::string without_dict(body.size(), '\0');
    Slice wod(without_dict);
    ASSERT_TRUE(codec->decompress(Slice(compressed), &wod).ok());

    ASSERT_EQ(body, std::string(wd.data, wd.size));
    ASSERT_EQ(std::string(wod.data, wod.size), std::string(wd.data, wd.size));
}

// Contract: the dict overloads fail loudly (NotSupported) on a non-ZSTD codec
// instead of silently producing undecodable bytes.
TEST_F(BlockCompressionTest, dict_overload_not_supported_on_non_zstd) {
    const BlockCompressionCodec* lz4 = nullptr;
    ASSERT_TRUE(get_block_compression_codec(CompressionTypePB::LZ4, &lz4).ok());
    std::string body = generate_str(1000);

    std::string out(lz4->max_compressed_len(body.size()), '\0');
    Slice o(out);
    std::vector<Slice> in{Slice(body)};
    Status s = lz4->compress(in, &o, /*use_compression_buffer=*/false, body.size(), nullptr, nullptr,
                             static_cast<const compression::ZstdCDict*>(nullptr));
    ASSERT_TRUE(s.is_not_supported());

    std::string dummy(body.size(), '\0');
    Slice d(dummy);
    Status s2 = lz4->decompress(Slice(body), &d, static_cast<const compression::ZstdDDict*>(nullptr));
    ASSERT_TRUE(s2.is_not_supported());
}

// The dictionary decompression path uses its own thread-local contexts (kept warm
// so consecutive pages do not re-establish the dictionary session) instead of the
// shared pool. Pin the two properties that could break:
//   1. alternating between SEVERAL dictionaries stays correct (the cache is keyed
//      by dictionary identity, and a stale entry must never be treated as a hit);
//   2. it does not disturb the shared pool -- interleaved no-dict decompression
//      still matches a clean reference.
TEST_F(BlockCompressionTest, dict_ctx_cache_multi_dict_and_pool_isolation) {
    const BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(CompressionTypePB::ZSTD, &codec).ok());

    // Three different dictionaries + matching bodies.
    struct Arm {
        std::string sample, body, compressed;
        std::unique_ptr<compression::ZstdDDict> ddict;
    };
    std::vector<Arm> arms(3);
    for (int i = 0; i < 3; i++) {
        for (int k = 0; k < 300; k++) {
            arms[i].sample += "dict" + std::to_string(i) + " frequent phrase " + std::to_string(k % 40) + " ";
        }
        for (int k = 0; k < 600; k++) {
            arms[i].body += "dict" + std::to_string(i) + " frequent phrase " + std::to_string(k % 40) + " payload ";
        }
        auto cd = compression::ZstdCDict::create(Slice(arms[i].sample), 3);
        ASSERT_TRUE(cd.ok());
        auto dd = compression::ZstdDDict::create(Slice(arms[i].sample));
        ASSERT_TRUE(dd.ok());
        arms[i].ddict = std::move(dd.value());
        arms[i].compressed.resize(codec->max_compressed_len(arms[i].body.size()));
        Slice out(arms[i].compressed);
        std::vector<Slice> in{Slice(arms[i].body)};
        ASSERT_TRUE(codec->compress(in, &out, false, arms[i].body.size(), nullptr, nullptr, cd.value().get()).ok());
        arms[i].compressed.resize(out.size);
    }
    // Every dictionary must have a distinct identity, which is what the cache keys on.
    ASSERT_NE(arms[0].ddict->id(), arms[1].ddict->id());
    ASSERT_NE(arms[1].ddict->id(), arms[2].ddict->id());

    // A clean no-dict reference to compare the interleaved no-dict work against.
    std::string plain = generate_str(4000);
    std::string plain_c(codec->max_compressed_len(plain.size()), '\0');
    Slice pc(plain_c);
    ASSERT_TRUE(codec->compress(plain, &pc).ok());
    plain_c.resize(pc.size);

    // Interleave: dict 0, 1, 2, 0, ... plus no-dict in between. More rounds than
    // the cache has slots, so entries really do get evicted and re-loaded.
    for (int round = 0; round < 25; round++) {
        for (int i = 0; i < 3; i++) {
            std::string got(arms[i].body.size(), '\0');
            Slice g(got);
            ASSERT_TRUE(codec->decompress(Slice(arms[i].compressed), &g, arms[i].ddict.get()).ok())
                    << "round " << round << " dict " << i;
            ASSERT_EQ(arms[i].body, std::string(g.data, g.size)) << "round " << round << " dict " << i;
        }
        std::string pgot(plain.size(), '\0');
        Slice pg(pgot);
        ASSERT_TRUE(codec->decompress(Slice(plain_c), &pg).ok());
        ASSERT_EQ(plain, std::string(pg.data, pg.size)) << "no-dict decode disturbed at round " << round;
    }
}

// The dictionary builders reject bad input instead of trusting it. The two size
// guards matter most: zstd_compression_dict_max_size is an operator-settable mutable
// config, and a value that is non-positive (widening to a huge size_t) or simply
// absurd would otherwise reach std::string::resize on a flush or compaction
// thread -- an allocation failure where the documented behaviour is "give up and
// write without a dictionary".
TEST_F(BlockCompressionTest, dict_builders_reject_bad_input) {
    // Empty bytes are the one input the builders must reject: a zero-length
    // dictionary would otherwise be handed to zstd, and the column would be
    // marked dictionary-ready while every page referenced nothing.
    ASSERT_FALSE(compression::ZstdCDict::create(Slice(), 3).ok());
    ASSERT_FALSE(compression::ZstdDDict::create(Slice()).ok());

    std::string sample = generate_str(64 * 1024);
    ASSERT_TRUE(compression::ZstdCDict::create(Slice(sample), 3).ok());
    ASSERT_TRUE(compression::ZstdDDict::create(Slice(sample)).ok());
}

} // namespace starrocks
