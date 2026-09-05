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

#include <atomic>
#include <iostream>
#include <set>
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

// ===================== per-column ZSTD dictionary: codec tests =====================

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
// (raw pages, value-dict pages) in one dictionary column safe on the read path.
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
// `use_ctx_cache` is not an operator switch -- reads always pass true. It exists so the
// pooled path stays reachable from a test: that path is also where a context-allocation
// failure lands, and it must decode correctly there too. Both values are exercised.
static void run_multi_dict_interleave(bool use_ctx_cache) {
    const BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(CompressionTypePB::ZSTD, &codec).ok());

    // One more dictionary than the cache has slots (kDictDCtxCacheSize == 4), so a
    // full round is guaranteed to evict, whatever the thread's slots held before.
    struct Arm {
        std::string sample, body, compressed;
        std::unique_ptr<compression::ZstdDDict> ddict;
    };
    constexpr int kArms = 5;
    std::vector<Arm> arms(kArms);
    for (int i = 0; i < kArms; i++) {
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
    std::set<uint64_t> ids;
    for (const auto& arm : arms) {
        ASSERT_TRUE(ids.insert(arm.ddict->id()).second);
    }

    // A clean no-dict reference to compare the interleaved no-dict work against.
    std::string plain = generate_str(4000);
    std::string plain_c(codec->max_compressed_len(plain.size()), '\0');
    Slice pc(plain_c);
    ASSERT_TRUE(codec->compress(plain, &pc).ok());
    plain_c.resize(pc.size);

    // Interleave: dict 0..4, then round again, plus no-dict in between. Five
    // identities against four slots means each round evicts and re-loads.
    for (int round = 0; round < 25; round++) {
        for (int i = 0; i < kArms; i++) {
            std::string got(arms[i].body.size(), '\0');
            Slice g(got);
            ASSERT_TRUE(codec->decompress(Slice(arms[i].compressed), &g, arms[i].ddict.get(), use_ctx_cache).ok())
                    << "round " << round << " dict " << i;
            ASSERT_EQ(arms[i].body, std::string(g.data, g.size)) << "round " << round << " dict " << i;
        }
        std::string pgot(plain.size(), '\0');
        Slice pg(pgot);
        ASSERT_TRUE(codec->decompress(Slice(plain_c), &pg).ok());
        ASSERT_EQ(plain, std::string(pg.data, pg.size)) << "no-dict decode disturbed at round " << round;
    }
}

// Run on a fresh thread: the cache is thread-local, so on a thread another test has
// already used, "evicted" could mean "replaced an entry that test left behind" and the
// coverage would depend on test order.
static void run_multi_dict_interleave_on_fresh_thread(bool use_ctx_cache) {
    std::thread t([use_ctx_cache] { run_multi_dict_interleave(use_ctx_cache); });
    t.join();
}

TEST_F(BlockCompressionTest, dict_ctx_cache_multi_dict_and_pool_isolation) {
    run_multi_dict_interleave_on_fresh_thread(/*use_ctx_cache=*/true);
}

// Same, with the context cache turned off: every page then goes through the shared
// pool, which must load the dictionary per call and drop it again on return.
TEST_F(BlockCompressionTest, dict_pooled_ctx_multi_dict_and_pool_isolation) {
    run_multi_dict_interleave_on_fresh_thread(/*use_ctx_cache=*/false);
}

// A page that fails to decompress leaves the context in an undefined state, so the
// cached-context path throws that context away. What must survive is the thread:
// the next page decoded on it has to be correct, and this has to hold however many
// times it happens (a dropped context is recreated, never reused).
TEST_F(BlockCompressionTest, dict_decompress_failure_does_not_poison_the_thread) {
    const BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(CompressionTypePB::ZSTD, &codec).ok());

    std::string sample;
    for (int k = 0; k < 300; k++) {
        sample += "recurring phrase " + std::to_string(k % 40) + " ";
    }
    std::string body;
    for (int k = 0; k < 600; k++) {
        body += "recurring phrase " + std::to_string(k % 40) + " payload ";
    }
    auto cd = compression::ZstdCDict::create(Slice(sample), 3);
    ASSERT_TRUE(cd.ok());
    auto dd = compression::ZstdDDict::create(Slice(sample));
    ASSERT_TRUE(dd.ok());

    std::string compressed(codec->max_compressed_len(body.size()), '\0');
    Slice out(compressed);
    std::vector<Slice> in{Slice(body)};
    ASSERT_TRUE(codec->compress(in, &out, false, body.size(), nullptr, nullptr, cd.value().get()).ok());
    compressed.resize(out.size);

    for (int round = 0; round < 5; round++) {
        // Garbage in: must be reported, not crash, and must not be mistaken for data.
        std::string garbage = "this is not a zstd frame at all";
        std::string sink(body.size(), '\0');
        Slice s1(sink);
        ASSERT_FALSE(codec->decompress(Slice(garbage), &s1, dd.value().get()).ok()) << "round " << round;

        // Truncating a real frame fails inside the decoder rather than at the header.
        std::string truncated = compressed.substr(0, compressed.size() / 2);
        std::string sink2(body.size(), '\0');
        Slice s2(sink2);
        ASSERT_FALSE(codec->decompress(Slice(truncated), &s2, dd.value().get()).ok()) << "round " << round;

        // The thread must still decode correctly right after both failures.
        std::string got(body.size(), '\0');
        Slice g(got);
        ASSERT_TRUE(codec->decompress(Slice(compressed), &g, dd.value().get()).ok()) << "round " << round;
        ASSERT_EQ(body, std::string(g.data, g.size)) << "round " << round;
    }
}

// The segment metacache sizes itself from what the readers report, and a DDict is
// held for as long as the reader that built it. So its reported size has to be
// real: roughly the dictionary it copied, and it has to grow with it.
TEST_F(BlockCompressionTest, ddict_reports_its_memory) {
    size_t last = 0;
    for (size_t dict_size : {4096UL, 64UL * 1024, 256UL * 1024}) {
        std::string sample = generate_str(dict_size);
        auto dd = compression::ZstdDDict::create(Slice(sample));
        ASSERT_TRUE(dd.ok());
        const size_t reported = dd.value()->mem_usage();
        EXPECT_GE(reported, dict_size) << "dict_size " << dict_size;
        EXPECT_LT(reported, dict_size * 2 + 128 * 1024) << "dict_size " << dict_size;
        EXPECT_GT(reported, last) << "dict_size " << dict_size;
        last = reported;
    }
}

TEST_F(BlockCompressionTest, dict_builders_reject_bad_input) {
    // Empty bytes are the one input the builders must reject: a zero-length
    // dictionary would otherwise be handed to zstd, and a column would be marked
    // dictionary-ready while every page referenced nothing.
    ASSERT_FALSE(compression::ZstdCDict::create(Slice(), 3).ok());
    ASSERT_FALSE(compression::ZstdDDict::create(Slice()).ok());

    std::string sample = generate_str(64 * 1024);
    ASSERT_TRUE(compression::ZstdCDict::create(Slice(sample), 3).ok());
    ASSERT_TRUE(compression::ZstdDDict::create(Slice(sample)).ok());
}

// The cached contexts are ~94 KB each and live until their thread exits, so the bytes
// have to be both visible and returned. Pins three things: the counter rises when a
// thread first decodes a dictionary page, the installed allocation scope is entered and
// left in pairs (that is what moves the charge off whatever query created the context),
// and everything is given back when the thread goes away.
namespace {
std::atomic<int> g_scope_enters{0};
std::atomic<int> g_scope_leaves{0};
void count_enter() {
    g_scope_enters.fetch_add(1);
}
void count_leave() {
    g_scope_leaves.fetch_add(1);
}
} // namespace

TEST_F(BlockCompressionTest, dict_ctx_cache_memory_is_accounted_and_returned) {
    const BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(CompressionTypePB::ZSTD, &codec).ok());

    std::string sample;
    for (int k = 0; k < 400; k++) {
        sample += "recurring phrase " + std::to_string(k % 40) + " ";
    }
    std::string body;
    for (int k = 0; k < 800; k++) {
        body += "recurring phrase " + std::to_string(k % 40) + " payload ";
    }
    auto cd = compression::ZstdCDict::create(Slice(sample), 3);
    ASSERT_TRUE(cd.ok());
    auto dd = compression::ZstdDDict::create(Slice(sample));
    ASSERT_TRUE(dd.ok());
    std::string compressed(codec->max_compressed_len(body.size()), '\0');
    Slice out(compressed);
    std::vector<Slice> in{Slice(body)};
    ASSERT_TRUE(codec->compress(in, &out, false, body.size(), nullptr, nullptr, cd.value().get()).ok());
    compressed.resize(out.size);

    const size_t before = dict_dctx_cache_memory_bytes();
    g_scope_enters.store(0);
    g_scope_leaves.store(0);
    set_dict_dctx_alloc_scope(&count_enter, &count_leave);

    size_t peak = 0;
    // a fresh thread, so the accounting is this thread's and not something an earlier
    // test left cached
    std::thread t([&] {
        for (int i = 0; i < 4; i++) {
            std::string got(body.size(), '\0');
            Slice g(got);
            ASSERT_TRUE(codec->decompress(Slice(compressed), &g, dd.value().get()).ok());
            ASSERT_EQ(body, std::string(g.data, g.size));
        }
        peak = dict_dctx_cache_memory_bytes();
    });
    t.join();

    set_dict_dctx_alloc_scope(nullptr, nullptr);

    // one context for this dictionary, held while the thread ran
    ASSERT_GT(peak, before) << "the context was never accounted";
    ASSERT_GE(peak - before, 1024u) << "a ZSTD_DCtx is tens of KB, not a handful of bytes";
    // and handed back when the thread exited
    ASSERT_EQ(before, dict_dctx_cache_memory_bytes()) << "the context was not returned at thread exit";
    // Exactly two scopes, and the count is what pins them: one around ZSTD_createDCtx for
    // the single dictionary this thread used, one around ZSTD_freeDCtx when the thread's
    // cache was destroyed at exit (the other three slots are empty and return before the
    // scope). "Entered as many times as left" is not enough -- deleting the scope from
    // either site alone still leaves it paired, at 1/1.
    ASSERT_EQ(2, g_scope_enters.load()) << "the allocation scope did not wrap both the create and the free";
    ASSERT_EQ(2, g_scope_leaves.load());
}

// Everything above proves the cache decodes correctly. None of it proves the cache does
// its job, or stays inside the bound its memory argument rests on -- both are invisible
// to an output comparison, and both are exactly what a regression would take away. The
// byte counter makes them observable: one context is ~94 KB, so counting bytes counts
// contexts.
TEST_F(BlockCompressionTest, dict_ctx_cache_hits_and_stays_bounded) {
    const BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(CompressionTypePB::ZSTD, &codec).ok());

    // eight distinct dictionaries, each with a body compressed against it
    constexpr int kDicts = 8;
    struct Arm {
        std::string sample, body, compressed;
        std::unique_ptr<compression::ZstdDDict> ddict;
    };
    std::vector<Arm> arms(kDicts);
    for (int i = 0; i < kDicts; i++) {
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

    auto decode = [&](int i) {
        std::string got(arms[i].body.size(), '\0');
        Slice g(got);
        ASSERT_TRUE(codec->decompress(Slice(arms[i].compressed), &g, arms[i].ddict.get()).ok());
        ASSERT_EQ(arms[i].body, std::string(g.data, g.size));
    };

    const size_t before = dict_dctx_cache_memory_bytes();
    size_t after_first_page = 0;
    size_t after_one_dict_many_pages = 0;
    size_t after_cycling_all = 0;
    size_t after_failure = 0;

    // a fresh thread, so the numbers are this thread's contexts and nobody else's
    std::thread t([&] {
        // 1. THE POINT OF THE CACHE: the first page of a dictionary establishes the
        //    session (one context); the next 49 pages of the SAME dictionary must add
        //    nothing. If the hit ever degrades, every page re-loads the dictionary --
        //    correct output, ~30% slower reads, and nothing else would notice. The
        //    single-context size is measured from the first page alone, so the
        //    comparison below is between two independent measurements.
        decode(0);
        after_first_page = dict_dctx_cache_memory_bytes();
        for (int p = 1; p < 50; p++) decode(0);
        after_one_dict_many_pages = dict_dctx_cache_memory_bytes();

        // 2. THE BOUND: cycling through more dictionaries than there are slots must not
        //    grow without limit. kDictDCtxCacheSize is 4 in the .cpp; the per-thread cost
        //    quoted in the design rests on this staying true.
        for (int round = 0; round < 5; round++) {
            for (int i = 0; i < kDicts; i++) decode(i);
        }
        after_cycling_all = dict_dctx_cache_memory_bytes();

        // 3. DISCARD: a page that fails to decode must cost that context its slot, not
        //    be handed out again. Output equality cannot see this; the counter can.
        //    Truncate rather than flip a byte: a short frame is always an error, while a
        //    flipped byte is only usually one -- zstd sets no frame checksum here and a
        //    raw-content dictionary carries no dictID, so roughly one flip position in six
        //    decodes "successfully" into wrong bytes, and which ones depends on the level
        //    and the data.
        std::string truncated = arms[0].compressed.substr(0, arms[0].compressed.size() / 2);
        std::string got(arms[0].body.size(), '\0');
        Slice g(got);
        ASSERT_FALSE(codec->decompress(Slice(truncated), &g, arms[0].ddict.get()).ok()) << "a half frame decoded";
        after_failure = dict_dctx_cache_memory_bytes();
    });
    t.join();

    const size_t one_ctx = after_first_page - before;
    ASSERT_GT(one_ctx, 1024u) << "a ZSTD_DCtx is tens of KB";
    // 49 more pages of the same dictionary added no context: the cache hit
    ASSERT_EQ(after_first_page, after_one_dict_many_pages)
            << "pages after the first re-allocated a context: the cache is not hitting";
    // 8 dictionaries over 5 rounds, still at most the 4 slots
    ASSERT_LE(after_cycling_all - before, 4 * one_ctx) << "the per-thread context set grew past kDictDCtxCacheSize";
    // the failed page cost its context its slot
    ASSERT_LT(after_failure, after_cycling_all) << "a context that failed mid-decompression was kept";
    // and nothing survives the thread
    ASSERT_EQ(before, dict_dctx_cache_memory_bytes());
}

// The whole reason this change exists is that an old BE must be able to read segments a
// newer one wrote. The other direction is the one that has to be safe by construction: a
// build WITHOUT this change meets a page compressed against a dictionary, decodes it with
// no dictionary at all, and must FAIL rather than hand back plausible-looking bytes. The
// page checksum covers the compressed body, so it passes either way -- the only thing
// standing between a downgraded cluster and silent wrong answers is that zstd refuses.
// It does refuse -- measured over 232 pages across two real corpora -- but note zstd does
// not *promise* to: no frame checksum is set and a raw-content dictionary carries no
// dictID. What makes this case robust rather than lucky is that the frame's backreferences
// point into a window the decoder does not have, not a checksum catching it.
TEST_F(BlockCompressionTest, dict_page_cannot_be_decoded_without_its_dictionary) {
    const BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(CompressionTypePB::ZSTD, &codec).ok());

    std::string sample;
    for (int k = 0; k < 400; k++) {
        sample += "recurring phrase " + std::to_string(k % 40) + " ";
    }
    auto cd = compression::ZstdCDict::create(Slice(sample), 3);
    ASSERT_TRUE(cd.ok());

    int refused = 0;
    for (int p = 0; p < 32; p++) {
        std::string body;
        for (int k = 0; k < 400; k++) {
            body += "recurring phrase " + std::to_string((k + p) % 40) + " payload " + std::to_string(p) + " ";
        }
        std::string compressed(codec->max_compressed_len(body.size()), '\0');
        Slice out(compressed);
        std::vector<Slice> in{Slice(body)};
        ASSERT_TRUE(codec->compress(in, &out, false, body.size(), nullptr, nullptr, cd.value().get()).ok());
        compressed.resize(out.size);

        // exactly what an old reader does: the two-argument overload, no dictionary
        std::string got(body.size(), '\0');
        Slice g(got);
        Status st = codec->decompress(Slice(compressed), &g);
        if (!st.ok()) {
            refused++;
        } else {
            // the unacceptable outcome: it "worked" and produced something else
            ASSERT_EQ(body, std::string(g.data, g.size))
                    << "page " << p << " decoded without its dictionary and returned WRONG bytes";
        }
    }
    ASSERT_EQ(32, refused) << "a dictionary-compressed page decoded without the dictionary";
}

TEST_F(BlockCompressionTest, gzip_decompression_empty_and_small) {
    const BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(CompressionTypePB::GZIP, &codec).ok());
    ASSERT_NE(nullptr, codec);

    // 1. Empty slice
    Slice empty_in;
    std::string empty_out;
    Slice empty_out_slice(empty_out);
    ASSERT_TRUE(codec->decompress(empty_in, &empty_out_slice).ok());
    ASSERT_EQ(0, empty_out_slice.size);

    // 2. Small string roundtrip
    std::string small_orig = "hello world gzip block compression";
    std::string compressed;
    compressed.resize(codec->max_compressed_len(small_orig.size()));
    Slice compressed_slice(compressed);
    ASSERT_TRUE(codec->compress(small_orig, &compressed_slice).ok());
    compressed.resize(compressed_slice.size);

    std::string decompressed;
    decompressed.resize(small_orig.size());
    Slice decomp_slice(decompressed);
    ASSERT_TRUE(codec->decompress(Slice(compressed), &decomp_slice).ok());
    ASSERT_EQ(small_orig, decompressed);
}

TEST_F(BlockCompressionTest, gzip_decompression_large_payload) {
    const BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(CompressionTypePB::GZIP, &codec).ok());
    ASSERT_NE(nullptr, codec);

    // Multi-megabyte repetitive and non-repetitive payload
    std::string orig;
    orig.reserve(2 * 1024 * 1024);
    for (int i = 0; i < 50000; ++i) {
        orig += "StarRocks GZIP decompression libdeflate accelerated payload record #" + std::to_string(i) + "\n";
    }

    std::string compressed;
    compressed.resize(codec->max_compressed_len(orig.size()));
    Slice compressed_slice(compressed);
    ASSERT_TRUE(codec->compress(orig, &compressed_slice).ok());
    compressed.resize(compressed_slice.size);

    std::string decompressed;
    decompressed.resize(orig.size());
    Slice decomp_slice(decompressed);
    ASSERT_TRUE(codec->decompress(Slice(compressed), &decomp_slice).ok());
    ASSERT_EQ(orig, decompressed);
}

TEST_F(BlockCompressionTest, gzip_decompression_corrupted_payload_returns_error) {
    const BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(CompressionTypePB::GZIP, &codec).ok());
    ASSERT_NE(nullptr, codec);

    std::string orig = "Data that will be corrupted after compression";
    std::string compressed;
    compressed.resize(codec->max_compressed_len(orig.size()));
    Slice compressed_slice(compressed);
    ASSERT_TRUE(codec->compress(orig, &compressed_slice).ok());
    compressed.resize(compressed_slice.size);

    // Corrupt payload bytes in the middle of the gzip stream
    if (compressed.size() > 10) {
        compressed[compressed.size() / 2] ^= 0xFF;
    }

    std::string decompressed;
    decompressed.resize(orig.size());
    Slice decomp_slice(decompressed);
    Status st = codec->decompress(Slice(compressed), &decomp_slice);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_invalid_argument()) << "Expected InvalidArgument, got: " << st.to_string();
}

TEST_F(BlockCompressionTest, benchmark_gzip_decompression_throughput) {
    const BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(CompressionTypePB::GZIP, &codec).ok());
    ASSERT_NE(nullptr, codec);

    // Create 64 KB block of realistic semi-compressible text
    std::string sample;
    sample.reserve(64 * 1024);
    for (int i = 0; sample.size() < 64 * 1024; ++i) {
        sample += "StarRocks column record index=" + std::to_string(i) +
                  " timestamp=" + std::to_string(1700000000 + i) + " value=" + std::to_string(i * 1.5) +
                  " tag=analytics_ingestion_metric\n";
    }
    sample.resize(64 * 1024);

    std::string compressed;
    compressed.resize(codec->max_compressed_len(sample.size()));
    Slice comp_slice(compressed);
    ASSERT_TRUE(codec->compress(sample, &comp_slice).ok());
    compressed.resize(comp_slice.size);

    std::string decompressed;
    decompressed.resize(sample.size());
    Slice decomp_slice(decompressed);

    // Warm-up
    for (int i = 0; i < 50; ++i) {
        decomp_slice.size = decompressed.size();
        ASSERT_TRUE(codec->decompress(Slice(compressed), &decomp_slice).ok());
    }

    // Benchmark 2000 iterations (approx 128 MB decompressed data)
    const int iterations = 2000;
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; ++i) {
        decomp_slice.size = decompressed.size();
        ASSERT_TRUE(codec->decompress(Slice(compressed), &decomp_slice).ok());
    }
    auto end = std::chrono::high_resolution_clock::now();
    double elapsed_sec = std::chrono::duration<double>(end - start).count();
    double total_mb = (sample.size() * iterations) / (1024.0 * 1024.0);
    double throughput_mb_s = total_mb / elapsed_sec;

    std::cout << "========================================================\n"
              << "[BENCHMARK RESULTS] GZIP Decompression Throughput\n"
              << "Block Size: " << sample.size() / 1024 << " KB | Compressed: " << compressed.size() << " bytes\n"
              << "Total Decompressed: " << total_mb << " MB in " << elapsed_sec << " s\n"
              << "Decompression Throughput: " << throughput_mb_s << " MB/s\n"
              << "========================================================\n";
}

} // namespace starrocks
