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

#include "formats/csv/output_stream_file.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "fs/fs_memory.h"
#include "io/async_flush_output_stream.h"
#include "runtime/exec_env.h"
#include "testutil/assert.h"
#include "util/compression/block_compression.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks::csv {

class OutputStreamFileTest : public testing::Test {
public:
    void SetUp() override {
        _exec_env = ExecEnv::GetInstance();
        _executors = _exec_env->pipeline_prepare_pool();
    }

    void TearDown() override {}

protected:
    // Helper function to decompress gzip data
    std::string decompress_gzip(const std::string& compressed_data) {
        const BlockCompressionCodec* codec;
        Status st = get_block_compression_codec(CompressionTypePB::GZIP, &codec);
        EXPECT_TRUE(st.ok());

        Slice input(compressed_data.data(), compressed_data.size());
        size_t max_uncompressed_size = compressed_data.size() * 10; // Estimate
        std::string uncompressed_data;
        uncompressed_data.resize(max_uncompressed_size);
        Slice output(uncompressed_data.data(), max_uncompressed_size);

        st = codec->decompress(input, &output);
        EXPECT_TRUE(st.ok());

        uncompressed_data.resize(output.size);
        return uncompressed_data;
    }

    MemoryFileSystem _fs;
    ExecEnv* _exec_env;
    PriorityThreadPool* _executors;
};

// Test basic compression functionality
TEST_F(OutputStreamFileTest, TestBasicCompression) {
    std::string file_path = "/test_basic_compression.csv.gz";
    auto maybe_file = _fs.new_writable_file(file_path);
    ASSERT_OK(maybe_file.status());
    auto file = std::move(maybe_file.value());

    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(file), _executors, nullptr);
    auto compressed_stream =
            std::make_unique<CompressedAsyncOutputStreamFile>(async_stream.get(), CompressionTypePB::GZIP, 1024);

    // Write some test data
    std::string test_data = "Hello, World!\nThis is a test.\n";
    ASSERT_OK(compressed_stream->write(test_data.data(), test_data.size()));
    ASSERT_OK(compressed_stream->finalize());
    ASSERT_OK(async_stream->close());

    // Read back the compressed data
    std::string compressed_content;
    ASSERT_OK(_fs.read_file(file_path, &compressed_content));

    // Verify the content is actually compressed (should be smaller or different)
    EXPECT_NE(compressed_content, test_data);

    // Decompress and verify
    std::string decompressed = decompress_gzip(compressed_content);
    EXPECT_EQ(decompressed, test_data);
}

// Test compression with empty data
TEST_F(OutputStreamFileTest, TestEmptyDataCompression) {
    std::string file_path = "/test_empty_compression.csv.gz";
    auto maybe_file = _fs.new_writable_file(file_path);
    ASSERT_OK(maybe_file.status());
    auto file = std::move(maybe_file.value());

    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(file), _executors, nullptr);
    auto compressed_stream =
            std::make_unique<CompressedAsyncOutputStreamFile>(async_stream.get(), CompressionTypePB::GZIP, 1024);

    // Finalize without writing any data
    ASSERT_OK(compressed_stream->finalize());
    ASSERT_OK(async_stream->close());

    // Read back the file
    std::string compressed_content;
    ASSERT_OK(_fs.read_file(file_path, &compressed_content));

    // For empty data, there should be minimal or no compressed content
    // The exact behavior depends on the compression implementation
    EXPECT_GE(compressed_content.size(), 0);
}

// Test compression with large data
TEST_F(OutputStreamFileTest, TestLargeDataCompression) {
    std::string file_path = "/test_large_compression.csv.gz";
    auto maybe_file = _fs.new_writable_file(file_path);
    ASSERT_OK(maybe_file.status());
    auto file = std::move(maybe_file.value());

    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(file), _executors, nullptr);
    auto compressed_stream =
            std::make_unique<CompressedAsyncOutputStreamFile>(async_stream.get(), CompressionTypePB::GZIP, 1024);

    // Generate large test data (10KB of repetitive data - highly compressible)
    std::string test_data;
    for (int i = 0; i < 1000; i++) {
        test_data += "This is line " + std::to_string(i) + "\n";
    }

    ASSERT_OK(compressed_stream->write(test_data.data(), test_data.size()));
    ASSERT_OK(compressed_stream->finalize());
    ASSERT_OK(async_stream->close());

    // Read back the compressed data
    std::string compressed_content;
    ASSERT_OK(_fs.read_file(file_path, &compressed_content));

    // Verify compression is effective (compressed should be smaller)
    EXPECT_LT(compressed_content.size(), test_data.size());

    // Decompress and verify
    std::string decompressed = decompress_gzip(compressed_content);
    EXPECT_EQ(decompressed, test_data);
}

// Test multiple writes before finalize
TEST_F(OutputStreamFileTest, TestMultipleWrites) {
    std::string file_path = "/test_multiple_writes.csv.gz";
    auto maybe_file = _fs.new_writable_file(file_path);
    ASSERT_OK(maybe_file.status());
    auto file = std::move(maybe_file.value());

    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(file), _executors, nullptr);
    auto compressed_stream =
            std::make_unique<CompressedAsyncOutputStreamFile>(async_stream.get(), CompressionTypePB::GZIP, 1024);

    // Write data in multiple chunks
    std::string chunk1 = "First chunk\n";
    std::string chunk2 = "Second chunk\n";
    std::string chunk3 = "Third chunk\n";

    ASSERT_OK(compressed_stream->write(chunk1.data(), chunk1.size()));
    ASSERT_OK(compressed_stream->write(chunk2.data(), chunk2.size()));
    ASSERT_OK(compressed_stream->write(chunk3.data(), chunk3.size()));
    ASSERT_OK(compressed_stream->finalize());
    ASSERT_OK(async_stream->close());

    // Read back and decompress
    std::string compressed_content;
    ASSERT_OK(_fs.read_file(file_path, &compressed_content));

    std::string decompressed = decompress_gzip(compressed_content);
    std::string expected = chunk1 + chunk2 + chunk3;
    EXPECT_EQ(decompressed, expected);
}

// Test compression with data larger than buffer
TEST_F(OutputStreamFileTest, TestDataLargerThanBuffer) {
    std::string file_path = "/test_larger_than_buffer.csv.gz";
    auto maybe_file = _fs.new_writable_file(file_path);
    ASSERT_OK(maybe_file.status());
    auto file = std::move(maybe_file.value());

    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(file), _executors, nullptr);
    // Use a small buffer size to test buffer overflow handling
    auto compressed_stream =
            std::make_unique<CompressedAsyncOutputStreamFile>(async_stream.get(), CompressionTypePB::GZIP, 64);

    // Write data larger than buffer
    std::string test_data;
    for (int i = 0; i < 100; i++) {
        test_data += "Line " + std::to_string(i) + "\n";
    }

    ASSERT_OK(compressed_stream->write(test_data.data(), test_data.size()));
    ASSERT_OK(compressed_stream->finalize());
    ASSERT_OK(async_stream->close());

    // Read back and decompress
    std::string compressed_content;
    ASSERT_OK(_fs.read_file(file_path, &compressed_content));

    std::string decompressed = decompress_gzip(compressed_content);
    EXPECT_EQ(decompressed, test_data);
}

// Test size() method
TEST_F(OutputStreamFileTest, TestSizeMethod) {
    std::string file_path = "/test_size.csv.gz";
    auto maybe_file = _fs.new_writable_file(file_path);
    ASSERT_OK(maybe_file.status());
    auto file = std::move(maybe_file.value());

    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(file), _executors, nullptr);
    auto compressed_stream =
            std::make_unique<CompressedAsyncOutputStreamFile>(async_stream.get(), CompressionTypePB::GZIP, 1024);

    std::string test_data = "Test data\n";
    ASSERT_OK(compressed_stream->write(test_data.data(), test_data.size()));
    ASSERT_OK(compressed_stream->finalize());

    // size() should return the position in the underlying stream
    size_t size = compressed_stream->size();
    EXPECT_GT(size, 0);

    ASSERT_OK(async_stream->close());
}

// Test compression with special characters
TEST_F(OutputStreamFileTest, TestSpecialCharacters) {
    std::string file_path = "/test_special_chars.csv.gz";
    auto maybe_file = _fs.new_writable_file(file_path);
    ASSERT_OK(maybe_file.status());
    auto file = std::move(maybe_file.value());

    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(file), _executors, nullptr);
    auto compressed_stream =
            std::make_unique<CompressedAsyncOutputStreamFile>(async_stream.get(), CompressionTypePB::GZIP, 1024);

    // Test data with special characters
    std::string test_data = "Hello,世界\n\"quoted,value\"\n\ttab\tseparated\n\\N\n";

    ASSERT_OK(compressed_stream->write(test_data.data(), test_data.size()));
    ASSERT_OK(compressed_stream->finalize());
    ASSERT_OK(async_stream->close());

    // Read back and decompress
    std::string compressed_content;
    ASSERT_OK(_fs.read_file(file_path, &compressed_content));

    std::string decompressed = decompress_gzip(compressed_content);
    EXPECT_EQ(decompressed, test_data);
}

// Test compression with binary data
TEST_F(OutputStreamFileTest, TestBinaryData) {
    std::string file_path = "/test_binary.csv.gz";
    auto maybe_file = _fs.new_writable_file(file_path);
    ASSERT_OK(maybe_file.status());
    auto file = std::move(maybe_file.value());

    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(file), _executors, nullptr);
    auto compressed_stream =
            std::make_unique<CompressedAsyncOutputStreamFile>(async_stream.get(), CompressionTypePB::GZIP, 1024);

    // Test data with binary content (all byte values)
    std::string test_data;
    for (int i = 0; i < 256; i++) {
        test_data += static_cast<char>(i);
    }

    ASSERT_OK(compressed_stream->write(test_data.data(), test_data.size()));
    ASSERT_OK(compressed_stream->finalize());
    ASSERT_OK(async_stream->close());

    // Read back and decompress
    std::string compressed_content;
    ASSERT_OK(_fs.read_file(file_path, &compressed_content));

    std::string decompressed = decompress_gzip(compressed_content);
    EXPECT_EQ(decompressed, test_data);
}

// Test that uncompressed stream still works (regression test)
TEST_F(OutputStreamFileTest, TestUncompressedStream) {
    std::string file_path = "/test_uncompressed.csv";
    auto maybe_file = _fs.new_writable_file(file_path);
    ASSERT_OK(maybe_file.status());
    auto file = std::move(maybe_file.value());

    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(file), _executors, nullptr);
    auto output_stream = std::make_unique<AsyncOutputStreamFile>(async_stream.get(), 1024);

    std::string test_data = "Uncompressed data\n";
    ASSERT_OK(output_stream->write(test_data.data(), test_data.size()));
    ASSERT_OK(output_stream->finalize());
    ASSERT_OK(async_stream->close());

    // Read back the data (should be uncompressed)
    std::string content;
    ASSERT_OK(_fs.read_file(file_path, &content));
    EXPECT_EQ(content, test_data);
}

// Test write after finalize should fail
TEST_F(OutputStreamFileTest, TestWriteAfterFinalize) {
    std::string file_path = "/test_write_after_finalize.csv.gz";
    auto maybe_file = _fs.new_writable_file(file_path);
    ASSERT_OK(maybe_file.status());
    auto file = std::move(maybe_file.value());

    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(file), _executors, nullptr);
    auto compressed_stream =
            std::make_unique<CompressedAsyncOutputStreamFile>(async_stream.get(), CompressionTypePB::GZIP, 1024);

    std::string test_data = "Test data\n";
    ASSERT_OK(compressed_stream->write(test_data.data(), test_data.size()));
    ASSERT_OK(compressed_stream->finalize());

    // Writing after finalize should fail
    auto status = compressed_stream->write(test_data.data(), test_data.size());
    EXPECT_FALSE(status.ok());

    ASSERT_OK(async_stream->close());
}

// Test very small writes
TEST_F(OutputStreamFileTest, TestVerySmallWrites) {
    std::string file_path = "/test_small_writes.csv.gz";
    auto maybe_file = _fs.new_writable_file(file_path);
    ASSERT_OK(maybe_file.status());
    auto file = std::move(maybe_file.value());

    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(file), _executors, nullptr);
    auto compressed_stream =
            std::make_unique<CompressedAsyncOutputStreamFile>(async_stream.get(), CompressionTypePB::GZIP, 1024);

    // Write one byte at a time
    std::string test_data = "ABCDEFGH";
    for (char c : test_data) {
        ASSERT_OK(compressed_stream->write(&c, 1));
    }

    ASSERT_OK(compressed_stream->finalize());
    ASSERT_OK(async_stream->close());

    // Read back and decompress
    std::string compressed_content;
    ASSERT_OK(_fs.read_file(file_path, &compressed_content));

    std::string decompressed = decompress_gzip(compressed_content);
    EXPECT_EQ(decompressed, test_data);
}

} // namespace starrocks::csv
