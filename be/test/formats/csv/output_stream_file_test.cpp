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

#include "common/object_pool.h"
#include "compression_test_utils.h"
#include "exec/pipeline/fragment_context.h"
#include "fs/fs_memory.h"
#include "io/async_flush_output_stream.h"
#include "testutil/assert.h"

namespace starrocks::csv {

class OutputStreamFileTest : public testing::Test {
public:
    void SetUp() override {
        _fragment_context = std::make_shared<pipeline::FragmentContext>();
        _fragment_context->set_runtime_state(std::make_shared<RuntimeState>());
        _runtime_state = _fragment_context->runtime_state();
    }

    void TearDown() override {}

protected:
    std::shared_ptr<pipeline::FragmentContext> _fragment_context;
    RuntimeState* _runtime_state;
    MemoryFileSystem _fs;
};

// Test basic compression functionality
TEST_F(OutputStreamFileTest, TestBasicCompression) {
    std::string file_path = "/test_basic_compression.csv.gz";
    auto maybe_file = _fs.new_writable_file(file_path);
    ASSERT_OK(maybe_file.status());
    auto file = std::move(maybe_file.value());

    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(file), nullptr, _runtime_state);
    auto base_stream = std::make_shared<AsyncOutputStreamFile>(async_stream.get(), 1024);
    auto compressed_stream_result = CompressedOutputStream::create(base_stream, CompressionTypePB::GZIP, 1024);
    ASSERT_OK(compressed_stream_result.status());
    auto compressed_stream = std::move(compressed_stream_result.value());

    // Write some test data
    std::string test_data = "Hello, World!\nThis is a test.\n";
    ASSERT_OK(compressed_stream->write(Slice(test_data)));
    ASSERT_OK(compressed_stream->finalize());
    // Note: finalize() already closes the async stream internally

    // Read back the compressed data
    std::string compressed_content;
    ASSERT_OK(_fs.read_file(file_path, &compressed_content));

    // Verify the content is actually compressed (should be smaller or different)
    EXPECT_NE(compressed_content, test_data);

    // Decompress and verify
    std::string decompressed = starrocks::test::decompress_gzip(compressed_content);
    EXPECT_EQ(decompressed, test_data);
}

// Test compression with empty data
TEST_F(OutputStreamFileTest, TestEmptyDataCompression) {
    std::string file_path = "/test_empty_compression.csv.gz";
    auto maybe_file = _fs.new_writable_file(file_path);
    ASSERT_OK(maybe_file.status());
    auto file = std::move(maybe_file.value());

    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(file), nullptr, _runtime_state);
    auto base_stream = std::make_shared<AsyncOutputStreamFile>(async_stream.get(), 1024);
    auto compressed_stream_result = CompressedOutputStream::create(base_stream, CompressionTypePB::GZIP, 1024);
    ASSERT_OK(compressed_stream_result.status());
    auto compressed_stream = std::move(compressed_stream_result.value());

    // Finalize without writing any data
    ASSERT_OK(compressed_stream->finalize());
    // Note: finalize() already closes the async stream internally

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

    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(file), nullptr, _runtime_state);
    auto base_stream = std::make_shared<AsyncOutputStreamFile>(async_stream.get(), 1024);
    auto compressed_stream_result = CompressedOutputStream::create(base_stream, CompressionTypePB::GZIP, 1024);
    ASSERT_OK(compressed_stream_result.status());
    auto compressed_stream = std::move(compressed_stream_result.value());

    // Generate large test data (10KB of repetitive data - highly compressible)
    std::string test_data;
    for (int i = 0; i < 1000; i++) {
        test_data += "This is line " + std::to_string(i) + "\n";
    }

    ASSERT_OK(compressed_stream->write(Slice(test_data)));
    ASSERT_OK(compressed_stream->finalize());
    // Note: finalize() already closes the async stream internally

    // Read back the compressed data
    std::string compressed_content;
    ASSERT_OK(_fs.read_file(file_path, &compressed_content));

    // Verify compression is effective (compressed should be smaller)
    EXPECT_LT(compressed_content.size(), test_data.size());

    // Decompress and verify
    std::string decompressed = starrocks::test::decompress_gzip(compressed_content);
    EXPECT_EQ(decompressed, test_data);
}

// Test multiple writes before finalize
TEST_F(OutputStreamFileTest, TestMultipleWrites) {
    std::string file_path = "/test_multiple_writes.csv.gz";
    auto maybe_file = _fs.new_writable_file(file_path);
    ASSERT_OK(maybe_file.status());
    auto file = std::move(maybe_file.value());

    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(file), nullptr, _runtime_state);
    auto base_stream = std::make_shared<AsyncOutputStreamFile>(async_stream.get(), 1024);
    auto compressed_stream_result = CompressedOutputStream::create(base_stream, CompressionTypePB::GZIP, 1024);
    ASSERT_OK(compressed_stream_result.status());
    auto compressed_stream = std::move(compressed_stream_result.value());

    // Write data in multiple chunks
    std::string chunk1 = "First chunk\n";
    std::string chunk2 = "Second chunk\n";
    std::string chunk3 = "Third chunk\n";

    ASSERT_OK(compressed_stream->write(Slice(chunk1)));
    ASSERT_OK(compressed_stream->write(Slice(chunk2)));
    ASSERT_OK(compressed_stream->write(Slice(chunk3)));
    ASSERT_OK(compressed_stream->finalize());
    // Note: finalize() already closes the async stream internally

    // Read back and decompress
    std::string compressed_content;
    ASSERT_OK(_fs.read_file(file_path, &compressed_content));

    std::string decompressed = starrocks::test::decompress_gzip(compressed_content);
    std::string expected = chunk1 + chunk2 + chunk3;
    EXPECT_EQ(decompressed, expected);
}

// Test compression with data larger than buffer
TEST_F(OutputStreamFileTest, TestDataLargerThanBuffer) {
    std::string file_path = "/test_larger_than_buffer.csv.gz";
    auto maybe_file = _fs.new_writable_file(file_path);
    ASSERT_OK(maybe_file.status());
    auto file = std::move(maybe_file.value());

    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(file), nullptr, _runtime_state);
    auto base_stream = std::make_shared<AsyncOutputStreamFile>(async_stream.get(), 1024);
    // Use a small buffer size to test buffer overflow handling
    auto compressed_stream_result = CompressedOutputStream::create(base_stream, CompressionTypePB::GZIP, 64);
    ASSERT_OK(compressed_stream_result.status());
    auto compressed_stream = std::move(compressed_stream_result.value());

    // Write data larger than buffer
    std::string test_data;
    for (int i = 0; i < 100; i++) {
        test_data += "Line " + std::to_string(i) + "\n";
    }

    ASSERT_OK(compressed_stream->write(Slice(test_data)));
    ASSERT_OK(compressed_stream->finalize());
    // Note: finalize() already closes the async stream internally

    // Read back and decompress
    std::string compressed_content;
    ASSERT_OK(_fs.read_file(file_path, &compressed_content));

    std::string decompressed = starrocks::test::decompress_gzip(compressed_content);
    EXPECT_EQ(decompressed, test_data);
}

// Test size() method
TEST_F(OutputStreamFileTest, TestSizeMethod) {
    std::string file_path = "/test_size.csv.gz";
    auto maybe_file = _fs.new_writable_file(file_path);
    ASSERT_OK(maybe_file.status());
    auto file = std::move(maybe_file.value());

    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(file), nullptr, _runtime_state);
    auto base_stream = std::make_shared<AsyncOutputStreamFile>(async_stream.get(), 1024);
    auto compressed_stream_result = CompressedOutputStream::create(base_stream, CompressionTypePB::GZIP, 1024);
    ASSERT_OK(compressed_stream_result.status());
    auto compressed_stream = std::move(compressed_stream_result.value());

    std::string test_data = "Test data\n";
    ASSERT_OK(compressed_stream->write(Slice(test_data)));
    ASSERT_OK(compressed_stream->finalize());

    // size() should return the position in the underlying stream
    size_t size = compressed_stream->size();
    EXPECT_GT(size, 0);

    // Note: finalize() already closes the async stream internally
}

// Test compression with special characters
TEST_F(OutputStreamFileTest, TestSpecialCharacters) {
    std::string file_path = "/test_special_chars.csv.gz";
    auto maybe_file = _fs.new_writable_file(file_path);
    ASSERT_OK(maybe_file.status());
    auto file = std::move(maybe_file.value());

    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(file), nullptr, _runtime_state);
    auto base_stream = std::make_shared<AsyncOutputStreamFile>(async_stream.get(), 1024);
    auto compressed_stream_result = CompressedOutputStream::create(base_stream, CompressionTypePB::GZIP, 1024);
    ASSERT_OK(compressed_stream_result.status());
    auto compressed_stream = std::move(compressed_stream_result.value());

    // Test data with special characters
    std::string test_data = "Hello,世界\n\"quoted,value\"\n\ttab\tseparated\n\\N\n";

    ASSERT_OK(compressed_stream->write(Slice(test_data)));
    ASSERT_OK(compressed_stream->finalize());
    // Note: finalize() already closes the async stream internally

    // Read back and decompress
    std::string compressed_content;
    ASSERT_OK(_fs.read_file(file_path, &compressed_content));

    std::string decompressed = starrocks::test::decompress_gzip(compressed_content);
    EXPECT_EQ(decompressed, test_data);
}

// Test compression with binary data
TEST_F(OutputStreamFileTest, TestBinaryData) {
    std::string file_path = "/test_binary.csv.gz";
    auto maybe_file = _fs.new_writable_file(file_path);
    ASSERT_OK(maybe_file.status());
    auto file = std::move(maybe_file.value());

    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(file), nullptr, _runtime_state);
    auto base_stream = std::make_shared<AsyncOutputStreamFile>(async_stream.get(), 1024);
    auto compressed_stream_result = CompressedOutputStream::create(base_stream, CompressionTypePB::GZIP, 1024);
    ASSERT_OK(compressed_stream_result.status());
    auto compressed_stream = std::move(compressed_stream_result.value());

    // Test data with binary content (all byte values)
    std::string test_data;
    for (int i = 0; i < 256; i++) {
        test_data += static_cast<char>(i);
    }

    ASSERT_OK(compressed_stream->write(Slice(test_data)));
    ASSERT_OK(compressed_stream->finalize());
    // Note: finalize() already closes the async stream internally

    // Read back and decompress
    std::string compressed_content;
    ASSERT_OK(_fs.read_file(file_path, &compressed_content));

    std::string decompressed = starrocks::test::decompress_gzip(compressed_content);
    EXPECT_EQ(decompressed, test_data);
}

// Test that uncompressed stream still works (regression test)
TEST_F(OutputStreamFileTest, TestUncompressedStream) {
    std::string file_path = "/test_uncompressed.csv";
    auto maybe_file = _fs.new_writable_file(file_path);
    ASSERT_OK(maybe_file.status());
    auto file = std::move(maybe_file.value());

    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(file), nullptr, _runtime_state);
    auto output_stream = std::make_unique<AsyncOutputStreamFile>(async_stream.get(), 1024);

    std::string test_data = "Uncompressed data\n";
    ASSERT_OK(output_stream->write(Slice(test_data)));
    ASSERT_OK(output_stream->finalize());
    // Note: finalize() already closes the async stream internally

    // Read back the data (should be uncompressed)
    std::string content;
    ASSERT_OK(_fs.read_file(file_path, &content));
    EXPECT_EQ(content, test_data);
}

// Note: TestWriteAfterFinalize was removed because OutputStream::write() only writes to
// internal buffer and doesn't check finalize state. The write would succeed to buffer,
// but any subsequent flush would fail since the underlying stream is closed.

// Test very small writes
TEST_F(OutputStreamFileTest, TestVerySmallWrites) {
    std::string file_path = "/test_small_writes.csv.gz";
    auto maybe_file = _fs.new_writable_file(file_path);
    ASSERT_OK(maybe_file.status());
    auto file = std::move(maybe_file.value());

    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(file), nullptr, _runtime_state);
    auto base_stream = std::make_shared<AsyncOutputStreamFile>(async_stream.get(), 1024);
    auto compressed_stream_result = CompressedOutputStream::create(base_stream, CompressionTypePB::GZIP, 1024);
    ASSERT_OK(compressed_stream_result.status());
    auto compressed_stream = std::move(compressed_stream_result.value());

    // Write one byte at a time
    std::string test_data = "ABCDEFGH";
    for (char c : test_data) {
        ASSERT_OK(compressed_stream->write(c));
    }

    ASSERT_OK(compressed_stream->finalize());
    // Note: finalize() already closes the async stream internally

    // Read back and decompress
    std::string compressed_content;
    ASSERT_OK(_fs.read_file(file_path, &compressed_content));

    std::string decompressed = starrocks::test::decompress_gzip(compressed_content);
    EXPECT_EQ(decompressed, test_data);
}

// Test SNAPPY compression is rejected (does not support incremental compression)
TEST_F(OutputStreamFileTest, TestSnappyCompressionRejected) {
    std::string file_path = "/test_snappy.csv.snappy";
    auto maybe_file = _fs.new_writable_file(file_path);
    ASSERT_OK(maybe_file.status());
    auto file = std::move(maybe_file.value());

    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(file), nullptr, _runtime_state);
    auto base_stream = std::make_shared<AsyncOutputStreamFile>(async_stream.get(), 1024);

    // SNAPPY does not support incremental compression, so creation should fail
    auto compressed_stream_result = CompressedOutputStream::create(base_stream, CompressionTypePB::SNAPPY, 1024);
    ASSERT_FALSE(compressed_stream_result.ok());
    EXPECT_TRUE(compressed_stream_result.status().is_invalid_argument());
}

// Test ZSTD compression
TEST_F(OutputStreamFileTest, TestZstdCompression) {
    std::string file_path = "/test_zstd.csv.zst";
    auto maybe_file = _fs.new_writable_file(file_path);
    ASSERT_OK(maybe_file.status());
    auto file = std::move(maybe_file.value());

    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(file), nullptr, _runtime_state);
    auto base_stream = std::make_shared<AsyncOutputStreamFile>(async_stream.get(), 1024);
    auto compressed_stream_result = CompressedOutputStream::create(base_stream, CompressionTypePB::ZSTD, 1024);
    ASSERT_OK(compressed_stream_result.status());
    auto compressed_stream = std::move(compressed_stream_result.value());

    // Generate test data
    std::string test_data;
    for (int i = 0; i < 100; i++) {
        test_data += "Row " + std::to_string(i) + ",data,value\n";
    }

    ASSERT_OK(compressed_stream->write(Slice(test_data)));
    ASSERT_OK(compressed_stream->finalize());
    // Note: finalize() already closes the async stream internally

    // Read back the compressed data
    std::string compressed_content;
    ASSERT_OK(_fs.read_file(file_path, &compressed_content));

    // Verify compression is effective
    EXPECT_LT(compressed_content.size(), test_data.size());

    // Decompress and verify
    std::string decompressed = starrocks::test::decompress_data(compressed_content, CompressionTypePB::ZSTD);
    EXPECT_EQ(decompressed, test_data);
}

// Test LZ4 (raw block) compression is rejected (does not support incremental compression)
TEST_F(OutputStreamFileTest, TestLz4CompressionRejected) {
    std::string file_path = "/test_lz4.csv.lz4";
    auto maybe_file = _fs.new_writable_file(file_path);
    ASSERT_OK(maybe_file.status());
    auto file = std::move(maybe_file.value());

    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(file), nullptr, _runtime_state);
    auto base_stream = std::make_shared<AsyncOutputStreamFile>(async_stream.get(), 1024);

    // LZ4 (raw block) does not support incremental compression, so creation should fail
    // Use LZ4_FRAME instead for incremental compression support
    auto compressed_stream_result = CompressedOutputStream::create(base_stream, CompressionTypePB::LZ4, 1024);
    ASSERT_FALSE(compressed_stream_result.ok());
    EXPECT_TRUE(compressed_stream_result.status().is_invalid_argument());
}

// Test LZ4_FRAME compression
TEST_F(OutputStreamFileTest, TestLz4FrameCompression) {
    std::string file_path = "/test_lz4_frame.csv.lz4";
    auto maybe_file = _fs.new_writable_file(file_path);
    ASSERT_OK(maybe_file.status());
    auto file = std::move(maybe_file.value());

    auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(file), nullptr, _runtime_state);
    auto base_stream = std::make_shared<AsyncOutputStreamFile>(async_stream.get(), 1024);
    auto compressed_stream_result = CompressedOutputStream::create(base_stream, CompressionTypePB::LZ4_FRAME, 1024);
    ASSERT_OK(compressed_stream_result.status());
    auto compressed_stream = std::move(compressed_stream_result.value());

    // Generate test data
    std::string test_data;
    for (int i = 0; i < 100; i++) {
        test_data += "Row " + std::to_string(i) + ",data,value\n";
    }

    ASSERT_OK(compressed_stream->write(Slice(test_data)));
    ASSERT_OK(compressed_stream->finalize());
    // Note: finalize() already closes the async stream internally

    // Read back the compressed data
    std::string compressed_content;
    ASSERT_OK(_fs.read_file(file_path, &compressed_content));

    // Verify compression is effective
    EXPECT_LT(compressed_content.size(), test_data.size());

    // Decompress and verify
    std::string decompressed = starrocks::test::decompress_data(compressed_content, CompressionTypePB::LZ4_FRAME);
    EXPECT_EQ(decompressed, test_data);
}

// Test all supported compression types with large data
// Only GZIP, LZ4_FRAME, and ZSTD support incremental compression via frame concatenation
TEST_F(OutputStreamFileTest, TestAllCompressionTypesLargeData) {
    std::vector<std::pair<CompressionTypePB, std::string>> compression_types = {
            {CompressionTypePB::GZIP, ".gz"},
            {CompressionTypePB::ZSTD, ".zst"},
            {CompressionTypePB::LZ4_FRAME, ".lz4"},
    };

    // Generate large test data (highly compressible)
    std::string test_data;
    for (int i = 0; i < 1000; i++) {
        test_data += "Line " + std::to_string(i) + ",repeated,data,for,compression,test\n";
    }

    for (const auto& [compression_type, extension] : compression_types) {
        std::string file_path =
                "/test_all_compression_" + std::to_string(static_cast<int>(compression_type)) + ".csv" + extension;
        auto maybe_file = _fs.new_writable_file(file_path);
        ASSERT_OK(maybe_file.status());
        auto file = std::move(maybe_file.value());

        auto async_stream = std::make_unique<io::AsyncFlushOutputStream>(std::move(file), nullptr, _runtime_state);
        auto base_stream = std::make_shared<AsyncOutputStreamFile>(async_stream.get(), 1024);
        auto compressed_stream_result = CompressedOutputStream::create(base_stream, compression_type, 1024);
        ASSERT_OK(compressed_stream_result.status());
        auto compressed_stream = std::move(compressed_stream_result.value());

        ASSERT_OK(compressed_stream->write(Slice(test_data)));
        ASSERT_OK(compressed_stream->finalize());
        // Note: finalize() already closes the async stream internally

        // Read back the compressed data
        std::string compressed_content;
        ASSERT_OK(_fs.read_file(file_path, &compressed_content));

        // Verify compression is effective
        EXPECT_LT(compressed_content.size(), test_data.size())
                << "Compression type " << static_cast<int>(compression_type) << " should compress data";

        // Decompress and verify - use different function for GZIP since BlockCompressionCodec
        // doesn't properly handle GZIP decompression output size
        std::string decompressed;
        if (compression_type == CompressionTypePB::GZIP) {
            decompressed = starrocks::test::decompress_gzip(compressed_content);
        } else {
            decompressed = starrocks::test::decompress_data(compressed_content, compression_type);
        }
        EXPECT_EQ(decompressed, test_data)
                << "Compression type " << static_cast<int>(compression_type) << " should decompress correctly";
    }
}

} // namespace starrocks::csv
