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

#include "storage/segment_stream_converter.h"

#include <gtest/gtest.h>

#include "fs/fs.h"
#include "fs/fs_memory.h"
#include "testutil/assert.h"
#include "util/uuid_generator.h"

namespace starrocks {

extern std::atomic<size_t> s_segment_footer_buffer_size;

class SegmentStreamConverterTest : public testing::Test {
public:
    SegmentStreamConverterTest() = default;
    ~SegmentStreamConverterTest() override = default;

    void SetUp() override {}

    void TearDown() override {}
};

TEST_F(SegmentStreamConverterTest, test_no_convert) {
    s_segment_footer_buffer_size.store(1024);

    MemoryFileSystem memory_fs;
    auto status_or = memory_fs.new_writable_file("/test_url");
    EXPECT_TRUE(status_or.ok());

    auto wfile = std::move(status_or.value());
    SegmentStreamConverter segment_stream_converter("test_name", 1024, std::move(wfile), nullptr);

    char data[1024];
    auto status = segment_stream_converter.append(data, sizeof(data));
    EXPECT_TRUE(status.ok());

    EXPECT_EQ(segment_stream_converter._output_file->size(), sizeof(data));
    EXPECT_TRUE(segment_stream_converter._segment_footer_buffer.empty());
}

TEST_F(SegmentStreamConverterTest, test_convert_small_segment) {
    s_segment_footer_buffer_size.store(1024);

    MemoryFileSystem memory_fs;
    auto status_or = memory_fs.new_writable_file("/test_url");
    EXPECT_TRUE(status_or.ok());

    auto wfile = std::move(status_or.value());
    std::unordered_map<uint32_t, uint32_t> unique_id_map = {{1, 2}};
    SegmentStreamConverter segment_stream_converter("test_name", 1024, std::move(wfile), &unique_id_map);

    char data[512];
    auto status = segment_stream_converter.append(data, sizeof(data));
    EXPECT_TRUE(status.ok());

    EXPECT_EQ(segment_stream_converter._output_file->size(), 0);
    EXPECT_EQ(segment_stream_converter._segment_footer_buffer.size(), 512);

    status = segment_stream_converter.append(data, sizeof(data));
    EXPECT_TRUE(status.ok());

    EXPECT_EQ(segment_stream_converter._output_file->size(), 0);
    EXPECT_EQ(segment_stream_converter._segment_footer_buffer.size(), 1024);
}

TEST_F(SegmentStreamConverterTest, test_convert_large_segment) {
    s_segment_footer_buffer_size.store(512);

    MemoryFileSystem memory_fs;
    auto status_or = memory_fs.new_writable_file("/test_url");
    EXPECT_TRUE(status_or.ok());

    auto wfile = std::move(status_or.value());
    std::unordered_map<uint32_t, uint32_t> unique_id_map = {{1, 2}};
    SegmentStreamConverter segment_stream_converter("test_name", 1024, std::move(wfile), &unique_id_map);

    char data[256];
    auto status = segment_stream_converter.append(data, sizeof(data));
    EXPECT_TRUE(status.ok());

    EXPECT_EQ(segment_stream_converter._output_file->size(), 256);
    EXPECT_EQ(segment_stream_converter._segment_footer_buffer.size(), 0);

    status = segment_stream_converter.append(data, sizeof(data));
    EXPECT_TRUE(status.ok());

    EXPECT_EQ(segment_stream_converter._output_file->size(), 512);
    EXPECT_EQ(segment_stream_converter._segment_footer_buffer.size(), 0);

    status = segment_stream_converter.append(data, sizeof(data));
    EXPECT_TRUE(status.ok());

    EXPECT_EQ(segment_stream_converter._output_file->size(), 512);
    EXPECT_EQ(segment_stream_converter._segment_footer_buffer.size(), 256);

    status = segment_stream_converter.append(data, sizeof(data));
    EXPECT_TRUE(status.ok());

    EXPECT_EQ(segment_stream_converter._output_file->size(), 512);
    EXPECT_EQ(segment_stream_converter._segment_footer_buffer.size(), 512);
}

TEST_F(SegmentStreamConverterTest, test_convert_large_segment2) {
    s_segment_footer_buffer_size.store(512);

    MemoryFileSystem memory_fs;
    auto status_or = memory_fs.new_writable_file("/test_url");
    EXPECT_TRUE(status_or.ok());

    auto wfile = std::move(status_or.value());
    std::unordered_map<uint32_t, uint32_t> unique_id_map = {{1, 2}};
    SegmentStreamConverter segment_stream_converter("test_name", 1024, std::move(wfile), &unique_id_map);

    char data[512];
    auto status = segment_stream_converter.append(data, 256);
    EXPECT_TRUE(status.ok());

    EXPECT_EQ(segment_stream_converter._output_file->size(), 256);
    EXPECT_EQ(segment_stream_converter._segment_footer_buffer.size(), 0);

    status = segment_stream_converter.append(data, 512);
    EXPECT_TRUE(status.ok());

    EXPECT_EQ(segment_stream_converter._output_file->size(), 512);
    EXPECT_EQ(segment_stream_converter._segment_footer_buffer.size(), 256);

    status = segment_stream_converter.append(data, 256);
    EXPECT_TRUE(status.ok());

    EXPECT_EQ(segment_stream_converter._output_file->size(), 512);
    EXPECT_EQ(segment_stream_converter._segment_footer_buffer.size(), 512);
}

} // namespace starrocks
