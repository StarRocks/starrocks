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

#include "formats/parquet/page_reader.h"

#include <gtest/gtest.h>

#include <iostream>

#include "base/testutil/assert.h"
#include "cache/scan/shared_buffered_input_stream.h"
#include "common/util/thrift_util.h"
#include "connector/hive/scanner/hdfs_scanner.h"
#include "formats/parquet/column_reader.h"
#include "fs/fs_memory.h"
#include "gen_cpp/parquet_types.h"
#include "io/string_input_stream.h"
namespace starrocks::parquet {

class ParquetPageReaderTest : public testing::Test {
public:
    ParquetPageReaderTest() = default;
    ~ParquetPageReaderTest() override = default;
};

TEST_F(ParquetPageReaderTest, Normal) {
    std::string buffer;
    FormatScannerStats stats;

    // page 0
    {
        tparquet::PageHeader page_header;
        page_header.type = tparquet::PageType::DATA_PAGE;
        page_header.uncompressed_page_size = 100;
        page_header.compressed_page_size = 100;
        page_header.data_page_header.num_values = 10;

        ThriftSerializer ser(true, 100);
        uint32_t len = 0;
        uint8_t* header_ser = nullptr;
        CHECK(ser.serialize(&page_header, &len, &header_ser).ok());
        buffer.append((char*)header_ser, len);

        buffer.resize(buffer.size() + page_header.compressed_page_size);
    }

    size_t page_1_size = buffer.size();
    // page 1
    {
        tparquet::PageHeader page_header;
        page_header.type = tparquet::PageType::DATA_PAGE;
        page_header.uncompressed_page_size = 200;
        page_header.compressed_page_size = 300;
        page_header.data_page_header.num_values = 20;

        ThriftSerializer ser(true, 100);
        uint32_t len = 0;
        uint8_t* header_ser = nullptr;
        CHECK(ser.serialize(&page_header, &len, &header_ser).ok());
        buffer.append((char*)header_ser, len);

        buffer.resize(buffer.size() + page_header.compressed_page_size);
    }

    size_t total_size = buffer.size();

    RandomAccessFile file(std::make_shared<io::StringInputStream>(std::move(buffer)), "string-file");

    SharedBufferedInputStream stream(file.stream(), file.filename(), file.get_size().value());

    ColumnReaderOptions opts;
    opts.stats = &stats;
    PageReader reader(&stream, 0, total_size, 30, opts, tparquet::CompressionCodec::ZSTD);

    // read page 1
    auto st = reader.next_header();
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(100, reader.current_header()->uncompressed_page_size);

    // read page 2
    reader.seek_to_offset(page_1_size);
    st = reader.next_header();
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(200, reader.current_header()->uncompressed_page_size);

    // read out-of-page
    reader.seek_to_offset(total_size);
    st = reader.next_header();
    ASSERT_FALSE(st.ok());
}

TEST_F(ParquetPageReaderTest, ExtraBytes) {
    std::string buffer;
    FormatScannerStats stats;

    // page 0
    {
        tparquet::PageHeader page_header;
        page_header.type = tparquet::PageType::DATA_PAGE;
        page_header.uncompressed_page_size = 100;
        page_header.compressed_page_size = 100;
        page_header.data_page_header.num_values = 10;

        ThriftSerializer ser(true, 100);
        uint32_t len = 0;
        uint8_t* header_ser = nullptr;
        CHECK(ser.serialize(&page_header, &len, &header_ser).ok());
        buffer.append((char*)header_ser, len);

        buffer.resize(buffer.size() + page_header.compressed_page_size);
    }

    size_t page_1_size = buffer.size();
    // page 1
    {
        tparquet::PageHeader page_header;
        page_header.type = tparquet::PageType::DATA_PAGE;
        page_header.uncompressed_page_size = 200;
        page_header.compressed_page_size = 300;
        page_header.data_page_header.num_values = 20;

        ThriftSerializer ser(true, 100);
        uint32_t len = 0;
        uint8_t* header_ser = nullptr;
        CHECK(ser.serialize(&page_header, &len, &header_ser).ok());
        buffer.append((char*)header_ser, len);

        buffer.resize(buffer.size() + page_header.compressed_page_size);
    }
    size_t page_2_size = buffer.size();

    size_t extra_nbytes = 10;
    buffer.resize(buffer.size() + extra_nbytes);
    size_t total_size = buffer.size();

    RandomAccessFile file(std::make_shared<io::StringInputStream>(std::move(buffer)), "string-file");

    SharedBufferedInputStream stream(file.stream(), file.filename(), file.get_size().value());

    ColumnReaderOptions opts;
    opts.stats = &stats;
    PageReader reader(&stream, 0, total_size, 30, opts, tparquet::CompressionCodec::ZSTD);

    // read page 1
    auto st = reader.next_header();
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(100, reader.current_header()->uncompressed_page_size);

    // read page 2
    reader.seek_to_offset(page_1_size);
    st = reader.next_header();
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(200, reader.current_header()->uncompressed_page_size);

    // read out-of-page
    ASSERT_TRUE(reader.seek_to_offset(page_2_size).ok());
    st = reader.next_header();
    ASSERT_FALSE(st.ok());
}

// A page header larger than the first header-size guess (1KB) is still parsed: the guess doubles until
// the header deserializes, and the header is followed by the right page.
TEST_F(ParquetPageReaderTest, HeaderLargerThanFirstGuess) {
    FormatScannerStats stats;
    std::string buffer;
    std::vector<size_t> page_offsets;
    for (size_t min_max_size : {5000, 10}) {
        tparquet::PageHeader page_header;
        page_header.type = tparquet::PageType::DATA_PAGE;
        page_header.uncompressed_page_size = static_cast<int32_t>(min_max_size);
        page_header.compressed_page_size = 64;
        tparquet::Statistics statistics;
        statistics.__set_min_value(std::string(min_max_size, 'a'));
        statistics.__set_max_value(std::string(min_max_size, 'z'));
        tparquet::DataPageHeader data_page_header;
        data_page_header.num_values = 10;
        data_page_header.__set_statistics(statistics);
        page_header.__set_data_page_header(data_page_header);

        ThriftSerializer ser(true, 100);
        uint32_t len = 0;
        uint8_t* header_ser = nullptr;
        CHECK(ser.serialize(&page_header, &len, &header_ser).ok());
        page_offsets.push_back(buffer.size());
        buffer.append((char*)header_ser, len);
        buffer.resize(buffer.size() + page_header.compressed_page_size);
    }
    ASSERT_GT(page_offsets[1], 10000);
    size_t total_size = buffer.size();

    RandomAccessFile file(std::make_shared<io::StringInputStream>(std::move(buffer)), "string-file");
    SharedBufferedInputStream stream(file.stream(), file.filename(), file.get_size().value());

    ColumnReaderOptions opts;
    opts.stats = &stats;
    PageReader reader(&stream, 0, total_size, 20, opts, tparquet::CompressionCodec::UNCOMPRESSED);

    ASSERT_OK(reader.next_header());
    ASSERT_EQ(5000, reader.current_header()->uncompressed_page_size);
    ASSERT_EQ(std::string(5000, 'z'), reader.current_header()->data_page_header.statistics.max_value);

    reader.seek_to_offset(page_offsets[1]);
    ASSERT_OK(reader.next_header());
    ASSERT_EQ(10, reader.current_header()->uncompressed_page_size);
}

} // namespace starrocks::parquet
