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

#include "exec/hdfs_scanner.h"
#include "fs/fs_memory.h"
#include "formats/parquet/column_reader.h"
#include "gen_cpp/parquet_types.h"
#include "io/shared_buffered_input_stream.h"
#include "io/string_input_stream.h"
#include "util/thrift_util.h"
namespace starrocks::parquet {

class ParquetPageReaderTest : public testing::Test {
public:
    ParquetPageReaderTest() = default;
    ~ParquetPageReaderTest() override = default;
};

TEST_F(ParquetPageReaderTest, Normal) {
    std::string buffer;
    ColumnReaderOptions opts;

    std::vector<uint8_t> read_buffer;
    read_buffer.reserve(1024);
    uint8_t* data = read_buffer.data();

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
        ser.serialize(&page_header, &len, &header_ser);
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
        ser.serialize(&page_header, &len, &header_ser);
        buffer.append((char*)header_ser, len);

        buffer.resize(buffer.size() + page_header.compressed_page_size);
    }

    size_t total_size = buffer.size();

    RandomAccessFile file(std::make_shared<io::StringInputStream>(std::move(buffer)), "string-file");

    io::SharedBufferedInputStream stream(file.stream(), file.filename(), file.get_size().value());

    PageReader reader(&stream, 0, total_size, 30, opts);

    // read page 1
    auto st = reader.next_header();
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(100, reader.current_header()->uncompressed_page_size);

    // read page 2
    reader.seek_to_offset(page_1_size);
    st = reader.next_header();
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(200, reader.current_header()->uncompressed_page_size);

    st = reader.read_bytes(data, 100);
    ASSERT_TRUE(st.ok());

    st = reader.read_bytes(data, 250);
    ASSERT_FALSE(st.ok());

    // read out-of-page
    st = reader.next_header();
    ASSERT_FALSE(st.ok());
}

TEST_F(ParquetPageReaderTest, ExtraBytes) {
    std::string buffer;
    ColumnReaderOptions opts;

    std::vector<uint8_t> read_buffer;
    read_buffer.reserve(1024);
    uint8_t* data = read_buffer.data();

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
        ser.serialize(&page_header, &len, &header_ser);
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
        ser.serialize(&page_header, &len, &header_ser);
        buffer.append((char*)header_ser, len);

        buffer.resize(buffer.size() + page_header.compressed_page_size);
    }

    size_t extra_nbytes = 10;
    buffer.resize(buffer.size() + extra_nbytes);
    size_t total_size = buffer.size();

    RandomAccessFile file(std::make_shared<io::StringInputStream>(std::move(buffer)), "string-file");

    io::SharedBufferedInputStream stream(file.stream(), file.filename(), file.get_size().value());

    HdfsScannerContext ctx;
    PageReader reader(&stream, 0, total_size, 30, opts);

    // read page 1
    auto st = reader.next_header();
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(100, reader.current_header()->uncompressed_page_size);

    // read page 2
    reader.seek_to_offset(page_1_size);
    st = reader.next_header();
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(200, reader.current_header()->uncompressed_page_size);

    st = reader.read_bytes(data, 100);
    ASSERT_TRUE(st.ok());

    st = reader.read_bytes(data, 250);
    ASSERT_FALSE(st.ok());

    // read out-of-page
    st = reader.next_header();
    ASSERT_FALSE(st.ok());
}

} // namespace starrocks::parquet
