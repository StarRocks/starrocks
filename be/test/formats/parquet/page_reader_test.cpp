// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "formats/parquet/page_reader.h"

#include <gtest/gtest.h>

#include <iostream>

#include "fs/fs_memory.h"
#include "gen_cpp/parquet_types.h"
#include "io/string_input_stream.h"
#include "util/thrift_util.h"

namespace starrocks::parquet {

class ParquetPageReaderTest : public testing::Test {
public:
    ParquetPageReaderTest() {}
    ~ParquetPageReaderTest() override {}
};

TEST_F(ParquetPageReaderTest, Normal) {
    std::string buffer;
    // page 0
    {
        tparquet::PageHeader page_header;
        page_header.type = tparquet::PageType::DATA_PAGE;
        page_header.uncompressed_page_size = 100;
        page_header.compressed_page_size = 100;

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

        ThriftSerializer ser(true, 100);
        uint32_t len = 0;
        uint8_t* header_ser = nullptr;
        ser.serialize(&page_header, &len, &header_ser);
        buffer.append((char*)header_ser, len);

        buffer.resize(buffer.size() + page_header.compressed_page_size);
    }

    size_t total_size = buffer.size();

    RandomAccessFile file(std::make_shared<io::StringInputStream>(std::move(buffer)), "string-file");
    DefaultBufferedInputStream stream(&file, 0, total_size);

    PageReader reader(&stream, 0, total_size);

    // read page 1
    auto st = reader.next_header();
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(100, reader.current_header()->uncompressed_page_size);

    // read page 2
    reader.seek_to_offset(page_1_size);
    st = reader.next_header();
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(200, reader.current_header()->uncompressed_page_size);
    const uint8_t* data;
    st = reader.read_bytes(&data, 100);
    ASSERT_TRUE(st.ok());

    st = reader.read_bytes(&data, 250);
    ASSERT_FALSE(st.ok());

    // read out-of-page
    st = reader.next_header();
    ASSERT_FALSE(st.ok());
}

} // namespace starrocks::parquet
