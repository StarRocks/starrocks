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

#include "cache/lrucache_engine.h"
#include "exec/hdfs_scanner.h"
#include "formats/parquet/column_reader.h"
#include "fs/fs_memory.h"
#include "gen_cpp/parquet_types.h"
#include "io/shared_buffered_input_stream.h"
#include "io/string_input_stream.h"
#include "util/thrift_util.h"
#include "testutil/assert.h"

namespace starrocks::parquet {

class ParquetPageReaderTest : public testing::Test {
public:
    ParquetPageReaderTest() = default;
    ~ParquetPageReaderTest() override = default;
};

TEST_F(ParquetPageReaderTest, with_cache) {
    auto prev_page_cache = DataCache::GetInstance()->page_cache_ptr();
    CacheOptions options {.mem_space_size = 10 * 1024 * 1024};
    auto lru_cache = std::make_shared<LRUCacheEngine>();
    ASSERT_OK(lru_cache->init(options));
    auto cur_page_cache = std::make_shared<StoragePageCache>(lru_cache.get());
    DataCache::GetInstance()->set_page_cache(cur_page_cache);

    std::string buffer;
    HdfsScanStats stats;
    // page 0
    {
        tparquet::PageHeader page_header;
        page_header.type = tparquet::PageType::DATA_PAGE_V2;
        page_header.uncompressed_page_size = 100;
        page_header.compressed_page_size = 100;
        //page_header.data_page_header_v2.num_values = 10;
        //page_header.data_page_header_v2.__set_is_compressed(false);
        tparquet::DataPageHeaderV2 data_page_v2;
        data_page_v2.__set_num_values(10);
        data_page_v2.__set_is_compressed(false);
        page_header.__set_data_page_header_v2(data_page_v2);

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
        page_header.type = tparquet::PageType::DATA_PAGE_V2;
        page_header.uncompressed_page_size = 200;
        page_header.compressed_page_size = 300;
        tparquet::DataPageHeaderV2 data_page_v2;
        data_page_v2.__set_num_values(20);
        data_page_v2.__set_is_compressed(false);
        page_header.__set_data_page_header_v2(data_page_v2);

        ThriftSerializer ser(true, 100);
        uint32_t len = 0;
        uint8_t* header_ser = nullptr;
        CHECK(ser.serialize(&page_header, &len, &header_ser).ok());
        buffer.append((char*)header_ser, len);

        buffer.resize(buffer.size() + page_header.compressed_page_size);
    }

    // page 1
    {
        tparquet::PageHeader page_header;
        page_header.type = tparquet::PageType::DATA_PAGE_V2;
        page_header.uncompressed_page_size = 200;
        page_header.compressed_page_size = 300;
        tparquet::DataPageHeaderV2 data_page_v2;
        data_page_v2.__set_num_values(20);
        data_page_v2.__set_is_compressed(false);
        page_header.__set_data_page_header_v2(data_page_v2);

        ThriftSerializer ser(true, 100);
        uint32_t len = 0;
        uint8_t* header_ser = nullptr;
        CHECK(ser.serialize(&page_header, &len, &header_ser).ok());
        buffer.append((char*)header_ser, len);

        buffer.resize(buffer.size() + page_header.compressed_page_size);
    }

    size_t total_size = buffer.size();

    RandomAccessFile file(std::make_shared<io::StringInputStream>(std::move(buffer)), "string-file");

    io::SharedBufferedInputStream stream(file.stream(), file.filename(), file.get_size().value());

    DataCacheOptions cache_opts;
    cache_opts.enable_datacache = true;
    cache_opts.datacache_evict_probability = 100;
    ColumnReaderOptions opts;
    opts.stats = &stats;
    opts.datacache_options = &cache_opts;
    opts.use_file_pagecache = true;
    opts.file = &file;
    opts.file_size = 30;
    FileMetaData file_meta_data;
    file_meta_data._writer_version = ApplicationVersion::PARQUET_CPP_10353_FIXED_VERSION();
    opts.file_meta_data = &file_meta_data;
    PageReader reader(&stream, 0, total_size, 50, opts, tparquet::CompressionCodec::ZSTD);

    config::enable_adjustment_page_cache_skip = false;

    // read page 1
    cache_opts.datacache_evict_probability = 0;
    auto st = reader.next_header();
    LOG(ERROR) << "CUR_ST: " << st;
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(100, reader.current_header()->uncompressed_page_size);
    ASSERT_OK(reader.next_page());

    // read page 2
    cache_opts.datacache_evict_probability = 0;
    //reader.seek_to_offset(page_1_size);
    st = reader.next_header();
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(200, reader.current_header()->uncompressed_page_size);
    ASSERT_OK(reader.next_page());

    // read page 2
    cache_opts.datacache_evict_probability = 0;
    //reader.seek_to_offset(page_1_size);
    st = reader.next_header();
    LOG(ERROR) << "ERROR:" << st;
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(200, reader.current_header()->uncompressed_page_size);

    DataCache::GetInstance()->set_page_cache(prev_page_cache);
}

TEST_F(ParquetPageReaderTest, Normal) {
    std::string buffer;
    HdfsScanStats stats;

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

    io::SharedBufferedInputStream stream(file.stream(), file.filename(), file.get_size().value());

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
    HdfsScanStats stats;

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

    io::SharedBufferedInputStream stream(file.stream(), file.filename(), file.get_size().value());

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

} // namespace starrocks::parquet
