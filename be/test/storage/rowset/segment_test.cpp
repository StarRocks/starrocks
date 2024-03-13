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
//   https://github.com/apache/incubator-doris/blob/master/be/test/olap/rowset/segment_v2/segment_test.cpp

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

#include "storage/rowset/segment.h"

#include <gtest/gtest.h>

#include <functional>
#include <iostream>

#include "column/datum_tuple.h"
#include "common/logging.h"
#include "fs/fs_memory.h"
#include "gutil/strings/substitute.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/olap_common.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_writer.h"
#include "storage/tablet_schema.h"
#include "storage/tablet_schema_helper.h"
#include "testutil/assert.h"

namespace starrocks {

using std::string;
using std::shared_ptr;

using std::vector;

using ValueGenerator = std::function<Datum(size_t rid, int cid, int block_id)>;

// 0,  1,  2,  3
// 10, 11, 12, 13
// 20, 21, 22, 23
static Datum DefaultIntGenerator(size_t rid, int cid, int block_id) {
    return {static_cast<int32_t>(rid * 10 + cid)};
}

class SegmentReaderWriterTest : public ::testing::Test {
protected:
    void SetUp() override {
        _fs = std::make_shared<MemoryFileSystem>();
        ASSERT_TRUE(_fs->create_dir(kSegmentDir).ok());
    }

    void TearDown() override { StoragePageCache::instance()->prune(); }

    void build_segment(const SegmentWriterOptions& opts, const TabletSchemaCSPtr& build_schema,
                       const TabletSchemaCSPtr& query_schema, size_t nrows, const ValueGenerator& generator,
                       shared_ptr<Segment>* res) {
        static int seg_id = 0;
        // must use unique filename for each segment, otherwise page cache kicks in and produces
        // the wrong answer (it use (filename,offset) as cache key)
        std::string filename = strings::Substitute("$0/seg_$1.dat", kSegmentDir, seg_id++);
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(filename));
        SegmentWriter writer(std::move(wfile), 0, build_schema, opts);
        ASSERT_OK(writer.init());

        auto schema = ChunkHelper::convert_schema(build_schema);
        auto chunk = ChunkHelper::new_chunk(schema, nrows);
        for (size_t rid = 0; rid < nrows; ++rid) {
            auto& cols = chunk->columns();
            for (int cid = 0; cid < build_schema->num_columns(); ++cid) {
                int row_block_id = rid / opts.num_rows_per_block;
                cols[cid]->append_datum(generator(rid, cid, row_block_id));
            }
        }
        ASSERT_OK(writer.append_chunk(*chunk));

        uint64_t file_size, index_size, footer_position;
        ASSERT_OK(writer.finalize(&file_size, &index_size, &footer_position));

        *res = *Segment::open(_fs, FileInfo{filename}, 0, query_schema);
        ASSERT_EQ(nrows, (*res)->num_rows());
    }

    const std::string kSegmentDir = "/segment_test";

    std::shared_ptr<MemoryFileSystem> _fs = nullptr;
    std::unique_ptr<MemTracker> _page_cache_mem_tracker = nullptr;
};

TEST_F(SegmentReaderWriterTest, estimate_segment_size) {
    size_t num_rows_per_block = 10;

    std::shared_ptr<TabletSchema> tablet_schema = TabletSchemaHelper::create_tablet_schema(
            {create_int_key_pb(1), create_int_key_pb(2), create_int_key_pb(3), create_int_value_pb(4)}, 2);
    tablet_schema->_num_rows_per_row_block = 2;

    // segment write
    std::string dname = "/segment_write_size";
    ASSERT_OK(_fs->create_dir(dname));

    SegmentWriterOptions opts;
    opts.num_rows_per_block = num_rows_per_block;

    std::string fname = dname + "/int_case";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(fname));
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);
    ASSERT_OK(writer.init());

    // 0, 1, 2, 3
    // 10, 11, 12, 13
    // 20, 21, 22, 23
    size_t nrows = 1048576;
    auto schema = ChunkHelper::convert_schema(tablet_schema);
    auto chunk = ChunkHelper::new_chunk(schema, nrows);
    for (size_t rid = 0; rid < nrows; ++rid) {
        auto& cols = chunk->columns();
        for (int cid = 0; cid < tablet_schema->num_columns(); ++cid) {
            cols[cid]->append_datum(Datum(static_cast<int32_t>(rid * 10 + cid)));
        }
    }
    ASSERT_OK(writer.append_chunk(*chunk));

    uint32_t segment_size = writer.estimate_segment_size();
    LOG(INFO) << "estimated segment sizes=" << segment_size;

    uint64_t file_size = 0;
    uint64_t index_size;
    uint64_t footer_position;
    ASSERT_OK(writer.finalize(&file_size, &index_size, &footer_position));

    file_size = _fs->get_file_size(fname).value();
    LOG(INFO) << "segment file size=" << file_size;

    ASSERT_NE(segment_size, 0);
}

TEST_F(SegmentReaderWriterTest, TestBloomFilterIndexUniqueModel) {
    std::shared_ptr<TabletSchema> schema =
            TabletSchemaHelper::create_tablet_schema({create_int_key_pb(1), create_int_key_pb(2), create_int_key_pb(3),
                                                      create_int_value_pb(4, "REPLACE", true, "", true)});

    // for not base segment
    SegmentWriterOptions opts1;
    shared_ptr<Segment> seg1;
    build_segment(opts1, schema, schema, 100, DefaultIntGenerator, &seg1);
    ASSERT_TRUE(seg1->column(3)->has_bloom_filter_index());

    // for base segment
    SegmentWriterOptions opts2;
    shared_ptr<Segment> seg2;
    build_segment(opts2, schema, schema, 100, DefaultIntGenerator, &seg2);
    ASSERT_TRUE(seg2->column(3)->has_bloom_filter_index());
}

TEST_F(SegmentReaderWriterTest, TestHorizontalWrite) {
    std::shared_ptr<TabletSchema> tablet_schema = TabletSchemaHelper::create_tablet_schema(
            {create_int_key_pb(1), create_int_key_pb(2), create_int_value_pb(3), create_int_value_pb(4)});

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 10;

    std::string file_name = kSegmentDir + "/horizontal_write_case";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));

    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);
    ASSERT_OK(writer.init());

    int32_t chunk_size = config::vector_chunk_size;
    size_t num_rows = 10000;
    auto schema = ChunkHelper::convert_schema(tablet_schema);
    auto chunk = ChunkHelper::new_chunk(schema, chunk_size);
    for (auto i = 0; i < num_rows % chunk_size; ++i) {
        chunk->reset();
        auto& cols = chunk->columns();
        for (auto j = 0; j < chunk_size && i * chunk_size + j < num_rows; ++j) {
            cols[0]->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j)));
            cols[1]->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j + 1)));
            cols[2]->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j + 2)));
            cols[3]->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j + 3)));
        }
        ASSERT_OK(writer.append_chunk(*chunk));
    }

    uint64_t file_size = 0;
    uint64_t index_size;
    uint64_t footer_position;
    ASSERT_OK(writer.finalize(&file_size, &index_size, &footer_position));

    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);

    SegmentReadOptions seg_options;
    seg_options.fs = _fs;
    OlapReaderStatistics stats;
    seg_options.stats = &stats;
    auto res = segment->new_iterator(schema, seg_options);
    ASSERT_FALSE(res.status().is_end_of_file() || !res.ok() || res.value() == nullptr);
    auto seg_iterator = res.value();

    size_t count = 0;
    while (true) {
        chunk->reset();
        auto st = seg_iterator->get_next(chunk.get());
        if (st.is_end_of_file()) {
            break;
        }
        ASSERT_FALSE(!st.ok());
        for (auto i = 0; i < chunk->num_rows(); ++i) {
            EXPECT_EQ(count, chunk->get(i)[0].get_int32());
            EXPECT_EQ(count + 1, chunk->get(i)[1].get_int32());
            EXPECT_EQ(count + 2, chunk->get(i)[2].get_int32());
            EXPECT_EQ(count + 3, chunk->get(i)[3].get_int32());
            ++count;
        }
    }
    EXPECT_EQ(count, num_rows);

    // Test new_column_iterator
    {
        auto r = segment->new_column_iterator(5 /* nonexist column id*/, nullptr);
        ASSERT_FALSE(r.ok());
        ASSERT_TRUE(r.status().is_not_found()) << r.status();
    }
    // Test new_column_iterator_or_default
    {
        TabletColumn column;
        column.set_unique_id(5);
        column.set_type(LogicalType::TYPE_BIGINT);
        column.set_is_nullable(false);

        auto r = segment->new_column_iterator_or_default(column, nullptr);
        ASSERT_FALSE(r.ok());

        column.set_is_nullable(true);
        r = segment->new_column_iterator_or_default(column, nullptr);
        ASSERT_TRUE(r.ok()) << r.status();

        column.set_is_nullable(false);
        column.set_default_value("10");
        r = segment->new_column_iterator_or_default(column, nullptr);
        ASSERT_TRUE(r.ok()) << r.status();
    }
}

// NOLINTNEXTLINE
TEST_F(SegmentReaderWriterTest, TestVerticalWrite) {
    std::shared_ptr<TabletSchema> tablet_schema = TabletSchemaHelper::create_tablet_schema(
            {create_int_key_pb(1), create_int_key_pb(2), create_int_value_pb(3), create_int_value_pb(4)});

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 10;

    std::string file_name = kSegmentDir + "/vertical_write_case";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));

    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);

    int32_t chunk_size = config::vector_chunk_size;
    size_t num_rows = 10000;
    uint64_t file_size = 0;
    uint64_t index_size = 0;

    {
        // col1 col2
        std::vector<uint32_t> column_indexes{0, 1};
        ASSERT_OK(writer.init(column_indexes, true));
        auto schema = ChunkHelper::convert_schema(tablet_schema, column_indexes);
        auto chunk = ChunkHelper::new_chunk(schema, chunk_size);
        for (auto i = 0; i < num_rows % chunk_size; ++i) {
            chunk->reset();
            auto& cols = chunk->columns();
            for (auto j = 0; j < chunk_size && i * chunk_size + j < num_rows; ++j) {
                cols[0]->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j)));
                cols[1]->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j + 1)));
            }
            ASSERT_OK(writer.append_chunk(*chunk));
        }
        ASSERT_OK(writer.finalize_columns(&index_size));
    }

    {
        // col3
        std::vector<uint32_t> column_indexes{2};
        ASSERT_OK(writer.init(column_indexes, false));
        auto schema = ChunkHelper::convert_schema(tablet_schema, column_indexes);
        auto chunk = ChunkHelper::new_chunk(schema, chunk_size);
        for (auto i = 0; i < num_rows % chunk_size; ++i) {
            chunk->reset();
            auto& cols = chunk->columns();
            for (auto j = 0; j < chunk_size && i * chunk_size + j < num_rows; ++j) {
                cols[0]->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j + 2)));
            }
            ASSERT_OK(writer.append_chunk(*chunk));
        }
        ASSERT_OK(writer.finalize_columns(&index_size));
    }

    {
        // col4
        std::vector<uint32_t> column_indexes{3};
        ASSERT_OK(writer.init(column_indexes, false));
        auto schema = ChunkHelper::convert_schema(tablet_schema, column_indexes);
        auto chunk = ChunkHelper::new_chunk(schema, chunk_size);
        for (auto i = 0; i < num_rows % chunk_size; ++i) {
            chunk->reset();
            auto& cols = chunk->columns();
            for (auto j = 0; j < chunk_size && i * chunk_size + j < num_rows; ++j) {
                cols[0]->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j + 3)));
            }
            ASSERT_OK(writer.append_chunk(*chunk));
        }
        ASSERT_OK(writer.finalize_columns(&index_size));
    }

    ASSERT_OK(writer.finalize_footer(&file_size));

    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);

    SegmentReadOptions seg_options;
    seg_options.fs = _fs;
    OlapReaderStatistics stats;
    seg_options.stats = &stats;
    auto schema = ChunkHelper::convert_schema(tablet_schema);
    auto res = segment->new_iterator(schema, seg_options);
    ASSERT_FALSE(res.status().is_end_of_file() || !res.ok() || res.value() == nullptr);
    auto seg_iterator = res.value();

    size_t count = 0;
    auto chunk = ChunkHelper::new_chunk(schema, chunk_size);
    while (true) {
        chunk->reset();
        auto st = seg_iterator->get_next(chunk.get());
        if (st.is_end_of_file()) {
            break;
        }
        ASSERT_FALSE(!st.ok());
        for (auto i = 0; i < chunk->num_rows(); ++i) {
            EXPECT_EQ(count, chunk->get(i)[0].get_int32());
            EXPECT_EQ(count + 1, chunk->get(i)[1].get_int32());
            EXPECT_EQ(count + 2, chunk->get(i)[2].get_int32());
            EXPECT_EQ(count + 3, chunk->get(i)[3].get_int32());
            ++count;
        }
    }
    EXPECT_EQ(count, num_rows);
}

TEST_F(SegmentReaderWriterTest, TestReadMultipleTypesColumn) {
    std::string s1("abcdefghijklmnopqrstuvwxyz");
    std::string s2("bbcdefghijklmnopqrstuvwxyz");
    std::string s3("cbcdefghijklmnopqrstuvwxyz");
    std::string s4("dbcdefghijklmnopqrstuvwxyz");
    std::string s5("ebcdefghijklmnopqrstuvwxyz");
    std::string s6("fbcdefghijklmnopqrstuvwxyz");
    std::string s7("gbcdefghijklmnopqrstuvwxyz");
    std::string s8("hbcdefghijklmnopqrstuvwxyz");
    std::vector<Slice> data_strs{s1, s2, s3, s4, s5, s6, s7, s8};

    ColumnPB c1 = create_int_key_pb(1);
    ColumnPB c2 = create_int_key_pb(2);
    ColumnPB c3 = create_with_default_value_pb("VARCHAR", "");
    c3.set_length(65535);

    std::shared_ptr<TabletSchema> tablet_schema = TabletSchemaHelper::create_tablet_schema({c1, c2, c3});

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 10;

    std::string file_name = kSegmentDir + "/read_multiple_types_column";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));

    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);

    int32_t chunk_size = config::vector_chunk_size;
    size_t num_rows = 10000;
    uint64_t file_size = 0;
    uint64_t index_size = 0;

    {
        // col1 col2
        std::vector<uint32_t> column_indexes{0, 1};
        ASSERT_OK(writer.init(column_indexes, true));
        auto schema = ChunkHelper::convert_schema(tablet_schema, column_indexes);
        auto chunk = ChunkHelper::new_chunk(schema, chunk_size);
        for (auto i = 0; i < num_rows % chunk_size; ++i) {
            chunk->reset();
            auto& cols = chunk->columns();
            for (auto j = 0; j < chunk_size && i * chunk_size + j < num_rows; ++j) {
                cols[0]->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j)));
                cols[1]->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j + 1)));
            }
            ASSERT_OK(writer.append_chunk(*chunk));
        }
        ASSERT_OK(writer.finalize_columns(&index_size));
    }

    {
        // col3
        std::vector<uint32_t> column_indexes{2};
        ASSERT_OK(writer.init(column_indexes, false));
        auto schema = ChunkHelper::convert_schema(tablet_schema, column_indexes);
        auto chunk = ChunkHelper::new_chunk(schema, chunk_size);
        for (auto i = 0; i < num_rows % chunk_size; ++i) {
            chunk->reset();
            auto& cols = chunk->columns();
            for (auto j = 0; j < chunk_size && i * chunk_size + j < num_rows; ++j) {
                cols[0]->append_datum(Datum(data_strs[j % 8]));
            }
            ASSERT_OK(writer.append_chunk(*chunk));
        }
        ASSERT_OK(writer.finalize_columns(&index_size));
    }

    ASSERT_OK(writer.finalize_footer(&file_size));
    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);

    SegmentReadOptions seg_options;
    seg_options.fs = _fs;
    OlapReaderStatistics stats;
    seg_options.stats = &stats;
    auto schema = ChunkHelper::convert_schema(tablet_schema);
    auto res = segment->new_iterator(schema, seg_options);
    ASSERT_FALSE(res.status().is_end_of_file() || !res.ok() || res.value() == nullptr);
    auto seg_iterator = res.value();

    size_t count = 0;
    auto chunk = ChunkHelper::new_chunk(schema, chunk_size);
    while (true) {
        chunk->reset();
        auto st = seg_iterator->get_next(chunk.get());
        if (st.is_end_of_file()) {
            break;
        }
        ASSERT_FALSE(!st.ok());
        for (auto i = 0; i < chunk->num_rows(); ++i) {
            EXPECT_EQ(count, chunk->get(i)[0].get_int32());
            EXPECT_EQ(count + 1, chunk->get(i)[1].get_int32());
            EXPECT_EQ(data_strs[i % 8].to_string(), chunk->get(i)[2].get_slice().to_string());
            ++count;
        }
    }
    EXPECT_EQ(count, num_rows);
}

} // namespace starrocks
