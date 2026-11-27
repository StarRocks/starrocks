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

#include <algorithm>
#include <functional>
#include <iostream>

#include "base/bit/rle_encoding.h"
#include "base/coding.h"
#include "base/failpoint/fail_point.h"
#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "cache/mem_cache/page_handle.h"
#include "column/chunk_factory.h"
#include "column/datum_tuple.h"
#include "column/geo_column.h"
#include "column/nullable_column.h"
#include "common/config_exec_fwd.h"
#include "common/logging.h"
#include "fs/fs_memory.h"
#include "gutil/strings/substitute.h"
#include "platform/key_cache.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/olap_common.h"
#include "storage/rowset/binary_plain_page.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/scalar_column_iterator.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_writer.h"
#include "storage/tablet_schema.h"
#include "storage/tablet_schema_helper.h"
#include "storage_primitive/chunk_iterator.h"

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

    void TearDown() override {}

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
        auto chunk = ChunkFactory::new_chunk(schema, nrows);
        for (size_t rid = 0; rid < nrows; ++rid) {
            auto cols = chunk->columns();
            for (int cid = 0; cid < build_schema->num_columns(); ++cid) {
                int row_block_id = rid / opts.num_rows_per_block;
                cols[cid]->as_mutable_ptr()->append_datum(generator(rid, cid, row_block_id));
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

// The per-column page size has to survive the whole way from the table property to the
// column writer: the page is what a point lookup decompresses, and a schema that loses
// the size on the way silently writes the column at the BE default instead. Assert on
// what the segment actually contains rather than on the schema that produced it.
TEST_F(SegmentReaderWriterTest, per_column_zstd_page_size_changes_the_pages_written) {
    constexpr size_t kRows = 2000;
    constexpr size_t kValueBytes = 1024;

    auto make_schema = [](uint32_t page_size) {
        TabletSchemaPB pb;
        pb.set_keys_type(DUP_KEYS);
        pb.set_num_short_key_columns(1);
        auto* k = pb.add_column();
        k->set_unique_id(1);
        k->set_name("k");
        k->set_type("INT");
        k->set_is_key(true);
        k->set_is_nullable(false);
        k->set_length(4);
        k->set_index_length(4);
        k->set_aggregation("NONE");
        auto* v = pb.add_column();
        v->set_unique_id(2);
        v->set_name("v");
        v->set_type("VARCHAR");
        v->set_is_key(false);
        v->set_is_nullable(false);
        v->set_length(65535);
        v->set_aggregation("NONE");
        v->set_use_zstd_compression(true);
        v->set_zstd_compression_page_size(page_size);
        return TabletSchema::create(pb);
    };

    // values are unique and incompressible enough not to be dictionary-encoded, so the
    // page size is what decides how many pages the column takes.
    auto write = [&](const TabletSchemaCSPtr& schema, int32_t* value_pages) {
        static int seg_id = 0;
        std::string filename = strings::Substitute("$0/page_size_$1.dat", kSegmentDir, seg_id++);
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(filename));
        SegmentWriterOptions opts;
        SegmentWriter writer(std::move(wfile), 0, schema, opts);
        ASSERT_OK(writer.init());

        auto chunk_schema = ChunkHelper::convert_schema(schema);
        auto chunk = ChunkFactory::new_chunk(chunk_schema, kRows);
        for (size_t rid = 0; rid < kRows; ++rid) {
            // every value must be distinct, or the writer speculates DICT encoding and the
            // column collapses into a single data page whatever the page size is.
            std::string value(kValueBytes, ' ');
            uint64_t state = rid * 6364136223846793005ULL + 1442695040888963407ULL;
            for (size_t i = 0; i < kValueBytes; ++i) {
                state = state * 6364136223846793005ULL + 1442695040888963407ULL;
                value[i] = static_cast<char>('a' + ((state >> 33) % 26));
            }
            auto cols = chunk->columns();
            cols[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(rid)));
            cols[1]->as_mutable_ptr()->append_datum(Datum(Slice(value)));
        }
        ASSERT_OK(writer.append_chunk(*chunk));
        uint64_t file_size, index_size, footer_position;
        ASSERT_OK(writer.finalize(&file_size, &index_size, &footer_position));

        ASSIGN_OR_ABORT(auto segment, Segment::open(_fs, FileInfo{filename}, 0, schema));
        ASSERT_EQ(kRows, segment->num_rows());
        ASSIGN_OR_ABORT(auto iter, segment->new_column_iterator(schema->column(1), nullptr));
        ColumnIteratorOptions iter_opts;
        ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(filename));
        OlapReaderStatistics stats;
        iter_opts.stats = &stats;
        iter_opts.read_file = rfile.get();
        ASSERT_OK(iter->init(iter_opts));
        *value_pages = segment->column(1)->num_data_pages();
    };

    int32_t default_pages = 0;
    int32_t big_page_pages = 0;
    write(make_schema(0), &default_pages);
    write(make_schema(1024 * 1024), &big_page_pages);

    // ~2 MB of values: roughly 32 pages at the 64 KB default, a couple at 1 MB.
    ASSERT_GT(default_pages, 8);
    ASSERT_LT(big_page_pages, default_pages / 4);
}

TEST_F(SegmentReaderWriterTest, nullable_geo_plain_v2_scan_and_rowid_fallback) {
    constexpr size_t kPageTarget = 4096;
    // ISO WKB LINESTRING EMPTY and a linestring larger than the page target, both little-endian.
    std::string empty_line(1, '\1');
    put_fixed32_le(&empty_line, 2);
    put_fixed32_le(&empty_line, 0);
    std::string long_line(1, '\1');
    put_fixed32_le(&long_line, 2);
    put_fixed32_le(&long_line, 256);
    long_line.resize(9 + 256 * 2 * sizeof(double), '\0');
    std::string point(21, '\0'); // Big-endian POINT (1 0).
    point[4] = 1;
    point[5] = 0x3f;
    point[6] = static_cast<char>(0xf0);
    const GeoTypeDescriptor descriptors[] = {
            {GEO_LOGICAL_TYPE_GEOGRAPHY, GEO_EDGE_ALGORITHM_SPHERICAL, "OGC:CRS84", std::nullopt},
            {GEO_LOGICAL_TYPE_GEOMETRY, GEO_EDGE_ALGORITHM_PLANAR, "EPSG:3857", 3857},
            {GEO_LOGICAL_TYPE_GEOMETRY, GEO_EDGE_ALGORITHM_PLANAR, "custom:LocalSurveyGrid-Moscow-2026", std::nullopt},
    };
    for (const auto& descriptor : descriptors) {
        SCOPED_TRACE(descriptor.crs);
        const auto type = descriptor.logical_type == GEO_LOGICAL_TYPE_GEOGRAPHY ? TYPE_GEOGRAPHY : TYPE_GEOMETRY;
        auto tablet = std::make_shared<TabletSchema>();
        TabletColumn key(STORAGE_AGGREGATE_NONE, TYPE_INT, false, 0, sizeof(int32_t));
        key.set_name("key");
        key.set_is_key(true);
        tablet->append_column(key);
        TabletColumn value(STORAGE_AGGREGATE_NONE, type, true, 1, 0);
        value.set_name("geo");
        value.set_geo_descriptor(descriptor);
        value.set_zstd_compression_page_size(kPageTarget);
        tablet->append_column(value);

        const auto schema = ChunkHelper::convert_schema(tablet);
        auto src = ChunkFactory::new_chunk(schema, 4);
        const Slice input[] = {point, empty_line, long_line, point};
        const Slice expected[] = {Slice(""), empty_line, long_line, point};
        auto* src_geo = down_cast<NullableColumn*>(src->get_column_raw_ptr_by_index(1));
        for (size_t i = 0; i < std::size(input); ++i) {
            src->get_column_raw_ptr_by_index(0)->append_datum(Datum(static_cast<int32_t>(i)));
            src_geo->append_datum(Datum(input[i]));
        }
        ASSERT_TRUE(src_geo->set_null(0));
        ASSERT_EQ(point, down_cast<const GeoColumn*>(src_geo->data_column_raw_ptr())->get_wkb(0).to_string());

        const auto filename = kSegmentDir + "/geo_plain_" + descriptor.crs;
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(filename));
        SegmentWriter writer(std::move(wfile), 0, tablet, {});
        ASSERT_OK(writer.init());
        ASSERT_OK(writer.append_chunk(*src));
        uint64_t file_size, index_size, footer_position;
        ASSERT_OK(writer.finalize(&file_size, &index_size, &footer_position));

        ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(filename));
        SegmentFooterPB footer;
        ASSERT_TRUE(Segment::parse_segment_footer(rfile.get(), &footer, nullptr, nullptr).ok());
        ASSERT_EQ(type, footer.columns(1).type());
        ASSERT_EQ(PLAIN_ENCODING, footer.columns(1).encoding());

        auto query_tablet = std::make_shared<TabletSchema>();
        query_tablet->append_column(key);
        value.set_geo_descriptor(descriptor);
        query_tablet->append_column(value);
        ASSIGN_OR_ABORT(auto segment, Segment::open(_fs, FileInfo{filename}, 0, query_tablet));
        OlapReaderStatistics stats;
        SegmentReadOptions opts;
        opts.fs = _fs;
        opts.stats = &stats;
        opts.chunk_size = std::size(input);
        opts.use_page_cache = false;
        const auto query_schema = ChunkHelper::convert_schema(query_tablet);
        ASSIGN_OR_ABORT(auto scan, segment->new_iterator(query_schema, opts));
        auto dst = ChunkFactory::new_chunk(query_schema, std::size(input));
        ASSERT_OK(scan->get_next(dst.get()));
        ASSERT_EQ(std::size(input), dst->num_rows());
        const auto* nullable = down_cast<const NullableColumn*>(dst->get_column_raw_ptr_by_index(1));
        const auto* geo = down_cast<const GeoColumn*>(nullable->data_column_raw_ptr());
        ASSERT_EQ(descriptor, geo->descriptor().type);
        ASSERT_EQ(dst->num_rows(), geo->size());
        ASSERT_EQ(geo->size(), nullable->null_column_data().size());
        ASSERT_TRUE(nullable->has_null());
        for (size_t i = 0; i < std::size(input); ++i) {
            EXPECT_EQ(i, dst->get_column_raw_ptr_by_index(0)->get(i).get_int32());
            EXPECT_EQ(i == 0, nullable->null_column_data()[i]);
            EXPECT_EQ(expected[i], geo->get_wkb(i));
        }

        ASSIGN_OR_ABORT(auto col, segment->new_column_iterator(query_tablet->column(1), nullptr));
        ColumnIteratorOptions col_opts;
        col_opts.stats = &stats;
        col_opts.read_file = rfile.get();
        col_opts.use_page_cache = false;
        ASSERT_OK(col->init(col_opts));
        ASSERT_EQ(2, segment->column(1)->num_data_pages());
        for (rowid_t ordinal : {0u, 3u}) {
            ASSERT_OK(col->seek_to_ordinal(ordinal));
            auto* page = down_cast<ScalarColumnIterator*>(col.get())->get_current_page();
            PageHandle handle;
            Slice body;
            PageFooterPB page_footer;
            ASSERT_OK(
                    col->get_column_reader()->read_page(col_opts, page->page_pointer(), &handle, &body, &page_footer));
            const auto& data_footer = page_footer.data_page_footer();
            ASSERT_EQ(2, data_footer.format_version());
            ASSERT_EQ(ordinal == 0, data_footer.nullmap_size() != 0);
            if (ordinal == 0) {
                BinaryPlainPageDecoder<TYPE_GEOMETRY> decoder(Slice(body.data, body.size - data_footer.nullmap_size()));
                ASSERT_OK(decoder.init());
                EXPECT_EQ(0, decoder.string_at_index(0).size);
                EXPECT_EQ(Slice(empty_line), decoder.string_at_index(1));
            }
        }

        const rowid_t rowids[] = {0, 1, 3};
        auto selected = nullable->clone_empty();
        ASSERT_OK(col->fetch_values_by_rowid(rowids, std::size(rowids), selected.get()));
        const auto* selected_null = down_cast<const NullableColumn*>(selected.get());
        const auto* selected_geo = down_cast<const GeoColumn*>(selected_null->data_column_raw_ptr());
        ASSERT_EQ(std::size(rowids), selected_geo->size());
        ASSERT_EQ(selected_geo->size(), selected_null->null_column_data().size());
        EXPECT_EQ(descriptor, selected_geo->descriptor().type);
        for (size_t i = 0; i < std::size(rowids); ++i) {
            EXPECT_EQ(expected[rowids[i]], selected_geo->get_wkb(i));
            EXPECT_EQ(rowids[i] == 0, selected_null->null_column_data()[i]);
        }

        auto wrong_crs = descriptor;
        wrong_crs.crs += "-other";
        auto wrong_srid = descriptor;
        wrong_srid.srid = descriptor.srid.has_value() ? std::nullopt : std::optional<int32_t>(4326);
        for (const auto& wrong : {wrong_crs, wrong_srid}) {
            auto wrong_dst =
                    NullableColumn::create(GeoColumn::create(GeoColumnDescriptor{wrong}), NullColumn::create());
            wrong_dst->append_datum(Datum(Slice(point)));
            ASSERT_OK(col->seek_to_first());
            EXPECT_TRUE(col->next_batch(SparseRange<>(0, 2), wrong_dst.get()).is_invalid_argument());
            size_t count = 2;
            EXPECT_TRUE(col->next_batch(&count, wrong_dst.get()).is_invalid_argument());
            EXPECT_TRUE(col->fetch_values_by_rowid(rowids, std::size(rowids), wrong_dst.get()).is_invalid_argument());
            EXPECT_EQ(1, wrong_dst->size());
            EXPECT_EQ(1, wrong_dst->null_column_data().size());
            EXPECT_FALSE(wrong_dst->is_null(0));
            EXPECT_EQ(Slice(point), wrong_dst->get(0).get_slice());
        }
    }
}

TEST_F(SegmentReaderWriterTest, nullable_geo_plain_adaptive_page_versions) {
    constexpr size_t kPageTarget = 4096;
    constexpr rowid_t kV1First = 10;
    constexpr rowid_t kV1Nulls = 8;
    constexpr rowid_t kV1Values = 8;
    constexpr rowid_t kLast = kV1First + kV1Nulls + kV1Values;
    std::string empty_line(1, '\1');
    put_fixed32_le(&empty_line, 2);
    put_fixed32_le(&empty_line, 0);
    std::string long_line(1, '\1');
    put_fixed32_le(&long_line, 2);
    put_fixed32_le(&long_line, 256);
    long_line.resize(empty_line.size() + 256 * 2 * sizeof(double), '\0');
    std::string point(21, '\0'); // Big-endian POINT (1 0).
    point[4] = 1;
    point[5] = 0x3f;
    point[6] = static_cast<char>(0xf0);

    // The long WKB closes each of the first two pages. The non-NULL run that
    // starts with EMPTY crosses from v1 to v2 at kLast.
    std::vector<Slice> expected(kLast + 1, Slice(point));
    std::fill_n(expected.begin(), kV1First - 1, Slice(""));
    expected[kV1First - 1] = long_line;
    std::fill_n(expected.begin() + kV1First, kV1Nulls, Slice(""));
    expected[kV1First + kV1Nulls] = empty_line;
    expected[kLast - 1] = long_line;

    const GeoTypeDescriptor descriptors[] = {
            {GEO_LOGICAL_TYPE_GEOGRAPHY, GEO_EDGE_ALGORITHM_SPHERICAL, "OGC:CRS84", std::nullopt},
            {GEO_LOGICAL_TYPE_GEOMETRY, GEO_EDGE_ALGORITHM_PLANAR, "EPSG:3857", 3857},
    };
    for (const auto& descriptor : descriptors) {
        SCOPED_TRACE(descriptor.crs);
        const auto type = descriptor.logical_type == GEO_LOGICAL_TYPE_GEOGRAPHY ? TYPE_GEOGRAPHY : TYPE_GEOMETRY;
        auto tablet = std::make_shared<TabletSchema>();
        TabletColumn key(STORAGE_AGGREGATE_NONE, TYPE_INT, false, 0, sizeof(int32_t));
        key.set_name("key");
        key.set_is_key(true);
        tablet->append_column(key);
        TabletColumn value(STORAGE_AGGREGATE_NONE, type, true, 1, 0);
        value.set_name("geo");
        value.set_geo_descriptor(descriptor);
        value.set_zstd_compression_page_size(kPageTarget);
        tablet->append_column(value);
        const auto schema = ChunkHelper::convert_schema(tablet);
        auto src = ChunkFactory::new_chunk(schema, expected.size());
        auto* src_geo = down_cast<NullableColumn*>(src->get_column_raw_ptr_by_index(1));
        std::vector<rowid_t> all_rows;
        for (rowid_t i = 0; i < expected.size(); ++i) {
            all_rows.push_back(i);
            src->get_column_raw_ptr_by_index(0)->append_datum(Datum(static_cast<int32_t>(i)));
            src_geo->append_datum(Datum(expected[i].empty() ? Slice(point) : expected[i]));
            if (expected[i].empty()) {
                ASSERT_TRUE(src_geo->set_null(i));
            }
        }

        const auto filename = kSegmentDir + "/geo_adaptive_" + descriptor.crs;
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(filename));
        SegmentWriter writer(std::move(wfile), 0, tablet, {});
        ASSERT_OK(writer.init());
        ASSERT_OK(writer.append_chunk(*src));
        uint64_t file_size, index_size, footer_position;
        ASSERT_OK(writer.finalize(&file_size, &index_size, &footer_position));

        ASSIGN_OR_ABORT(auto segment, Segment::open(_fs, FileInfo{filename}, 0, tablet));
        ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(filename));
        const auto* reader_descriptor = segment->column(1)->geo_descriptor();
        ASSERT_NE(nullptr, reader_descriptor);
        EXPECT_EQ(descriptor, *reader_descriptor);
        EXPECT_NE(tablet->column(1).geo_descriptor(), reader_descriptor);

        SegmentFooterPB segment_footer;
        ASSERT_TRUE(Segment::parse_segment_footer(rfile.get(), &segment_footer, nullptr, nullptr).ok());
        TabletColumn missing_descriptor(STORAGE_AGGREGATE_NONE, type, true, 1, 0);
        const TabletColumn* incomplete_schema[] = {nullptr, &missing_descriptor};
        for (const auto* column : incomplete_schema) {
            auto meta = segment_footer.columns(1);
            ASSERT_TRUE(ColumnReader::create(&meta, segment.get(), column).status().is_invalid_argument());
        }
        auto int_meta = segment_footer.columns(0);
        ASSIGN_OR_ABORT(auto int_reader, ColumnReader::create(&int_meta, segment.get(), &value));
        EXPECT_EQ(nullptr, int_reader->geo_descriptor());

        OlapReaderStatistics stats;
        ColumnIteratorOptions col_opts;
        col_opts.stats = &stats;
        col_opts.read_file = rfile.get();
        col_opts.use_page_cache = false;
        ASSIGN_OR_ABORT(auto col, segment->new_column_iterator(tablet->column(1), nullptr));
        ASSERT_OK(col->init(col_opts));
        ASSERT_EQ(3, segment->column(1)->num_data_pages());
        const rowid_t starts[] = {0, kV1First, kLast};
        const size_t counts[] = {kV1First, kV1Nulls + kV1Values, 1};
        for (size_t i = 0; i < std::size(starts); ++i) {
            ASSERT_OK(col->seek_to_ordinal(starts[i]));
            auto* page = down_cast<ScalarColumnIterator*>(col.get())->get_current_page();
            PageHandle handle;
            Slice body;
            PageFooterPB footer;
            ASSERT_OK(col->get_column_reader()->read_page(col_opts, page->page_pointer(), &handle, &body, &footer));
            const auto& data = footer.data_page_footer();
            ASSERT_EQ(i == 1 ? 1 : 2, data.format_version());
            ASSERT_EQ(starts[i], data.first_ordinal());
            ASSERT_EQ(counts[i], data.num_values());
            ASSERT_EQ(i < 2, data.nullmap_size() != 0);
            if (i == 1) {
                const size_t data_size = body.size - data.nullmap_size();
                BinaryPlainPageDecoder<TYPE_GEOMETRY> decoder(Slice(body.data, data_size));
                ASSERT_OK(decoder.init());
                ASSERT_EQ(kV1Values, decoder.count());
                RleDecoder<bool> flags(reinterpret_cast<const uint8_t*>(body.data + data_size), data.nullmap_size(), 1);
                // Read the full runs, including any flags beyond the logical row count.
                bool is_null = false;
                EXPECT_EQ(kV1Nulls, flags.GetNextRun(&is_null, data.num_values() + 1));
                EXPECT_TRUE(is_null);
                EXPECT_EQ(decoder.count(), flags.GetNextRun(&is_null, data.num_values() + 1));
                EXPECT_FALSE(is_null);
                EXPECT_EQ(0, flags.GetNextRun(&is_null, 1));
                EXPECT_EQ(Slice(empty_line), decoder.string_at_index(0));
            }
        }

        const auto check_values = [&](const Column* column, const std::vector<rowid_t>& rowids) {
            const auto* nullable = down_cast<const NullableColumn*>(column);
            const auto* geo = down_cast<const GeoColumn*>(nullable->data_column_raw_ptr());
            ASSERT_EQ(descriptor, geo->descriptor().type);
            ASSERT_EQ(rowids.size(), geo->size());
            ASSERT_EQ(geo->size(), nullable->null_column_data().size());
            for (size_t i = 0; i < rowids.size(); ++i) {
                EXPECT_EQ(expected[rowids[i]], geo->get_wkb(i)) << rowids[i];
                EXPECT_EQ(expected[rowids[i]].empty(), nullable->null_column_data()[i]) << rowids[i];
            }
        };

        SegmentReadOptions opts;
        opts.fs = _fs;
        opts.stats = &stats;
        opts.chunk_size = expected.size();
        opts.use_page_cache = false;
        ASSIGN_OR_ABORT(auto scan, segment->new_iterator(schema, opts));
        auto dst = ChunkFactory::new_chunk(schema, expected.size());
        ASSERT_OK(scan->get_next(dst.get()));
        ASSERT_EQ(expected.size(), dst->num_rows());
        for (rowid_t row : all_rows) {
            EXPECT_EQ(row, dst->get_column_raw_ptr_by_index(0)->get(row).get_int32());
        }
        check_values(dst->get_column_raw_ptr_by_index(1), all_rows);

        const std::vector<rowid_t> rowids = {
                0, kV1First - 1, kV1First + kV1Nulls - 1, kV1First + kV1Nulls, kLast - 2, kLast - 1, kLast};
        auto selected = src_geo->clone_empty();
        ASSERT_OK(col->fetch_values_by_rowid(rowids.data(), rowids.size(), selected.get()));
        check_values(selected.get(), rowids);

        auto wrong_crs = descriptor;
        wrong_crs.crs += "-other";
        auto wrong_srid = descriptor;
        wrong_srid.srid = descriptor.srid.has_value() ? std::nullopt : std::optional<int32_t>(4326);
        for (const auto& wrong : {wrong_crs, wrong_srid}) {
            auto rejected = NullableColumn::create(GeoColumn::create(GeoColumnDescriptor{wrong}), NullColumn::create());
            rejected->append_datum(Datum(Slice(point)));
            ASSERT_OK(col->seek_to_ordinal(kV1First));
            EXPECT_TRUE(col->next_batch(SparseRange<>(kV1First, kV1First + 2), rejected.get()).is_invalid_argument());
            EXPECT_TRUE(col->fetch_values_by_rowid(rowids.data(), rowids.size(), rejected.get()).is_invalid_argument());
            ASSERT_EQ(1, rejected->size());
            ASSERT_EQ(1, rejected->null_column_data().size());
            EXPECT_FALSE(rejected->is_null(0));
            EXPECT_EQ(Slice(point), rejected->get(0).get_slice());
        }
    }
}

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
    auto chunk = ChunkFactory::new_chunk(schema, nrows);
    for (size_t rid = 0; rid < nrows; ++rid) {
        auto cols = chunk->columns();
        for (int cid = 0; cid < tablet_schema->num_columns(); ++cid) {
            cols[cid]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(rid * 10 + cid)));
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
    auto chunk = ChunkFactory::new_chunk(schema, chunk_size);
    for (auto i = 0; i < num_rows % chunk_size; ++i) {
        chunk->reset();
        auto cols = chunk->columns();
        for (auto j = 0; j < chunk_size && i * chunk_size + j < num_rows; ++j) {
            cols[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j)));
            cols[1]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j + 1)));
            cols[2]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j + 2)));
            cols[3]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j + 3)));
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
    const auto& seg_iterator = res.value();

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
        TabletColumn column;
        column.set_unique_id(5);
        column.set_type(LogicalType::TYPE_BIGINT);
        column.set_is_nullable(false);
        auto r = segment->new_column_iterator(column, nullptr);
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
    // test new_dcg_segment
    {
        auto ep = KeyCache::instance().create_plain_random_encryption_meta_pair().value();
        DeltaColumnGroup dcg;
        dcg.init(1, {{1}}, {"abc0.cols"}, {ep.encryption_meta});
        auto r = segment->new_dcg_segment(dcg, 0, nullptr);
        ASSERT_FALSE(r.ok());
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
        auto chunk = ChunkFactory::new_chunk(schema, chunk_size);
        for (auto i = 0; i < num_rows % chunk_size; ++i) {
            chunk->reset();
            auto cols = chunk->columns();
            for (auto j = 0; j < chunk_size && i * chunk_size + j < num_rows; ++j) {
                cols[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j)));
                cols[1]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j + 1)));
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
        auto chunk = ChunkFactory::new_chunk(schema, chunk_size);
        for (auto i = 0; i < num_rows % chunk_size; ++i) {
            chunk->reset();
            auto cols = chunk->columns();
            for (auto j = 0; j < chunk_size && i * chunk_size + j < num_rows; ++j) {
                cols[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j + 2)));
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
        auto chunk = ChunkFactory::new_chunk(schema, chunk_size);
        for (auto i = 0; i < num_rows % chunk_size; ++i) {
            chunk->reset();
            auto cols = chunk->columns();
            for (auto j = 0; j < chunk_size && i * chunk_size + j < num_rows; ++j) {
                cols[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j + 3)));
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
    const auto& seg_iterator = res.value();

    size_t count = 0;
    auto chunk = ChunkFactory::new_chunk(schema, chunk_size);
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
        auto chunk = ChunkFactory::new_chunk(schema, chunk_size);
        for (auto i = 0; i < num_rows % chunk_size; ++i) {
            chunk->reset();
            auto cols = chunk->columns();
            for (auto j = 0; j < chunk_size && i * chunk_size + j < num_rows; ++j) {
                cols[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j)));
                cols[1]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j + 1)));
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
        auto chunk = ChunkFactory::new_chunk(schema, chunk_size);
        for (auto i = 0; i < num_rows % chunk_size; ++i) {
            chunk->reset();
            auto cols = chunk->columns();
            for (auto j = 0; j < chunk_size && i * chunk_size + j < num_rows; ++j) {
                cols[0]->as_mutable_ptr()->append_datum(Datum(data_strs[j % 8]));
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
    const auto& seg_iterator = res.value();

    size_t count = 0;
    auto chunk = ChunkFactory::new_chunk(schema, chunk_size);
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

TEST_F(SegmentReaderWriterTest, TestTypeConversion) {
    auto tablet_schema = std::shared_ptr<TabletSchema>{
            TabletSchemaHelper::create_tablet_schema({create_int_key_pb(0), create_int_value_pb(1)})};
    auto opts = SegmentWriterOptions{};
    opts.num_rows_per_block = 10;
    auto file_name = kSegmentDir + "/type_conversion_cast";
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file_name));

    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);
    ASSERT_OK(writer.init());

    auto chunk_size = std::max<int32_t>(10, config::vector_chunk_size);
    auto num_rows = chunk_size * 2;
    auto write_schema = ChunkHelper::convert_schema(tablet_schema);
    auto chunk = ChunkFactory::new_chunk(write_schema, chunk_size);
    for (auto i = 0; i < num_rows / chunk_size; ++i) {
        chunk->reset();
        auto cols = chunk->columns();
        for (auto j = 0; j < chunk_size; ++j) {
            cols[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j)));
            cols[1]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j + 1)));
        }
        ASSERT_OK(writer.append_chunk(*chunk));
    }

    auto file_size = uint64_t{0};
    auto index_size = uint64_t{0};
    auto footer_position = uint64_t{0};
    ASSERT_OK(writer.finalize(&file_size, &index_size, &footer_position));

    auto segment = *Segment::open(_fs, FileInfo{file_name}, 0, tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);

    auto tablet_schema_for_read = std::shared_ptr<TabletSchema>{
            TabletSchemaHelper::create_tablet_schema({create_int_key_pb(0), create_bigint_value_pb(1)})};
    auto read_schema = ChunkHelper::convert_schema(tablet_schema_for_read);
    // full scan
    {
        auto count = 0;
        auto c1_type_info = get_type_info(LogicalType::TYPE_BIGINT);
        auto stats = OlapReaderStatistics{};
        auto seg_options = SegmentReadOptions{};
        auto read_chunk = ChunkFactory::new_chunk(read_schema, chunk_size);
        seg_options.fs = _fs;
        seg_options.stats = &stats;
        seg_options.tablet_schema = tablet_schema_for_read;
        ASSIGN_OR_ABORT(auto seg_iter, segment->new_iterator(read_schema, seg_options));
        while (true) {
            read_chunk->reset();
            auto st = seg_iter->get_next(read_chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            ASSERT_OK(st);
            for (auto i = 0; i < read_chunk->num_rows(); ++i) {
                EXPECT_EQ(count, read_chunk->get(i)[0].get_int32());
                EXPECT_EQ(count + 1, read_chunk->get(i)[1].get_int64());
                ++count;
            }
        }
        EXPECT_EQ(count, num_rows);
    }
    // With predicate
    {
        auto c1_type_info = get_type_info(LogicalType::TYPE_BIGINT);
        auto predicate = new_column_eq_predicate(c1_type_info, 1, "10");
        auto guard = std::unique_ptr<ColumnPredicate>{predicate};
        auto pred_root = PredicateAndNode{};
        pred_root.add_child(PredicateColumnNode{predicate});
        auto stats = OlapReaderStatistics{};
        auto seg_options = SegmentReadOptions{};
        auto read_chunk = ChunkFactory::new_chunk(read_schema, chunk_size);
        seg_options.fs = _fs;
        seg_options.stats = &stats;
        seg_options.tablet_schema = tablet_schema_for_read;
        seg_options.pred_tree = PredicateTree::create(std::move(pred_root));
        seg_options.pred_tree_for_zone_map = seg_options.pred_tree;
        ASSIGN_OR_ABORT(auto seg_iter, segment->new_iterator(read_schema, seg_options));
        ASSERT_OK(seg_iter->get_next(read_chunk.get()));
        EXPECT_EQ(1, read_chunk->num_rows());
        EXPECT_EQ(9, read_chunk->get(0)[0].get_int32());
        EXPECT_EQ(10, read_chunk->get(0)[1].get_int64());
    }
}

// A Segment builds its column readers from the schema it was constructed with, so a column that the
// construction schema omits is unreachable even when the footer carries it: the read falls back to
// the column's default value with no error. This is why any cache shared between readers must only
// ever hold Segments built from the full tablet schema -- see the `allow_segment_cache` argument in
// lake::UpdateManager::get_column_values.
TEST_F(SegmentReaderWriterTest, TestNarrowConstructionSchemaHidesColumnPresentInFooter) {
    auto full_schema = std::shared_ptr<TabletSchema>{TabletSchemaHelper::create_tablet_schema(
            {create_int_key_pb(0), create_int_value_pb(1),
             create_int_value_pb(2, "SUM", /*is_nullable=*/true, /*default_value=*/"77")})};
    // Same file, opened through a schema that stops at column 1.
    auto narrow_schema = std::shared_ptr<TabletSchema>{
            TabletSchemaHelper::create_tablet_schema({create_int_key_pb(0), create_int_value_pb(1)})};

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 10;
    constexpr size_t kNumRows = 20;

    // DefaultIntGenerator writes column c of row r as r * 10 + c, so column 2 holds 2, 12, 22 -- well
    // away from the default value the read must not fall back to.
    std::shared_ptr<Segment> narrow_segment;
    build_segment(opts, full_schema, narrow_schema, kNumRows, DefaultIntGenerator, &narrow_segment);

    ColumnIteratorOptions iter_opts;
    OlapReaderStatistics stats;
    iter_opts.stats = &stats;
    ASSIGN_OR_ABORT(auto read_file, _fs->new_random_access_file(narrow_segment->file_name()));
    iter_opts.read_file = read_file.get();
    const std::vector<rowid_t> rowids{0, 1, 2};

    auto read_column_2 = [&](Segment* segment) {
        auto read_schema = ChunkHelper::convert_schema(full_schema, std::vector<ColumnId>{2});
        auto values = ChunkFactory::column_from_field(*read_schema.field(0).get())->clone_empty();
        auto iter = segment->new_column_iterator_or_default(full_schema->column(2), nullptr);
        CHECK(iter.ok()) << iter.status();
        CHECK(iter.value()->init(iter_opts).ok());
        CHECK(iter.value()->fetch_values_by_rowid(rowids.data(), rowids.size(), values.get()).ok());
        std::vector<int32_t> out;
        for (size_t i = 0; i < values->size(); ++i) {
            out.push_back(values->get(i).get_int32());
        }
        return out;
    };

    // Column 1 is in the construction schema, so it reads through to the stored data.
    {
        auto read_schema = ChunkHelper::convert_schema(full_schema, std::vector<ColumnId>{1});
        auto values = ChunkFactory::column_from_field(*read_schema.field(0).get())->clone_empty();
        ASSIGN_OR_ABORT(auto iter, narrow_segment->new_column_iterator_or_default(full_schema->column(1), nullptr));
        ASSERT_OK(iter->init(iter_opts));
        ASSERT_OK(iter->fetch_values_by_rowid(rowids.data(), rowids.size(), values.get()));
        ASSERT_EQ(rowids.size(), values->size());
        for (size_t i = 0; i < rowids.size(); ++i) {
            EXPECT_EQ(static_cast<int32_t>(rowids[i] * 10 + 1), values->get(i).get_int32());
        }
    }

    // Column 2 is not, so the same file silently yields its default value instead of 2, 12, 22.
    EXPECT_EQ(std::vector<int32_t>({77, 77, 77}), read_column_2(narrow_segment.get()));

    // Opened through the full schema the same file returns the stored values.
    ASSIGN_OR_ABORT(auto full_segment, Segment::open(_fs, FileInfo{narrow_segment->file_name()}, 0, full_schema));
    EXPECT_EQ(std::vector<int32_t>({2, 12, 22}), read_column_2(full_segment.get()));
}

TEST_F(SegmentReaderWriterTest, TestCheckColumnUniqueIdUniqueness) {
    std::shared_ptr<TabletSchema> tablet_schema = TabletSchemaHelper::create_tablet_schema();

    SegmentWriterOptions opts;
    shared_ptr<Segment> res;

    PFailPointTriggerMode trigger_mode;
    trigger_mode.set_mode(FailPointTriggerModeType::ENABLE);
    auto fp = starrocks::failpoint::FailPointRegistry::GetInstance()->get("ingest_duplicate_column_unique_id");
    fp->setMode(trigger_mode);
    // The failpoint is process-global, so turn it back off on every exit path: left enabled, it makes
    // every later Segment::open() in the test binary fail with "Duplicate column id found in tablet
    // schema".
    DeferOp disable_fp([&]() {
        PFailPointTriggerMode disable_mode;
        disable_mode.set_mode(FailPointTriggerModeType::DISABLE);
        fp->setMode(disable_mode);
    });

    static int seg_id = 0;
    std::string filename = strings::Substitute("$0/seg_$1.dat", kSegmentDir, seg_id++);
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(filename));
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, opts);
    ASSERT_OK(writer.init());

    auto schema = ChunkHelper::convert_schema(tablet_schema);
    auto chunk = ChunkFactory::new_chunk(schema, 100);
    for (size_t rid = 0; rid < 100; ++rid) {
        auto cols = chunk->columns();
        for (int cid = 0; cid < tablet_schema->num_columns(); ++cid) {
            int row_block_id = rid / opts.num_rows_per_block;
            cols[cid]->as_mutable_ptr()->append_datum(DefaultIntGenerator(rid, cid, row_block_id));
        }
    }
    ASSERT_OK(writer.append_chunk(*chunk));

    uint64_t file_size, index_size, footer_position;
    ASSERT_OK(writer.finalize(&file_size, &index_size, &footer_position));
    auto result = Segment::open(_fs, FileInfo{filename}, 0, tablet_schema);
    ASSERT_TRUE(!result.ok());

    const auto& st = result.status();
    EXPECT_EQ(st.code(), TStatusCode::INTERNAL_ERROR)
            << "Expected InternalError, got: " << st.code() << ", message: " << st.message();
    EXPECT_EQ(st.message(), "Duplicate column id found in tablet schema")
            << "Error message should indicate duplicate column id in tablet schema.";
}

} // namespace starrocks
