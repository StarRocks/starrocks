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

#include <gtest/gtest.h>

#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "common/object_pool.h"
#include "common/util/thrift_util.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/scalar_column_reader.h"
#include "formats/parquet/stored_column_reader_with_index.h"
#include "fs/fs.h"
#include "io/string_input_stream.h"
#include "storage_primitive/column_predicate_factory.h"
#include "types/type_descriptor.h"

namespace starrocks::parquet {
namespace {

std::string plain_int32(int32_t v) {
    std::string s(sizeof(v), '\0');
    memcpy(s.data(), &v, sizeof(v));
    return s;
}

template <typename T>
std::string serialize_compact(T* obj) {
    ThriftSerializer serializer(true, 64);
    std::vector<uint8_t> bytes;
    EXPECT_TRUE(serializer.serialize(obj, &bytes).ok());
    return {bytes.begin(), bytes.end()};
}

// Two pages: [1, 1] for rows [0, 50) and [10, 10] for rows [50, 100).
tparquet::ColumnIndex two_page_column_index() {
    tparquet::ColumnIndex index;
    index.__set_null_pages({false, false});
    index.__set_min_values({plain_int32(1), plain_int32(10)});
    index.__set_max_values({plain_int32(1), plain_int32(10)});
    index.__set_boundary_order(tparquet::BoundaryOrder::UNORDERED);
    index.__set_null_counts({0, 0});
    return index;
}

tparquet::OffsetIndex offset_index_of(const std::vector<int64_t>& first_rows) {
    tparquet::OffsetIndex index;
    std::vector<tparquet::PageLocation> locations;
    for (size_t i = 0; i < first_rows.size(); i++) {
        tparquet::PageLocation location;
        location.__set_offset(4 + 100 * i);
        location.__set_compressed_page_size(100);
        location.__set_first_row_index(first_rows[i]);
        locations.push_back(location);
    }
    index.__set_page_locations(locations);
    return index;
}

class PageIndexFailedReadTest : public ::testing::Test {
protected:
    void SetUp() override {
        tparquet::SchemaElement root;
        root.__set_name("root");
        root.__set_num_children(1);
        tparquet::SchemaElement col;
        col.__set_name("c");
        col.__set_type(tparquet::Type::INT32);
        col.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
        tparquet::FileMetaData thrift;
        thrift.__set_schema({root, col});
        ASSERT_TRUE(_metadata.init(thrift, true).ok());
        _field = _metadata.schema().get_stored_column_by_field_idx(0);
        ASSERT_NE(nullptr, _field);

        tparquet::ColumnMetaData column;
        column.__set_type(tparquet::Type::INT32);
        column.__set_num_values(100);
        column.__set_data_page_offset(4);
        _chunk.__set_meta_data(column);

        _predicate = _pool.add(new_column_eq_predicate_from_datum(get_type_info(TYPE_INT), 1, Datum(int32_t(10))));
        ASSERT_NE(nullptr, _predicate);
    }

    // Lays out the ColumnIndex at offset 0 and the OffsetIndex right after it. With `offset_index` null the
    // OffsetIndex points past the end of the file, so reading it fails.
    void build_file(tparquet::OffsetIndex* offset_index) {
        auto column_index = two_page_column_index();
        std::string bytes = serialize_compact(&column_index);
        _chunk.__set_column_index_offset(0);
        _chunk.__set_column_index_length(static_cast<int32_t>(bytes.size()));
        if (offset_index != nullptr) {
            std::string offset_bytes = serialize_compact(offset_index);
            _chunk.__set_offset_index_offset(static_cast<int64_t>(bytes.size()));
            _chunk.__set_offset_index_length(static_cast<int32_t>(offset_bytes.size()));
            bytes += offset_bytes;
        } else {
            _chunk.__set_offset_index_offset(static_cast<int64_t>(bytes.size()) + 4096);
            _chunk.__set_offset_index_length(64);
        }
        _file = std::make_unique<RandomAccessFile>(std::make_shared<io::StringInputStream>(std::move(bytes)),
                                                   "page-index.parquet");
        _opts.file = _file.get();
        _opts.file_meta_data = &_metadata;
    }

    StatusOr<bool> filter(ScalarColumnReader* reader, SparseRange<uint64_t>* ranges) {
        return reader->page_index_zone_map_filter({_predicate}, ranges, CompoundNodeType::AND, 0, 100);
    }

    ObjectPool _pool;
    FileMetaData _metadata;
    const ParquetField* _field = nullptr;
    tparquet::ColumnChunk _chunk;
    ColumnReaderOptions _opts{};
    std::unique_ptr<RandomAccessFile> _file;
    const ColumnPredicate* _predicate = nullptr;
};

} // namespace

// A failed OffsetIndex read must not leave an empty OffsetIndex cached on the reader: page index filtering
// swallows the error, and every later caller would then index page_locations by the ColumnIndex page count.
TEST_F(PageIndexFailedReadTest, failed_offset_index_read_is_not_cached) {
    build_file(nullptr);
    ScalarColumnReader reader(_field, &_chunk, &TYPE_INT_DESC, _opts);

    for (int i = 0; i < 3; i++) {
        SparseRange<uint64_t> ranges;
        auto result = filter(&reader, &ranges);
        EXPECT_FALSE(result.ok());
        EXPECT_TRUE(ranges.empty());
        EXPECT_EQ(nullptr, reader._offset_index_ctx);
    }

    // The read path must not wrap the page reader around an index it could not read.
    SparseRange<uint64_t> range(0, 100);
    reader.select_offset_index(range, 0);
    EXPECT_EQ(nullptr, reader._offset_index_ctx);
    EXPECT_EQ(nullptr, dynamic_cast<StoredColumnReaderWithIndex*>(reader._reader.get()));
}

// The OffsetIndex lists one page while the ColumnIndex lists two. Either may be the broken one, so the reader
// must neither prune with them nor read through the OffsetIndex: StoredColumnReaderWithIndex stops after the
// pages the OffsetIndex lists, which would drop the rest of the column chunk.
TEST_F(PageIndexFailedReadTest, mismatched_page_counts_skip_filtering_and_indexed_read) {
    auto offset_index = offset_index_of({0});
    build_file(&offset_index);
    ScalarColumnReader reader(_field, &_chunk, &TYPE_INT_DESC, _opts);
    ASSERT_OK(reader.prepare());

    SparseRange<uint64_t> ranges;
    auto result = filter(&reader, &ranges);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_FALSE(result.value());
    EXPECT_TRUE(ranges.empty());

    // Another column pruned the row group, so the group reader selects pages on every column.
    SparseRange<uint64_t> range(50, 100);
    reader.select_offset_index(range, 0);
    ASSERT_NE(nullptr, reader._reader);
    EXPECT_EQ(nullptr, dynamic_cast<StoredColumnReaderWithIndex*>(reader._reader.get()));
}

// Control for the case above: a consistent page index is used for indexed reading.
TEST_F(PageIndexFailedReadTest, consistent_offset_index_is_used_for_reading) {
    auto offset_index = offset_index_of({0, 50});
    build_file(&offset_index);
    ScalarColumnReader reader(_field, &_chunk, &TYPE_INT_DESC, _opts);
    ASSERT_OK(reader.prepare());

    SparseRange<uint64_t> ranges;
    auto result = filter(&reader, &ranges);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_TRUE(result.value());

    reader.select_offset_index(ranges, 0);
    EXPECT_NE(nullptr, dynamic_cast<StoredColumnReaderWithIndex*>(reader._reader.get()));
}

TEST_F(PageIndexFailedReadTest, empty_offset_index_is_not_used_for_reading) {
    auto offset_index = offset_index_of({});
    build_file(&offset_index);
    ScalarColumnReader reader(_field, &_chunk, &TYPE_INT_DESC, _opts);

    SparseRange<uint64_t> ranges;
    auto result = filter(&reader, &ranges);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_FALSE(result.value());

    SparseRange<uint64_t> range(0, 100);
    reader.select_offset_index(range, 0);
    EXPECT_EQ(nullptr, dynamic_cast<StoredColumnReaderWithIndex*>(reader._reader.get()));
}

TEST_F(PageIndexFailedReadTest, consistent_page_index_prunes_pages) {
    auto offset_index = offset_index_of({0, 50});
    build_file(&offset_index);
    ScalarColumnReader reader(_field, &_chunk, &TYPE_INT_DESC, _opts);

    SparseRange<uint64_t> ranges;
    auto result = filter(&reader, &ranges);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_TRUE(result.value());
    ASSERT_EQ(1, ranges.size());
    EXPECT_EQ(50, ranges.begin());
    EXPECT_EQ(100, ranges.end());
}

} // namespace starrocks::parquet
