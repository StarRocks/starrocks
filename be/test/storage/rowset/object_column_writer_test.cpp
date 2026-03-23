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

#include "base/testutil/assert.h"
#include "column/nullable_column.h"
#include "column/object_column.h"
#include "fs/fs_memory.h"
#include "gen_cpp/segment.pb.h"
#include "storage/aggregate_type.h"
#include "storage/chunk_helper.h"
#include "storage/olap_common.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/column_writer.h"
#include "storage/rowset/segment.h"
#include "types/bitmap_value.h"
#include "types/hll.h"
#include "types/percentile_value.h"

namespace starrocks {

static const std::string TEST_DIR = "/object_column_writer_test";

class ObjectColumnWriterTest : public testing::Test {
public:
    void SetUp() override;
    void TearDown() override;

protected:
    std::shared_ptr<Segment> create_dummy_segment(const std::shared_ptr<FileSystem>& fs, const std::string& fname) {
        return std::make_shared<Segment>(fs, FileInfo{fname}, 1, _dummy_segment_schema, nullptr);
    }

    template <LogicalType type>
    void write_object_column(const Column& src, bool nullable, ColumnMetaPB* meta);

    template <LogicalType type>
    void read_and_verify_object_column(ColumnMetaPB* meta, const Column& src, bool nullable) const;

    template <LogicalType type>
    void test_object_data(const Column& src, bool nullable);

    std::shared_ptr<TabletSchema> _dummy_segment_schema;
    std::shared_ptr<MemoryFileSystem> _fs;
    std::string _fname;
    std::shared_ptr<Segment> _segment;
};

void ObjectColumnWriterTest::SetUp() {
    TabletSchemaPB schema_pb;
    auto* c0 = schema_pb.add_column();
    c0->set_name("pk");
    c0->set_is_key(true);
    c0->set_type("BIGINT");
    _dummy_segment_schema = TabletSchema::create(schema_pb);

    _fs = std::make_shared<MemoryFileSystem>();
    EXPECT_OK(_fs->create_dir(TEST_DIR));
    _fname = strings::Substitute("$0/test.data", TEST_DIR);
    _segment = create_dummy_segment(_fs, _fname);
}

void ObjectColumnWriterTest::TearDown() {
    (void)_fs->delete_file(_fname);
}

template <LogicalType type>
void ObjectColumnWriterTest::write_object_column(const Column& src, bool nullable, ColumnMetaPB* meta) {
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(_fname));

    ColumnWriterOptions writer_opts;
    writer_opts.page_format = 2;
    writer_opts.meta = meta;
    writer_opts.meta->set_column_id(0);
    writer_opts.meta->set_unique_id(0);
    writer_opts.meta->set_type(type);
    writer_opts.meta->set_length(0);
    writer_opts.meta->set_encoding(PLAIN_ENCODING);
    writer_opts.meta->set_compression(LZ4_FRAME);
    writer_opts.meta->set_is_nullable(nullable);

    TabletColumn column(STORAGE_AGGREGATE_NONE, type);
    ASSIGN_OR_ABORT(auto writer, ColumnWriter::create(writer_opts, &column, wfile.get()));
    ASSERT_OK(writer->init());
    ASSERT_OK(writer->append(src));
    ASSERT_OK(writer->finish());
    ASSERT_OK(writer->write_data());
    ASSERT_OK(writer->write_ordinal_index());
    ASSERT_OK(wfile->close());
}

template <LogicalType type>
void ObjectColumnWriterTest::read_and_verify_object_column(ColumnMetaPB* meta, const Column& src, bool nullable) const {
    auto res = ColumnReader::create(meta, _segment.get(), nullptr);
    ASSERT_TRUE(res.ok());
    auto reader = std::move(res).value();

    ASSIGN_OR_ABORT(auto iter, reader->new_iterator());
    ASSIGN_OR_ABORT(auto read_file, _fs->new_random_access_file(_fname));

    ColumnIteratorOptions iter_opts;
    OlapReaderStatistics stats;
    iter_opts.stats = &stats;
    iter_opts.read_file = read_file.get();
    ASSERT_OK(iter->init(iter_opts));
    ASSERT_OK(iter->seek_to_first());

    MutableColumnPtr dst = ChunkHelper::column_from_field_type(type, nullable);
    size_t rows_read = src.size();
    dst->reserve(rows_read);
    ASSERT_OK(iter->next_batch(&rows_read, dst.get()));
    ASSERT_EQ(src.size(), rows_read);

    ASSERT_EQ(src.debug_string(), dst->debug_string());
}

template <LogicalType type>
void ObjectColumnWriterTest::test_object_data(const Column& src, bool nullable) {
    ColumnMetaPB meta;
    write_object_column<type>(src, nullable, &meta);
    read_and_verify_object_column<type>(&meta, src, nullable);
}

TEST_F(ObjectColumnWriterTest, test_hll) {
    auto col = HyperLogLogColumn::create();
    for (int i = 0; i < 100; i++) {
        HyperLogLog hll;
        hll.update(static_cast<uint64_t>(i));
        col->append(hll);
    }
    test_object_data<TYPE_HLL>(*col, false);
}

TEST_F(ObjectColumnWriterTest, test_hll_nullable) {
    auto data_col = HyperLogLogColumn::create();
    auto null_col = NullColumn::create();
    for (int i = 0; i < 100; i++) {
        HyperLogLog hll;
        hll.update(static_cast<uint64_t>(i));
        data_col->append(hll);
        null_col->append(i % 5 == 0 ? DATUM_NULL : DATUM_NOT_NULL);
    }
    auto nullable = NullableColumn::create(std::move(data_col), std::move(null_col));
    test_object_data<TYPE_HLL>(*nullable, true);
}

TEST_F(ObjectColumnWriterTest, test_bitmap_not_null) {
    auto col = BitmapColumn::create();
    for (int i = 0; i < 100; i++) {
        BitmapValue bmp;
        for (int j = 0; j <= i; j++) {
            bmp.add(static_cast<uint64_t>(j));
        }
        col->append(bmp);
    }
    test_object_data<TYPE_OBJECT>(*col, false);
}

TEST_F(ObjectColumnWriterTest, test_bitmap_nullable) {
    auto data_col = BitmapColumn::create();
    auto null_col = NullColumn::create();
    for (int i = 0; i < 100; i++) {
        BitmapValue bmp;
        bmp.add(static_cast<uint64_t>(i));
        data_col->append(bmp);
        null_col->append(i % 2 == 1 ? DATUM_NULL : DATUM_NOT_NULL);
    }
    auto nullable = NullableColumn::create(std::move(data_col), std::move(null_col));
    test_object_data<TYPE_OBJECT>(*nullable, true);
}

TEST_F(ObjectColumnWriterTest, test_percentile_not_null) {
    auto col = PercentileColumn::create();
    for (int i = 0; i < 100; i++) {
        PercentileValue pct;
        for (int j = 0; j <= i; j++) {
            pct.add(static_cast<float>(j));
        }
        col->append(pct);
    }
    test_object_data<TYPE_PERCENTILE>(*col, false);
}

TEST_F(ObjectColumnWriterTest, test_percentile_nullable) {
    auto data_col = PercentileColumn::create();
    auto null_col = NullColumn::create();
    for (int i = 0; i < 100; i++) {
        PercentileValue pct;
        pct.add(static_cast<float>(i));
        data_col->append(pct);
        null_col->append(i % 3 == 0 ? DATUM_NULL : DATUM_NOT_NULL);
    }
    auto nullable = NullableColumn::create(std::move(data_col), std::move(null_col));
    test_object_data<TYPE_PERCENTILE>(*nullable, true);
}

} // namespace starrocks