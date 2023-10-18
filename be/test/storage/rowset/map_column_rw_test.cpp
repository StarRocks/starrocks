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

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column.h"
#include "column/column_access_path.h"
#include "column/datum_convert.h"
#include "column/fixed_length_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "fs/fs_memory.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/column_writer.h"
#include "storage/rowset/map_column_iterator.h"
#include "storage/rowset/segment.h"
#include "storage/tablet_schema_helper.h"
#include "testutil/assert.h"

namespace starrocks {

// NOLINTNEXTLINE
static const std::string TEST_DIR = "/map_column_rw_test";

class MapColumnRWTest : public testing::Test {
public:
    MapColumnRWTest() = default;

    ~MapColumnRWTest() override = default;

protected:
    void SetUp() override {}

    void TearDown() override {}

    std::shared_ptr<Segment> create_dummy_segment(const std::shared_ptr<FileSystem>& fs, const std::string& fname) {
        return std::make_shared<Segment>(fs, fname, 1, _dummy_segment_schema.get());
    }

    void test_int_map() {
        auto fs = std::make_shared<MemoryFileSystem>();
        ASSERT_TRUE(fs->create_dir(TEST_DIR).ok());

        TabletColumn map_column = create_map(0, true);
        // add key
        TabletColumn key_column = create_int_value(1, STORAGE_AGGREGATE_NONE, true);
        map_column.add_sub_column(key_column);
        // add value
        TabletColumn value_column = create_int_value(2, STORAGE_AGGREGATE_NONE, true);
        map_column.add_sub_column(value_column);

        auto src_offsets = UInt32Column::create();
        auto src_keys = NullableColumn::create(Int32Column::create(), NullColumn::create());
        auto src_values = NullableColumn::create(Int32Column::create(), NullColumn::create());

        ColumnPtr src_column = MapColumn::create(src_keys, src_values, src_offsets);

        //  {1 = 1}
        src_keys->append_datum(1);
        src_values->append_datum(1);
        src_offsets->append(1);
        // {}
        src_offsets->append(1);
        // { 2 = 200, 3 = 3000}
        src_keys->append_datum(2);
        src_keys->append_datum(3);
        src_values->append_datum(200);
        src_values->append_datum(3000);
        src_offsets->append(3);
        // { 4 = -1, 5 = -2, 6 = -3}
        src_keys->append_datum(4);
        src_keys->append_datum(5);
        src_keys->append_datum(6);
        src_values->append_datum(-1);
        src_values->append_datum(-2);
        src_values->append_datum(-3);
        src_offsets->append(6);

        TypeInfoPtr type_info = get_type_info(map_column);
        ColumnMetaPB meta;

        // delete test file.
        const std::string fname = TEST_DIR + "/test_map_rw_int_int.data";
        auto segment = create_dummy_segment(fs, fname);
        // write data
        {
            ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(fname));

            ColumnWriterOptions writer_opts;
            writer_opts.meta = &meta;
            writer_opts.meta->set_column_id(0);
            writer_opts.meta->set_unique_id(0);
            writer_opts.meta->set_type(TYPE_MAP);
            writer_opts.meta->set_length(0);
            writer_opts.meta->set_encoding(DEFAULT_ENCODING);
            writer_opts.meta->set_compression(starrocks::LZ4_FRAME);
            writer_opts.meta->set_is_nullable(false);
            writer_opts.need_zone_map = false;

            // init integer sub column
            ColumnMetaPB* key_meta = writer_opts.meta->add_children_columns();
            key_meta->set_column_id(0);
            key_meta->set_unique_id(0);
            key_meta->set_type(key_column.type());
            key_meta->set_length(key_column.length());
            key_meta->set_encoding(DEFAULT_ENCODING);
            key_meta->set_compression(LZ4_FRAME);
            key_meta->set_is_nullable(false);

            ColumnMetaPB* value_meta = writer_opts.meta->add_children_columns();
            value_meta->set_column_id(0);
            value_meta->set_unique_id(0);
            value_meta->set_type(key_column.type());
            value_meta->set_length(value_column.length());
            value_meta->set_encoding(DEFAULT_ENCODING);
            value_meta->set_compression(LZ4_FRAME);
            value_meta->set_is_nullable(false);

            ASSIGN_OR_ABORT(auto writer, ColumnWriter::create(writer_opts, &map_column, wfile.get()));
            ASSERT_OK(writer->init());

            ASSERT_TRUE(writer->append(*src_column).ok());

            ASSERT_TRUE(writer->finish().ok());
            ASSERT_TRUE(writer->write_data().ok());
            ASSERT_TRUE(writer->write_ordinal_index().ok());

            // close the file
            ASSERT_TRUE(wfile->close().ok());
        }

        // read and check
        auto res = ColumnReader::create(&meta, segment.get());
        ASSERT_TRUE(res.ok());
        auto reader = std::move(res).value();

        {
            ASSIGN_OR_ABORT(auto iter, reader->new_iterator());
            ASSIGN_OR_ABORT(auto read_file, fs->new_random_access_file(fname));

            ColumnIteratorOptions iter_opts;
            OlapReaderStatistics stats;
            iter_opts.stats = &stats;
            iter_opts.read_file = read_file.get();
            ASSERT_TRUE(iter->init(iter_opts).ok());

            // sequence read
            {
                auto st = iter->seek_to_first();
                ASSERT_TRUE(st.ok()) << st.to_string();

                auto dst_offsets = UInt32Column::create();
                auto dst_keys = NullableColumn::create(Int32Column::create(), NullColumn::create());
                auto dst_values = NullableColumn::create(Int32Column::create(), NullColumn::create());
                auto dst_column = MapColumn::create(dst_keys, dst_values, dst_offsets);
                size_t rows_read = src_column->size();
                st = iter->next_batch(&rows_read, dst_column.get());
                ASSERT_TRUE(st.ok());
                ASSERT_EQ(src_column->size(), rows_read);

                ASSERT_EQ("{1:1}", dst_column->debug_item(0));
                ASSERT_EQ("{}", dst_column->debug_item(1));
                ASSERT_EQ("{2:200,3:3000}", dst_column->debug_item(2));
                ASSERT_EQ("{4:-1,5:-2,6:-3}", dst_column->debug_item(3));
            }
        }

        {
            auto child_path = std::make_unique<ColumnAccessPath>();
            child_path->init(TAccessPathType::type::OFFSET, "offsets", 1);

            ColumnAccessPath path;
            path.init(TAccessPathType::type::ROOT, "root", 0);
            path.children().emplace_back(std::move(child_path));

            ASSIGN_OR_ABORT(auto iter, reader->new_iterator(&path));
            ASSIGN_OR_ABORT(auto read_file, fs->new_random_access_file(fname));

            ColumnIteratorOptions iter_opts;
            OlapReaderStatistics stats;
            iter_opts.stats = &stats;
            iter_opts.read_file = read_file.get();
            ASSERT_TRUE(iter->init(iter_opts).ok());

            // sequence read
            {
                auto st = iter->seek_to_first();
                ASSERT_TRUE(st.ok()) << st.to_string();

                auto dst_offsets = UInt32Column::create();
                auto dst_keys = NullableColumn::create(Int32Column::create(), NullColumn::create());
                auto dst_values = NullableColumn::create(Int32Column::create(), NullColumn::create());
                auto dst_column = MapColumn::create(dst_keys, dst_values, dst_offsets);
                size_t rows_read = src_column->size();
                st = iter->next_batch(&rows_read, dst_column.get());
                ASSERT_TRUE(st.ok());
                ASSERT_EQ(src_column->size(), rows_read);

                ASSERT_EQ("{NULL:NULL}", dst_column->debug_item(0));
                ASSERT_EQ("{}", dst_column->debug_item(1));
                ASSERT_EQ("{NULL:NULL,NULL:NULL}", dst_column->debug_item(2));
                ASSERT_EQ("{NULL:NULL,NULL:NULL,NULL:NULL}", dst_column->debug_item(3));
            }
        }

        {
            auto child_path = std::make_unique<ColumnAccessPath>();
            child_path->init(TAccessPathType::type::KEY, "key", 1);

            ColumnAccessPath path;
            path.init(TAccessPathType::type::ROOT, "root", 0);
            path.children().emplace_back(std::move(child_path));

            ASSIGN_OR_ABORT(auto iter, reader->new_iterator(&path));
            ASSIGN_OR_ABORT(auto read_file, fs->new_random_access_file(fname));

            ColumnIteratorOptions iter_opts;
            OlapReaderStatistics stats;
            iter_opts.stats = &stats;
            iter_opts.read_file = read_file.get();
            ASSERT_TRUE(iter->init(iter_opts).ok());

            // sequence read
            {
                auto st = iter->seek_to_first();
                ASSERT_TRUE(st.ok()) << st.to_string();

                auto dst_offsets = UInt32Column::create();
                auto dst_keys = NullableColumn::create(Int32Column::create(), NullColumn::create());
                auto dst_values = NullableColumn::create(Int32Column::create(), NullColumn::create());
                auto dst_column = MapColumn::create(dst_keys, dst_values, dst_offsets);
                size_t rows_read = src_column->size();
                st = iter->next_batch(&rows_read, dst_column.get());
                ASSERT_TRUE(st.ok());
                ASSERT_EQ(src_column->size(), rows_read);

                ASSERT_EQ("{1:NULL}", dst_column->debug_item(0));
                ASSERT_EQ("{}", dst_column->debug_item(1));
                ASSERT_EQ("{2:NULL,3:NULL}", dst_column->debug_item(2));
                ASSERT_EQ("{4:NULL,5:NULL,6:NULL}", dst_column->debug_item(3));
            }
        }
    }

private:
    std::shared_ptr<TabletSchema> _dummy_segment_schema;
};

// test map<int, int>, and nullable
TEST_F(MapColumnRWTest, test_map_int) {
    test_int_map();
}

} // namespace starrocks
