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
#include "column/struct_column.h"
#include "fs/fs_memory.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/column_writer.h"
#include "storage/rowset/map_column_iterator.h"
#include "storage/rowset/segment.h"
#include "storage/tablet_schema_helper.h"
#include "testutil/assert.h"

namespace starrocks {

// NOLINTNEXTLINE
static const std::string TEST_DIR = "/struct_column_rw_test";

class StructColumnRWTest : public testing::Test {
public:
    StructColumnRWTest() = default;

    ~StructColumnRWTest() override = default;

protected:
    void SetUp() override {}

    void TearDown() override {}

    std::shared_ptr<Segment> create_dummy_segment(const std::shared_ptr<FileSystem>& fs, const std::string& fname) {
        return std::make_shared<Segment>(fs, fname, 1, _dummy_segment_schema, nullptr);
    }

    void test_int_struct() {
        auto fs = std::make_shared<MemoryFileSystem>();
        ASSERT_TRUE(fs->create_dir(TEST_DIR).ok());

        TabletColumn struct_column = create_struct(0, true);
        std::vector<std::string> names{"f1", "f2"};
        TabletColumn f1_tablet_column = create_int_value(1, STORAGE_AGGREGATE_NONE, true);
        struct_column.add_sub_column(f1_tablet_column);
        // add f2
        TabletColumn f2_tablet_column = create_varchar_key(2, true);
        struct_column.add_sub_column(f2_tablet_column);

        auto f1_column = Int32Column::create();
        f1_column->append(1);

        auto f2_column = BinaryColumn::create();
        f2_column->append_string("Column2");

        Columns columns;
        columns.emplace_back(std::move(f1_column));
        columns.emplace_back(std::move(f2_column));

        ColumnPtr src_column = StructColumn::create(columns, names);

        TypeInfoPtr type_info = get_type_info(struct_column);
        ColumnMetaPB meta;

        // delete test file.
        const std::string fname = TEST_DIR + "/test_struct_rw_int.data";
        auto segment = create_dummy_segment(fs, fname);
        // write data
        {
            ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(fname));

            ColumnWriterOptions writer_opts;
            writer_opts.meta = &meta;
            writer_opts.meta->set_column_id(0);
            writer_opts.meta->set_unique_id(0);
            writer_opts.meta->set_type(TYPE_STRUCT);
            writer_opts.meta->set_length(0);
            writer_opts.meta->set_encoding(DEFAULT_ENCODING);
            writer_opts.meta->set_compression(starrocks::LZ4_FRAME);
            writer_opts.meta->set_is_nullable(false);
            writer_opts.need_zone_map = false;

            // init integer sub column
            ColumnMetaPB* f1_meta = writer_opts.meta->add_children_columns();
            f1_meta->set_column_id(0);
            f1_meta->set_unique_id(0);
            f1_meta->set_type(f1_tablet_column.type());
            f1_meta->set_length(f1_tablet_column.length());
            f1_meta->set_encoding(DEFAULT_ENCODING);
            f1_meta->set_compression(LZ4_FRAME);
            f1_meta->set_is_nullable(false);

            ColumnMetaPB* f2_meta = writer_opts.meta->add_children_columns();
            f2_meta->set_column_id(0);
            f2_meta->set_unique_id(0);
            f2_meta->set_type(f2_tablet_column.type());
            f2_meta->set_length(f2_tablet_column.length());
            f2_meta->set_encoding(DEFAULT_ENCODING);
            f2_meta->set_compression(LZ4_FRAME);
            f2_meta->set_is_nullable(false);

            ASSIGN_OR_ABORT(auto writer, ColumnWriter::create(writer_opts, &struct_column, wfile.get()));
            ASSERT_OK(writer->init());

            ASSERT_TRUE(writer->append(*src_column).ok());

            ASSERT_TRUE(writer->finish().ok());
            ASSERT_TRUE(writer->write_data().ok());
            ASSERT_TRUE(writer->write_ordinal_index().ok());

            // close the file
            ASSERT_TRUE(wfile->close().ok());
        }

        LOG(INFO) << "Finish writing";
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
            auto st = iter->seek_to_first();
            ASSERT_TRUE(st.ok()) << st.to_string();

            auto dst_f1_column = Int32Column::create();
            auto dst_f2_column = BinaryColumn::create();
            Columns dst_columns;
            dst_columns.emplace_back(std::move(dst_f1_column));
            dst_columns.emplace_back(std::move(dst_f2_column));

            ColumnPtr dst_column = StructColumn::create(dst_columns, names);
            size_t rows_read = src_column->size();
            st = iter->next_batch(&rows_read, dst_column.get());
            ASSERT_TRUE(st.ok());
            ASSERT_EQ(src_column->size(), rows_read);

            ASSERT_EQ("{f1:1,f2:'Column2'}", dst_column->debug_item(0));
        }

        {
            auto child_path = std::make_unique<ColumnAccessPath>();
            child_path->init(TAccessPathType::type::FIELD, "f1", 0);

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

                auto dst_f1_column = Int32Column::create();
                auto dst_f2_column = BinaryColumn::create();
                Columns dst_columns;
                dst_columns.emplace_back(std::move(dst_f1_column));
                dst_columns.emplace_back(std::move(dst_f2_column));

                ColumnPtr dst_column = StructColumn::create(dst_columns, names);
                size_t rows_read = src_column->size();
                st = iter->next_batch(&rows_read, dst_column.get());
                ASSERT_TRUE(st.ok());
                ASSERT_EQ(src_column->size(), rows_read);

                ASSERT_EQ("{f1:1,f2:CONST: ''}", dst_column->debug_item(0));
            }
        }

        // read and check
        {
            auto child_path = std::make_unique<ColumnAccessPath>();
            child_path->init(TAccessPathType::type::FIELD, "f2", 1);

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

                auto dst_f1_column = Int32Column::create();
                auto dst_f2_column = BinaryColumn::create();
                Columns dst_columns;
                dst_columns.emplace_back(std::move(dst_f1_column));
                dst_columns.emplace_back(std::move(dst_f2_column));

                ColumnPtr dst_column = StructColumn::create(dst_columns, names);
                size_t rows_read = src_column->size();
                st = iter->next_batch(&rows_read, dst_column.get());
                ASSERT_TRUE(st.ok());
                ASSERT_EQ(src_column->size(), rows_read);

                ASSERT_EQ("{f1:CONST: 0,f2:'Column2'}", dst_column->debug_item(0));
            }
        }
    }

private:
    std::shared_ptr<TabletSchema> _dummy_segment_schema;
};

// test map<int, int>, and nullable
TEST_F(StructColumnRWTest, test_struct_int) {
    test_int_struct();
}

} // namespace starrocks
