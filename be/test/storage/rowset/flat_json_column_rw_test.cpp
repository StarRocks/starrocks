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

#include "column/column_access_path.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "common/statusor.h"
#include "fs/fs_memory.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gutil/casts.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/column_writer.h"
#include "storage/rowset/json_column_iterator.h"
#include "storage/rowset/segment.h"
#include "storage/tablet_schema_helper.h"
#include "storage/types.h"
#include "testutil/assert.h"
#include "types/logical_type.h"
#include "util/json.h"

namespace starrocks {

// NOLINTNEXTLINE
static const std::string TEST_DIR = "/flat_json_column_rw_test";

class FlatJsonColumnRWTest : public testing::Test {
public:
    FlatJsonColumnRWTest() = default;

    ~FlatJsonColumnRWTest() override = default;

protected:
    void SetUp() override {}

    void TearDown() override {}

    std::shared_ptr<Segment> create_dummy_segment(const std::shared_ptr<FileSystem>& fs, const std::string& fname) {
        return std::make_shared<Segment>(fs, FileInfo{fname}, 1, _dummy_segment_schema, nullptr);
    }

    void test_json(const std::string& case_file, ColumnPtr& write_col, ColumnPtr& read_col, ColumnAccessPath* path) {
        auto fs = std::make_shared<MemoryFileSystem>();
        ASSERT_TRUE(fs->create_dir(TEST_DIR).ok());

        TabletColumn json_tablet_column = create_with_default_value<TYPE_JSON>("");
        TypeInfoPtr type_info = get_type_info(json_tablet_column);
        ColumnMetaPB meta;

        const std::string fname = TEST_DIR + case_file;
        auto segment = create_dummy_segment(fs, fname);

        // write data
        {
            ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(fname));

            ColumnWriterOptions writer_opts;
            writer_opts.meta = &meta;
            writer_opts.meta->set_column_id(0);
            writer_opts.meta->set_unique_id(0);
            writer_opts.meta->set_type(TYPE_JSON);
            writer_opts.meta->set_length(0);
            writer_opts.meta->set_encoding(DEFAULT_ENCODING);
            writer_opts.meta->set_compression(starrocks::LZ4_FRAME);
            writer_opts.meta->set_is_nullable(write_col->is_nullable());
            writer_opts.need_zone_map = false;

            ASSIGN_OR_ABORT(auto writer, ColumnWriter::create(writer_opts, &json_tablet_column, wfile.get()));
            ASSERT_OK(writer->init());

            ASSERT_TRUE(writer->append(*write_col).ok());

            ASSERT_TRUE(writer->finish().ok());
            ASSERT_TRUE(writer->write_data().ok());
            ASSERT_TRUE(writer->write_ordinal_index().ok());

            // close the file
            ASSERT_TRUE(wfile->close().ok());
        }
        LOG(INFO) << "Finish writing";

        auto res = ColumnReader::create(&meta, segment.get());
        ASSERT_TRUE(res.ok());
        auto reader = std::move(res).value();

        {
            ASSIGN_OR_ABORT(auto iter, reader->new_iterator(path));
            ASSIGN_OR_ABORT(auto read_file, fs->new_random_access_file(fname));

            ColumnIteratorOptions iter_opts;
            OlapReaderStatistics stats;
            iter_opts.stats = &stats;
            iter_opts.read_file = read_file.get();
            ASSERT_TRUE(iter->init(iter_opts).ok());

            // sequence read
            auto st = iter->seek_to_first();
            ASSERT_TRUE(st.ok()) << st.to_string();

            size_t rows_read = write_col->size();
            st = iter->next_batch(&rows_read, read_col.get());
            ASSERT_TRUE(st.ok());
        }
    }

private:
    std::shared_ptr<TabletSchema> _dummy_segment_schema;
};

TEST_F(FlatJsonColumnRWTest, testNormalFlatJson) {
    config::json_flat_internal_column_min_limit = 1;

    ColumnPtr write_col = JsonColumn::create();
    auto* json_col = down_cast<JsonColumn*>(write_col.get());

    ASSIGN_OR_ABORT(auto jv1, JsonValue::parse("{\"a\": 1, \"b\": 21}"));
    ASSIGN_OR_ABORT(auto jv2, JsonValue::parse("{\"a\": 2, \"b\": 22}"));
    ASSIGN_OR_ABORT(auto jv3, JsonValue::parse("{\"a\": 3, \"b\": 23}"));
    ASSIGN_OR_ABORT(auto jv4, JsonValue::parse("{\"a\": 4, \"b\": 24}"));
    ASSIGN_OR_ABORT(auto jv5, JsonValue::parse("{\"a\": 5, \"b\": 25}"));

    json_col->append(&jv1);
    json_col->append(&jv2);
    json_col->append(&jv3);
    json_col->append(&jv4);
    json_col->append(&jv5);

    ASSIGN_OR_ABORT(auto root_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ASSIGN_OR_ABORT(auto f1_path, ColumnAccessPath::create(TAccessPathType::FIELD, "a", 0));
    ASSIGN_OR_ABORT(auto f2_path, ColumnAccessPath::create(TAccessPathType::FIELD, "b", 0));
    root_path->children().emplace_back(std::move(f1_path));
    root_path->children().emplace_back(std::move(f2_path));

    ColumnPtr read_col = JsonColumn::create();
    test_json("/test_flat_json_rw1.data", write_col, read_col, root_path.get());

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    ASSERT_EQ(2, read_json->get_flat_fields().size());
    EXPECT_EQ("{a: 1, b: 21}", read_json->debug_item(0));
    EXPECT_EQ("{a: 4, b: 24}", read_json->debug_item(3));

    EXPECT_EQ("3", read_json->get_flat_field("a")->debug_item(2));
}

TEST_F(FlatJsonColumnRWTest, testNullFlatJson) {
    config::json_flat_internal_column_min_limit = 1;

    ColumnPtr write_col = JsonColumn::create();
    auto* json_col = down_cast<JsonColumn*>(write_col.get());

    ASSIGN_OR_ABORT(auto jv1, JsonValue::parse("{\"a\": 1, \"b\": 21}"));
    ASSIGN_OR_ABORT(auto jv2, JsonValue::parse("{\"a\": 2, \"b\": 22}"));
    ASSIGN_OR_ABORT(auto jv3, JsonValue::parse("{\"a\": 3, \"b\": 23}"));
    ASSIGN_OR_ABORT(auto jv4, JsonValue::parse("{\"a\": 4, \"b\": 24}"));
    ASSIGN_OR_ABORT(auto jv5, JsonValue::parse("{\"a\": 5, \"b\": 25}"));

    json_col->append(&jv1);
    json_col->append(&jv2);
    json_col->append(&jv3);
    json_col->append(&jv4);
    json_col->append(&jv5);

    auto null_col = NullColumn::create();
    null_col->append(1);
    null_col->append(1);
    null_col->append(1);
    null_col->append(1);
    null_col->append(0);

    ColumnPtr write_nl_col = NullableColumn::create(write_col, null_col);

    ASSIGN_OR_ABORT(auto root_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ASSIGN_OR_ABORT(auto f1_path, ColumnAccessPath::create(TAccessPathType::FIELD, "a", 0));
    ASSIGN_OR_ABORT(auto f2_path, ColumnAccessPath::create(TAccessPathType::FIELD, "b", 0));
    root_path->children().emplace_back(std::move(f1_path));
    root_path->children().emplace_back(std::move(f2_path));

    ColumnPtr read_col = NullableColumn::create(JsonColumn::create(), NullColumn::create());
    test_json("/test_flat_json_rw2.data", write_nl_col, read_col, root_path.get());

    auto* read_json = down_cast<JsonColumn*>(down_cast<NullableColumn*>(read_col.get())->data_column().get());
    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    EXPECT_EQ("NULL", read_col->debug_item(0));
    EXPECT_EQ("{a: 5, b: 25}", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnRWTest, testLimitFlatJson) {
    config::json_flat_internal_column_min_limit = 5;

    ColumnPtr write_col = JsonColumn::create();
    auto* json_col = down_cast<JsonColumn*>(write_col.get());

    ASSIGN_OR_ABORT(auto jv1, JsonValue::parse("{\"a\": 1, \"b\": 21}"));
    ASSIGN_OR_ABORT(auto jv2, JsonValue::parse("{\"a\": 2, \"b\": 22}"));
    ASSIGN_OR_ABORT(auto jv3, JsonValue::parse("{\"a\": 3, \"b\": 23}"));
    ASSIGN_OR_ABORT(auto jv4, JsonValue::parse("{\"a\": 4, \"b\": 24}"));
    ASSIGN_OR_ABORT(auto jv5, JsonValue::parse("{\"a\": 5, \"b\": 25}"));

    json_col->append(&jv1);
    json_col->append(&jv2);
    json_col->append(&jv3);
    json_col->append(&jv4);
    json_col->append(&jv5);

    ASSIGN_OR_ABORT(auto root_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ASSIGN_OR_ABORT(auto f1_path, ColumnAccessPath::create(TAccessPathType::FIELD, "a", 0));
    ASSIGN_OR_ABORT(auto f2_path, ColumnAccessPath::create(TAccessPathType::FIELD, "b", 0));
    root_path->children().emplace_back(std::move(f1_path));
    root_path->children().emplace_back(std::move(f2_path));

    ColumnPtr read_col = JsonColumn::create();
    test_json("/test_flat_json_rw3.data", write_col, read_col, root_path.get());

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    ASSERT_EQ(2, read_json->get_flat_fields().size());
    EXPECT_EQ("{a: 1, b: 21}", read_json->debug_item(0));
    EXPECT_EQ("{a: 4, b: 24}", read_json->debug_item(3));
    EXPECT_EQ("3", read_json->get_flat_field("a")->debug_item(2));
}

TEST_F(FlatJsonColumnRWTest, tesArrayFlatJson) {
    config::json_flat_internal_column_min_limit = 5;

    ColumnPtr write_col = JsonColumn::create();
    auto* json_col = down_cast<JsonColumn*>(write_col.get());

    ASSIGN_OR_ABORT(auto jv1, JsonValue::parse(R"( [{"a": 1}, {"b": 21}] )"));
    ASSIGN_OR_ABORT(auto jv2, JsonValue::parse("{\"a\": 2, \"b\": 22}"));
    ASSIGN_OR_ABORT(auto jv3, JsonValue::parse("{\"a\": 3, \"b\": 23}"));
    ASSIGN_OR_ABORT(auto jv4, JsonValue::parse("{\"a\": 4, \"b\": 24}"));
    ASSIGN_OR_ABORT(auto jv5, JsonValue::parse("{\"a\": 5, \"b\": 25}"));

    json_col->append(&jv1);
    json_col->append(&jv2);
    json_col->append(&jv3);
    json_col->append(&jv4);
    json_col->append(&jv5);

    ASSIGN_OR_ABORT(auto root_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ASSIGN_OR_ABORT(auto f1_path, ColumnAccessPath::create(TAccessPathType::FIELD, "a", 0));
    ASSIGN_OR_ABORT(auto f2_path, ColumnAccessPath::create(TAccessPathType::FIELD, "b", 0));
    root_path->children().emplace_back(std::move(f1_path));
    root_path->children().emplace_back(std::move(f2_path));

    ColumnPtr read_col = JsonColumn::create();
    test_json("/test_flat_json_rw3.data", write_col, read_col, root_path.get());

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    ASSERT_EQ(2, read_json->get_flat_fields().size());
    EXPECT_EQ("{a: , b: }", read_json->debug_item(0));
    EXPECT_EQ("{a: 4, b: 24}", read_json->debug_item(3));
}

TEST_F(FlatJsonColumnRWTest, testEmptyObject) {
    config::json_flat_internal_column_min_limit = 5;

    ColumnPtr write_col = JsonColumn::create();
    auto* json_col = down_cast<JsonColumn*>(write_col.get());

    ASSIGN_OR_ABORT(auto jv1, JsonValue::parse(R"( "" )"));
    ASSIGN_OR_ABORT(auto jv2, JsonValue::parse("{\"a\": 2, \"b\": 22}"));
    ASSIGN_OR_ABORT(auto jv3, JsonValue::parse("{\"a\": 3, \"b\": 23}"));
    ASSIGN_OR_ABORT(auto jv4, JsonValue::parse("{\"a\": 4, \"b\": 24}"));
    ASSIGN_OR_ABORT(auto jv5, JsonValue::parse("{\"a\": 5, \"b\": 25}"));

    json_col->append(&jv1);
    json_col->append(&jv2);
    json_col->append(&jv3);
    json_col->append(&jv4);
    json_col->append(&jv5);

    ASSIGN_OR_ABORT(auto root_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ASSIGN_OR_ABORT(auto f1_path, ColumnAccessPath::create(TAccessPathType::FIELD, "a", 0));
    ASSIGN_OR_ABORT(auto f2_path, ColumnAccessPath::create(TAccessPathType::FIELD, "b", 0));
    root_path->children().emplace_back(std::move(f1_path));
    root_path->children().emplace_back(std::move(f2_path));

    ColumnPtr read_col = JsonColumn::create();
    test_json("/test_flat_json_rw4.data", write_col, read_col, root_path.get());

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    ASSERT_EQ(2, read_json->get_flat_fields().size());
    EXPECT_EQ("{a: , b: }", read_json->debug_item(0));
    EXPECT_EQ("{a: 4, b: 24}", read_json->debug_item(3));
}

} // namespace starrocks
