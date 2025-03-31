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

#include <memory>
#include <vector>

#include "column/column_access_path.h"
#include "column/column_helper.h"
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
#include "util/json_flattener.h"

namespace starrocks {

// NOLINTNEXTLINE
static const std::string TEST_DIR = "/flat_json_column_rw_test";

class FlatJsonColumnRWTest : public testing::Test {
public:
    FlatJsonColumnRWTest() = default;

    ~FlatJsonColumnRWTest() override = default;

protected:
    void SetUp() override { _meta.reset(new ColumnMetaPB()); }

    void TearDown() override {
        config::json_flat_sparsity_factor = 0.9;
        config::json_flat_null_factor = 0.3;
    }

    std::shared_ptr<Segment> create_dummy_segment(const std::shared_ptr<FileSystem>& fs, const std::string& fname) {
        return std::make_shared<Segment>(fs, FileInfo{fname}, 1, _dummy_segment_schema, nullptr);
    }

    void test_json(ColumnWriterOptions& writer_opts, const std::string& case_file, ColumnPtr& write_col,
                   ColumnPtr& read_col, ColumnAccessPath* path) {
        auto fs = std::make_shared<MemoryFileSystem>();
        ASSERT_TRUE(fs->create_dir(TEST_DIR).ok());

        TabletColumn json_tablet_column = create_with_default_value<TYPE_JSON>("");
        TypeInfoPtr type_info = get_type_info(json_tablet_column);

        const std::string fname = TEST_DIR + case_file;
        auto segment = create_dummy_segment(fs, fname);

        // write data
        {
            ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(fname));

            writer_opts.meta = _meta.get();
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

        auto res = ColumnReader::create(_meta.get(), segment.get(), nullptr);
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

    ColumnPtr create_json(const std::vector<std::string>& jsons, bool is_nullable) {
        auto json_col = JsonColumn::create();
        auto null_col = NullColumn::create();
        auto* json_column = down_cast<JsonColumn*>(json_col.get());
        for (auto& json : jsons) {
            if ("NULL" != json) {
                ASSIGN_OR_ABORT(auto jv, JsonValue::parse(json));
                json_column->append(&jv);
            } else {
                json_column->append_default();
            }
            null_col->append("NULL" == json);
        }

        if (is_nullable) {
            return NullableColumn::create(std::move(json_col), std::move(null_col));
        }
        return json_col;
    }

private:
    std::shared_ptr<TabletSchema> _dummy_segment_schema;
    std::shared_ptr<ColumnMetaPB> _meta;
};

TEST_F(FlatJsonColumnRWTest, testNormalJson) {
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

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, "/test_flat_json_rw1.data", write_col, read_col, nullptr);

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    EXPECT_EQ("{\"a\": 1, \"b\": 21}", read_json->debug_item(0));
    EXPECT_EQ("{\"a\": 4, \"b\": 24}", read_json->debug_item(3));
}

TEST_F(FlatJsonColumnRWTest, testNormalJsonWithPath) {
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
    ASSIGN_OR_ABORT(auto f1_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root.a", 0));
    ASSIGN_OR_ABORT(auto f2_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root.b", 0));
    root_path->children().emplace_back(std::move(f1_path));
    root_path->children().emplace_back(std::move(f2_path));

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, "/test_flat_json_rw1.data", write_col, read_col, root_path.get());

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(2, read_json->get_flat_fields().size());
    EXPECT_EQ("{a: 1, b: 21}", read_json->debug_item(0));
    EXPECT_EQ("{a: 4, b: 24}", read_json->debug_item(3));
}

TEST_F(FlatJsonColumnRWTest, testNormalFlatJsonWithPath) {
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
    ASSIGN_OR_ABORT(auto f1_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root.a", 0));
    ASSIGN_OR_ABORT(auto f2_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root.b", 0));
    root_path->children().emplace_back(std::move(f1_path));
    root_path->children().emplace_back(std::move(f2_path));

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, "/test_flat_json_rw1.data", write_col, read_col, root_path.get());

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    ASSERT_EQ(2, read_json->get_flat_fields().size());
    EXPECT_EQ("{a: 1, b: 21}", read_json->debug_item(0));
    EXPECT_EQ("{a: 4, b: 24}", read_json->debug_item(3));

    EXPECT_EQ("3", read_json->get_flat_field("a")->debug_item(2));
}

TEST_F(FlatJsonColumnRWTest, testNormalFlatJsonWithoutPath) {
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

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, "/test_flat_json_rw1.data", write_col, read_col, nullptr);

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    ASSERT_EQ(0, read_json->get_flat_fields().size());
    EXPECT_EQ("{\"a\": 1, \"b\": 21}", read_json->debug_item(0));
    EXPECT_EQ("{\"a\": 4, \"b\": 24}", read_json->debug_item(3));
}

TEST_F(FlatJsonColumnRWTest, testNullNormalFlatJson) {
    config::json_flat_null_factor = 0.4;
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

    ColumnPtr write_nl_col = NullableColumn::create(std::move(write_col), std::move(null_col));

    ASSIGN_OR_ABORT(auto root_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ASSIGN_OR_ABORT(auto f1_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root.a", 0));
    ASSIGN_OR_ABORT(auto f2_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root.b", 0));
    root_path->children().emplace_back(std::move(f1_path));
    root_path->children().emplace_back(std::move(f2_path));

    ColumnPtr read_col = NullableColumn::create(JsonColumn::create(), NullColumn::create());
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_nl_col, read_col, root_path.get());

    auto* read_json = down_cast<JsonColumn*>(down_cast<NullableColumn*>(read_col.get())->data_column().get());
    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    EXPECT_EQ("NULL", read_col->debug_item(0));
    EXPECT_EQ("{a: 5, b: 25}", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnRWTest, tesArrayFlatJson) {
    ColumnPtr write_col = JsonColumn::create();
    auto* json_col = down_cast<JsonColumn*>(write_col.get());

    ASSIGN_OR_ABORT(auto jv1, JsonValue::parse(R"([{"a": 1}, {"b": 21}] )"));
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
    ASSIGN_OR_ABORT(auto f1_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root.a", 0));
    ASSIGN_OR_ABORT(auto f2_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root.b", 0));
    root_path->children().emplace_back(std::move(f1_path));
    root_path->children().emplace_back(std::move(f2_path));

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, "/test_flat_json_rw3.data", write_col, read_col, root_path.get());

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    ASSERT_EQ(2, read_json->get_flat_fields().size());
    EXPECT_EQ("{a: NULL, b: NULL}", read_json->debug_item(0));
    EXPECT_EQ("{a: 4, b: 24}", read_json->debug_item(3));
}

TEST_F(FlatJsonColumnRWTest, testEmptyFlatObject) {
    ColumnPtr write_col = JsonColumn::create();
    auto* json_col = down_cast<JsonColumn*>(write_col.get());

    ASSIGN_OR_ABORT(auto jv1, JsonValue::parse(R"("" )"));
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
    ASSIGN_OR_ABORT(auto f1_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root.a", 0));
    ASSIGN_OR_ABORT(auto f2_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root.b", 0));
    root_path->children().emplace_back(std::move(f1_path));
    root_path->children().emplace_back(std::move(f2_path));

    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;

    ColumnPtr read_col = JsonColumn::create();
    test_json(writer_opts, "/test_flat_json_rw4.data", write_col, read_col, root_path.get());

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    ASSERT_EQ(2, read_json->get_flat_fields().size());
    EXPECT_EQ("{a: NULL, b: NULL}", read_json->debug_item(0));
    EXPECT_EQ("{a: 4, b: 24}", read_json->debug_item(3));
}

TEST_F(FlatJsonColumnRWTest, testMergeRemainFlatJson) {
    ColumnPtr write_col = JsonColumn::create();
    auto* json_col = down_cast<JsonColumn*>(write_col.get());

    ASSIGN_OR_ABORT(auto jv1, JsonValue::parse(R"({"a": 1, "b": 21, "c": 31})"));
    ASSIGN_OR_ABORT(auto jv2, JsonValue::parse(R"({"a": 2, "b": 22, "d": 32})"));
    ASSIGN_OR_ABORT(auto jv3, JsonValue::parse(R"({"a": 3, "b": 23, "e": [1,2,3]})"));
    ASSIGN_OR_ABORT(auto jv4, JsonValue::parse(R"({"a": 4, "b": 24, "g": {"x": 1}})"));
    ASSIGN_OR_ABORT(auto jv5, JsonValue::parse(R"({"a": 5, "b": 25})"));

    json_col->append(&jv1);
    json_col->append(&jv2);
    json_col->append(&jv3);
    json_col->append(&jv4);
    json_col->append(&jv5);

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, nullptr);

    EXPECT_EQ(3, writer_opts.meta->children_columns_size());
    EXPECT_TRUE(writer_opts.meta->json_meta().is_flat());
    EXPECT_TRUE(writer_opts.meta->json_meta().has_remain());
    EXPECT_EQ("a", writer_opts.meta->children_columns(0).name());
    EXPECT_EQ("b", writer_opts.meta->children_columns(1).name());
    EXPECT_EQ("remain", writer_opts.meta->children_columns(2).name());

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    EXPECT_EQ(R"({"a": 1, "b": 21, "c": 31})", read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 2, "b": 22, "d": 32})", read_col->debug_item(1));
    EXPECT_EQ(R"({"a": 3, "b": 23, "e": [1, 2, 3]})", read_col->debug_item(2));
    EXPECT_EQ(R"({"a": 4, "b": 24, "g": {"x": 1}})", read_col->debug_item(3));
    EXPECT_EQ(R"({"a": 5, "b": 25})", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnRWTest, testMergeRemainFlatJson2) {
    ColumnPtr write_col = JsonColumn::create();
    auto* json_col = down_cast<JsonColumn*>(write_col.get());
    std::vector<std::string> json = {
            R"({"a": 1, "b": {"b1": 22, "b2": {"b3": "abc"}, "b4": 1}, "c": 31})",
            R"({"a": 2, "b": {"b1": 23, "b2": {"b3": "efg"}, "b4": [1, 2, 3]}, "d": 32})",
            R"({"a": 3, "b": {"b1": 24, "b2": {"b3": "xyz"}, "b4": {"b5": 1}}, "e": [1, 2, 3]})",
            R"({"a": 4, "b": {"b1": 25, "b2": {"b3": "qwe"}, "b4": {"b7": 2}}, "g": {"x": 1}})",
            R"({"a": 5, "b": {"b1": 26, "b2": {"b3": "sdf"}, "b4": 23}})"};

    for (auto& x : json) {
        ASSIGN_OR_ABORT(auto jv, JsonValue::parse(x));
        json_col->append(jv);
    }

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, nullptr);

    EXPECT_EQ(5, writer_opts.meta->children_columns_size());
    EXPECT_TRUE(writer_opts.meta->json_meta().is_flat());
    EXPECT_TRUE(writer_opts.meta->json_meta().has_remain());
    EXPECT_EQ("a", writer_opts.meta->children_columns(0).name());
    EXPECT_EQ("b.b1", writer_opts.meta->children_columns(1).name());
    EXPECT_EQ("b.b2.b3", writer_opts.meta->children_columns(2).name());
    EXPECT_EQ("b.b4", writer_opts.meta->children_columns(3).name());
    EXPECT_EQ("remain", writer_opts.meta->children_columns(4).name());

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());

    for (size_t i = 0; i < json.size(); i++) {
        EXPECT_EQ(json[i], read_col->debug_item(i));
    }
}

TEST_F(FlatJsonColumnRWTest, testMergeMiddleRemainFlatJson) {
    ColumnPtr write_col = JsonColumn::create();
    auto* json_col = down_cast<JsonColumn*>(write_col.get());

    ASSIGN_OR_ABORT(auto jv1, JsonValue::parse(R"({"a": 1, "b": 21, "c": 31})"));
    ASSIGN_OR_ABORT(auto jv2, JsonValue::parse(R"({"a": 2, "b": 22, "d": 32})"));
    ASSIGN_OR_ABORT(auto jv3, JsonValue::parse(R"({"a": 3, "b": 23, "e": [1,2,3]})"));
    ASSIGN_OR_ABORT(auto jv4, JsonValue::parse(R"({"a": 4, "b": 24, "g": {"x": 1}})"));
    ASSIGN_OR_ABORT(auto jv5, JsonValue::parse(R"({"a": 5, "b": 25})"));

    json_col->append(&jv1);
    json_col->append(&jv2);
    json_col->append(&jv3);
    json_col->append(&jv4);
    json_col->append(&jv5);

    ASSIGN_OR_ABORT(auto root_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ASSIGN_OR_ABORT(auto f1_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root.a", 0));
    ASSIGN_OR_ABORT(auto f2_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root.c", 0));
    root_path->children().emplace_back(std::move(f1_path));
    root_path->children().emplace_back(std::move(f2_path));

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, root_path.get());

    auto* read_json = down_cast<JsonColumn*>(read_col.get());

    EXPECT_EQ(3, writer_opts.meta->children_columns_size());
    EXPECT_TRUE(writer_opts.meta->json_meta().is_flat());
    EXPECT_TRUE(writer_opts.meta->json_meta().has_remain());
    EXPECT_EQ("a", writer_opts.meta->children_columns(0).name());
    EXPECT_EQ("b", writer_opts.meta->children_columns(1).name());
    EXPECT_EQ("remain", writer_opts.meta->children_columns(2).name());

    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    EXPECT_EQ("{a: 1, c: 31}", read_col->debug_item(0));
    EXPECT_EQ("{a: 2, c: NULL}", read_col->debug_item(1));
    EXPECT_EQ("{a: 3, c: NULL}", read_col->debug_item(2));
    EXPECT_EQ("{a: 4, c: NULL}", read_col->debug_item(3));
    EXPECT_EQ("{a: 5, c: NULL}", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnRWTest, testMergeMiddleRemainFlatJson2) {
    config::json_flat_null_factor = 0.4;
    ColumnPtr write_col = JsonColumn::create();
    auto* json_col = down_cast<JsonColumn*>(write_col.get());
    std::vector<std::string> json = {
            R"({"a": 1, "b": {"b1": 22, "b2": {"b3": "abc"}, "b4": 1}, "c": 31})",
            R"({"a": 2, "b": {"b1": 23, "b2": {"b3": "efg"}, "b4": [1, 2, 3]}, "d": 32})",
            R"({"a": 3, "b": {"b1": 24, "b2": {"b3": "xyz"}, "b4": {"b5": 1}}, "e": [1, 2, 3]})",
            R"({"a": 4, "b": {"b1": 25, "b2": {"b3": "qwe"}, "b4": {"b7": 2}}, "g": {"x": 1}})",
            R"({"a": 5, "b": {"b1": 26, "b2": {"b3": "sdf"}, "b4": 23}})"};

    for (auto& x : json) {
        ASSIGN_OR_ABORT(auto jv, JsonValue::parse(x));
        json_col->append(jv);
    }

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, nullptr);

    EXPECT_EQ(5, writer_opts.meta->children_columns_size());
    EXPECT_TRUE(writer_opts.meta->json_meta().is_flat());
    EXPECT_TRUE(writer_opts.meta->json_meta().has_remain());
    EXPECT_EQ("a", writer_opts.meta->children_columns(0).name());
    EXPECT_EQ("b.b1", writer_opts.meta->children_columns(1).name());
    EXPECT_EQ("b.b2.b3", writer_opts.meta->children_columns(2).name());
    EXPECT_EQ("b.b4", writer_opts.meta->children_columns(3).name());
    EXPECT_EQ("remain", writer_opts.meta->children_columns(4).name());

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    for (size_t i = 0; i < json.size(); i++) {
        EXPECT_EQ(json[i], read_col->debug_item(i));
    }
}

TEST_F(FlatJsonColumnRWTest, testMergeMiddleRemainFlatJson3) {
    config::json_flat_null_factor = 0.4;
    ColumnPtr write_col = JsonColumn::create();
    auto* json_col = down_cast<JsonColumn*>(write_col.get());
    std::vector<std::string> json = {
            R"({"a": 1, "b": {"b1": 22, "b2": {"b3": "abc", "c1": {"c2": "a", "ce": 1},"bc": 1}, "b4": 1}})",
            R"({"a": 2, "b": {"b1": 23, "b2": {"b3": "efg", "c1": {"c2": "b", "cd": 2},"bd": 2}, "b4": [1, 2, 3]}})",
            R"({"a": 3, "b": {"b1": 24, "b2": {"b3": "xyz", "c1": {"c2": "c", "cf": 3},"be": 3}, "b4": {"b5": 1}}})",
            R"({"a": 4, "b": {"b1": 25, "b2": {"b3": "qwe", "c1": {"c2": "d", "cg": 4},"bf": 4}, "b4": {"b7": 2}}})",
            R"({"a": 5, "b": {"b1": 26, "b2": {"b3": "sdf", "c1": {"c2": "e", "ch": 5},"bg": 5}, "b4": 23}})"};

    for (auto& x : json) {
        ASSIGN_OR_ABORT(auto jv, JsonValue::parse(x));
        json_col->append(jv);
    }

    ASSIGN_OR_ABORT(auto root_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ASSIGN_OR_ABORT(auto b_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root.b", 0));
    ASSIGN_OR_ABORT(auto b2_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root.b.b2", 0));

    b_path->children().emplace_back(std::move(b2_path));
    root_path->children().emplace_back(std::move(b_path));

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, root_path.get());

    EXPECT_EQ(6, writer_opts.meta->children_columns_size());
    EXPECT_TRUE(writer_opts.meta->json_meta().is_flat());
    EXPECT_TRUE(writer_opts.meta->json_meta().has_remain());
    EXPECT_EQ("a", writer_opts.meta->children_columns(0).name());
    EXPECT_EQ("b.b1", writer_opts.meta->children_columns(1).name());
    EXPECT_EQ("b.b2.b3", writer_opts.meta->children_columns(2).name());
    EXPECT_EQ("b.b2.c1.c2", writer_opts.meta->children_columns(3).name());
    EXPECT_EQ("b.b4", writer_opts.meta->children_columns(4).name());
    EXPECT_EQ("remain", writer_opts.meta->children_columns(5).name());

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    EXPECT_EQ(R"({b.b2: {"b3": "abc", "bc": 1, "c1": {"c2": "a", "ce": 1}}})", read_col->debug_item(0));
    EXPECT_EQ(R"({b.b2: {"b3": "efg", "bd": 2, "c1": {"c2": "b", "cd": 2}}})", read_col->debug_item(1));
    EXPECT_EQ(R"({b.b2: {"b3": "xyz", "be": 3, "c1": {"c2": "c", "cf": 3}}})", read_col->debug_item(2));
    EXPECT_EQ(R"({b.b2: {"b3": "qwe", "bf": 4, "c1": {"c2": "d", "cg": 4}}})", read_col->debug_item(3));
    EXPECT_EQ(R"({b.b2: {"b3": "sdf", "bg": 5, "c1": {"c2": "e", "ch": 5}}})", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnRWTest, testDeepFlatJson) {
    ColumnPtr write_col = JsonColumn::create();
    auto* json_col = down_cast<JsonColumn*>(write_col.get());
    std::vector<std::string> json = {
            R"({"a": 1, "b": {"b1": 22, "b2": {"b3": "abc"}, "b4": 1}, "c": 31})",
            R"({"a": 2, "b": {"b1": 23, "b2": {"b3": "efg"}, "b4": [1,2,3]}, "d": 32})",
            R"({"a": 3, "b": {"b1": 24, "b2": {"b3": "xyz"}, "b4": {"b5": 1}}, "e": [1,2,3]})",
            R"({"a": 4, "b": {"b1": 25, "b2": {"b3": "qwe"}, "b4": {"b7": 2}}, "g": {"x": 1}})",
            R"({"a": 5, "b": {"b1": 26, "b2": {"b3": "sdf"}, "b4": 23}})"};

    for (auto& x : json) {
        ASSIGN_OR_ABORT(auto jv, JsonValue::parse(x));
        json_col->append(jv);
    }

    ASSIGN_OR_ABORT(auto root, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "b.b4.b5");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "b.b2.b3");

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, root.get());

    EXPECT_EQ(5, writer_opts.meta->children_columns_size());
    EXPECT_TRUE(writer_opts.meta->json_meta().is_flat());
    EXPECT_TRUE(writer_opts.meta->json_meta().has_remain());
    EXPECT_EQ("a", writer_opts.meta->children_columns(0).name());
    EXPECT_EQ("b.b1", writer_opts.meta->children_columns(1).name());
    EXPECT_EQ("b.b2.b3", writer_opts.meta->children_columns(2).name());
    EXPECT_EQ("b.b4", writer_opts.meta->children_columns(3).name());
    EXPECT_EQ("remain", writer_opts.meta->children_columns(4).name());

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: "abc"})", read_col->debug_item(0));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: "efg"})", read_col->debug_item(1));
    EXPECT_EQ(R"({b.b4.b5: 1, b.b2.b3: "xyz"})", read_col->debug_item(2));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: "qwe"})", read_col->debug_item(3));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: "sdf"})", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnRWTest, testHyperFlatJson) {
    config::json_flat_null_factor = 0.4;
    config::json_flat_sparsity_factor = 0.5;

    ColumnPtr write_col = JsonColumn::create();
    auto* json_col = down_cast<JsonColumn*>(write_col.get());
    std::vector<std::string> json = {
            R"({"a": 1, "gg": "te1", "ff": {"f1": "985"}, "b": {"b1": 22, "b2": {"b3": "abc", "c1": {"c2": "a", "ce": 1},"bc": 1}, "b4": 1}})",
            R"({"a": 2, "gg": "te2", "ff": {"f1": "984"}, "b": {"b1": 23, "b2": {"b3": "efg", "c1": {"c2": "b", "cd": 2},"bd": 2}, "b4": [1, 2, 3]}})",
            R"({"a": 3, "gg": "te3", "ff": {"f1": "983"}, "b": {"b1": 24, "b2": {"b3": "xyz", "c1": {"c2": "c", "cf": 3},"be": 3}, "b4": {"b5": 1}}})",
            R"({"a": 4, "gg": "te4", "ff": 781, "b": {"b1": 25, "b2": {"b3": "qwe", "c1": {"c2": "d", "cg": 4},"bf": 4}, "b4": {"b7": 2}}})",
            R"({"a": 5, "gg": "te5", "ff": 782, "b": {"b1": 26, "b2": {"b3": "sdf", "c1": {"c2": "e", "ch": 5},"bg": 5}, "b4": 23}})"};

    for (auto& x : json) {
        ASSIGN_OR_ABORT(auto jv, JsonValue::parse(x));
        json_col->append(jv);
    }

    ASSIGN_OR_ABORT(auto root, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "b.b4.b5");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "b.b2.b3");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "a");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "ff.f1");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "gg.g1");

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, root.get());

    int index = 0;
    EXPECT_EQ(8, writer_opts.meta->children_columns_size());
    EXPECT_TRUE(writer_opts.meta->json_meta().is_flat());
    EXPECT_TRUE(writer_opts.meta->json_meta().has_remain());
    EXPECT_EQ("a", writer_opts.meta->children_columns(index++).name());
    EXPECT_EQ("b.b1", writer_opts.meta->children_columns(index++).name());
    EXPECT_EQ("b.b2.b3", writer_opts.meta->children_columns(index++).name());
    EXPECT_EQ("b.b2.c1.c2", writer_opts.meta->children_columns(index++).name());
    EXPECT_EQ("b.b4", writer_opts.meta->children_columns(index++).name());
    EXPECT_EQ("ff.f1", writer_opts.meta->children_columns(index++).name());
    EXPECT_EQ("gg", writer_opts.meta->children_columns(index++).name());
    EXPECT_EQ("remain", writer_opts.meta->children_columns(index++).name());

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: "abc", a: 1, ff.f1: "985", gg.g1: NULL})", read_col->debug_item(0));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: "efg", a: 2, ff.f1: "984", gg.g1: NULL})", read_col->debug_item(1));
    EXPECT_EQ(R"({b.b4.b5: 1, b.b2.b3: "xyz", a: 3, ff.f1: "983", gg.g1: NULL})", read_col->debug_item(2));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: "qwe", a: 4, ff.f1: NULL, gg.g1: NULL})", read_col->debug_item(3));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: "sdf", a: 5, ff.f1: NULL, gg.g1: NULL})", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnRWTest, testMergeRemainJson) {
    ColumnPtr write_col = JsonColumn::create();
    auto* json_col = down_cast<JsonColumn*>(write_col.get());

    ASSIGN_OR_ABORT(auto jv1, JsonValue::parse(R"({"a": 1, "b": 21, "c": 31})"));
    ASSIGN_OR_ABORT(auto jv2, JsonValue::parse(R"({"a": 2, "b": 22, "d": 32})"));
    ASSIGN_OR_ABORT(auto jv3, JsonValue::parse(R"({"a": 3, "b": 23, "e": [1,2,3]})"));
    ASSIGN_OR_ABORT(auto jv4, JsonValue::parse(R"({"a": 4, "b": 24, "g": {"x": 1}})"));
    ASSIGN_OR_ABORT(auto jv5, JsonValue::parse(R"({"a": 5, "b": 25})"));

    json_col->append(&jv1);
    json_col->append(&jv2);
    json_col->append(&jv3);
    json_col->append(&jv4);
    json_col->append(&jv5);

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, nullptr);

    EXPECT_EQ(0, writer_opts.meta->children_columns_size());
    EXPECT_FALSE(writer_opts.meta->json_meta().is_flat());
    EXPECT_FALSE(writer_opts.meta->json_meta().has_remain());

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    EXPECT_EQ(R"({"a": 1, "b": 21, "c": 31})", read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 2, "b": 22, "d": 32})", read_col->debug_item(1));
    EXPECT_EQ(R"({"a": 3, "b": 23, "e": [1, 2, 3]})", read_col->debug_item(2));
    EXPECT_EQ(R"({"a": 4, "b": 24, "g": {"x": 1}})", read_col->debug_item(3));
    EXPECT_EQ(R"({"a": 5, "b": 25})", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnRWTest, testMergeMiddleRemainJson) {
    ColumnPtr write_col = JsonColumn::create();
    auto* json_col = down_cast<JsonColumn*>(write_col.get());

    ASSIGN_OR_ABORT(auto jv1, JsonValue::parse(R"({"a": 1, "b": 21, "c": 31})"));
    ASSIGN_OR_ABORT(auto jv2, JsonValue::parse(R"({"a": 2, "b": 22, "d": 32})"));
    ASSIGN_OR_ABORT(auto jv3, JsonValue::parse(R"({"a": 3, "b": 23, "e": [1,2,3]})"));
    ASSIGN_OR_ABORT(auto jv4, JsonValue::parse(R"({"a": 4, "b": 24, "g": {"x": 1}})"));
    ASSIGN_OR_ABORT(auto jv5, JsonValue::parse(R"({"a": 5, "b": 25})"));

    json_col->append(&jv1);
    json_col->append(&jv2);
    json_col->append(&jv3);
    json_col->append(&jv4);
    json_col->append(&jv5);

    ASSIGN_OR_ABORT(auto root_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ASSIGN_OR_ABORT(auto f1_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root.a", 0));
    ASSIGN_OR_ABORT(auto f2_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root.c", 0));
    root_path->children().emplace_back(std::move(f1_path));
    root_path->children().emplace_back(std::move(f2_path));

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, root_path.get());

    EXPECT_EQ(0, writer_opts.meta->children_columns_size());
    EXPECT_FALSE(writer_opts.meta->json_meta().is_flat());
    EXPECT_FALSE(writer_opts.meta->json_meta().has_remain());

    auto* read_json = down_cast<JsonColumn*>(read_col.get());

    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    EXPECT_EQ("{a: 1, c: 31}", read_col->debug_item(0));
    EXPECT_EQ("{a: 2, c: NULL}", read_col->debug_item(1));
    EXPECT_EQ("{a: 3, c: NULL}", read_col->debug_item(2));
    EXPECT_EQ("{a: 4, c: NULL}", read_col->debug_item(3));
    EXPECT_EQ("{a: 5, c: NULL}", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnRWTest, testMergeMiddleRemainJson2) {
    config::json_flat_null_factor = 0.4;
    ColumnPtr write_col = JsonColumn::create();
    auto* json_col = down_cast<JsonColumn*>(write_col.get());
    std::vector<std::string> json = {
            R"({"a": 1, "b": {"b1": 22, "b2": {"b3": "abc", "c1": {"c2": "a", "ce": 1},"bc": 1}, "b4": 1}})",
            R"({"a": 2, "b": {"b1": 23, "b2": {"b3": "efg", "c1": {"c2": "b", "cd": 2},"bd": 2}, "b4": [1, 2, 3]}})",
            R"({"a": 3, "b": {"b1": 24, "b2": {"b3": "xyz", "c1": {"c2": "c", "cf": 3},"be": 3}, "b4": {"b5": 1}}})",
            R"({"a": 4, "b": {"b1": 25, "b2": {"b3": "qwe", "c1": {"c2": "d", "cg": 4},"bf": 4}, "b4": {"b7": 2}}})",
            R"({"a": 5, "b": {"b1": 26, "b2": {"b3": "sdf", "c1": {"c2": "e", "ch": 5},"bg": 5}, "b4": 23}})"};

    for (auto& x : json) {
        ASSIGN_OR_ABORT(auto jv, JsonValue::parse(x));
        json_col->append(jv);
    }

    ASSIGN_OR_ABORT(auto root_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ASSIGN_OR_ABORT(auto b_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root.b", 0));
    ASSIGN_OR_ABORT(auto b2_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root.b.b2", 0));

    b_path->children().emplace_back(std::move(b2_path));
    root_path->children().emplace_back(std::move(b_path));

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, root_path.get());

    EXPECT_EQ(0, writer_opts.meta->children_columns_size());
    EXPECT_FALSE(writer_opts.meta->json_meta().is_flat());
    EXPECT_FALSE(writer_opts.meta->json_meta().has_remain());

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    EXPECT_EQ(R"({b.b2: {"b3": "abc", "bc": 1, "c1": {"c2": "a", "ce": 1}}})", read_col->debug_item(0));
    EXPECT_EQ(R"({b.b2: {"b3": "efg", "bd": 2, "c1": {"c2": "b", "cd": 2}}})", read_col->debug_item(1));
    EXPECT_EQ(R"({b.b2: {"b3": "xyz", "be": 3, "c1": {"c2": "c", "cf": 3}}})", read_col->debug_item(2));
    EXPECT_EQ(R"({b.b2: {"b3": "qwe", "bf": 4, "c1": {"c2": "d", "cg": 4}}})", read_col->debug_item(3));
    EXPECT_EQ(R"({b.b2: {"b3": "sdf", "bg": 5, "c1": {"c2": "e", "ch": 5}}})", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnRWTest, testDeepJson) {
    ColumnPtr write_col = JsonColumn::create();
    auto* json_col = down_cast<JsonColumn*>(write_col.get());
    std::vector<std::string> json = {
            R"({"a": 1, "b": {"b1": 22, "b2": {"b3": "abc"}, "b4": 1}, "c": 31})",
            R"({"a": 2, "b": {"b1": 23, "b2": {"b3": "efg"}, "b4": [1,2,3]}, "d": 32})",
            R"({"a": 3, "b": {"b1": 24, "b2": {"b3": "xyz"}, "b4": {"b5": 1}}, "e": [1,2,3]})",
            R"({"a": 4, "b": {"b1": 25, "b2": {"b3": "qwe"}, "b4": {"b7": 2}}, "g": {"x": 1}})",
            R"({"a": 5, "b": {"b1": 26, "b2": {"b3": "sdf"}, "b4": 23}})"};

    for (auto& x : json) {
        ASSIGN_OR_ABORT(auto jv, JsonValue::parse(x));
        json_col->append(jv);
    }

    ASSIGN_OR_ABORT(auto root, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "b.b4.b5");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "b.b2.b3");

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, root.get());

    EXPECT_EQ(0, writer_opts.meta->children_columns_size());
    EXPECT_FALSE(writer_opts.meta->json_meta().is_flat());
    EXPECT_FALSE(writer_opts.meta->json_meta().has_remain());

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: "abc"})", read_col->debug_item(0));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: "efg"})", read_col->debug_item(1));
    EXPECT_EQ(R"({b.b4.b5: 1, b.b2.b3: "xyz"})", read_col->debug_item(2));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: "qwe"})", read_col->debug_item(3));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: "sdf"})", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnRWTest, testHyperJson) {
    config::json_flat_null_factor = 0.4;
    config::json_flat_sparsity_factor = 0.5;

    ColumnPtr write_col = JsonColumn::create();
    auto* json_col = down_cast<JsonColumn*>(write_col.get());
    std::vector<std::string> json = {
            R"({"a": 1, "gg": "te1", "ff": {"f1": "985"}, "b": {"b1": 22, "b2": {"b3": "abc", "c1": {"c2": "a", "ce": 1},"bc": 1}, "b4": 1}})",
            R"({"a": 2, "gg": "te2", "ff": {"f1": "984"}, "b": {"b1": 23, "b2": {"b3": "efg", "c1": {"c2": "b", "cd": 2},"bd": 2}, "b4": [1, 2, 3]}})",
            R"({"a": 3, "gg": "te3", "ff": {"f1": "983"}, "b": {"b1": 24, "b2": {"b3": "xyz", "c1": {"c2": "c", "cf": 3},"be": 3}, "b4": {"b5": 1}}})",
            R"({"a": 4, "gg": "te4", "ff": 781, "b": {"b1": 25, "b2": {"b3": "qwe", "c1": {"c2": "d", "cg": 4},"bf": 4}, "b4": {"b7": 2}}})",
            R"({"a": 5, "gg": "te5", "ff": 782, "b": {"b1": 26, "b2": {"b3": "sdf", "c1": {"c2": "e", "ch": 5},"bg": 5}, "b4": 23}})"};

    for (auto& x : json) {
        ASSIGN_OR_ABORT(auto jv, JsonValue::parse(x));
        json_col->append(jv);
    }

    ASSIGN_OR_ABORT(auto root, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "b.b4.b5");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "b.b2.b3");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "a");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "ff.f1");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "gg.g1");

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, root.get());

    EXPECT_EQ(0, writer_opts.meta->children_columns_size());
    EXPECT_FALSE(writer_opts.meta->json_meta().is_flat());
    EXPECT_FALSE(writer_opts.meta->json_meta().has_remain());

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: "abc", a: 1, ff.f1: "985", gg.g1: NULL})", read_col->debug_item(0));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: "efg", a: 2, ff.f1: "984", gg.g1: NULL})", read_col->debug_item(1));
    EXPECT_EQ(R"({b.b4.b5: 1, b.b2.b3: "xyz", a: 3, ff.f1: "983", gg.g1: NULL})", read_col->debug_item(2));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: "qwe", a: 4, ff.f1: NULL, gg.g1: NULL})", read_col->debug_item(3));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: "sdf", a: 5, ff.f1: NULL, gg.g1: NULL})", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnRWTest, testHyperNoCastTypeJson) {
    config::json_flat_null_factor = 0.4;
    config::json_flat_sparsity_factor = 0.5;

    ColumnPtr write_col = JsonColumn::create();
    auto* json_col = down_cast<JsonColumn*>(write_col.get());
    std::vector<std::string> json = {
            R"({"a": 1, "gg": "te1", "ff": {"f1": "985"}, "b": {"b1": 22, "b2": {"b3": "abc", "c1": {"c2": "a", "ce": 1},"bc": 1}, "b4": 1}})",
            R"({"a": 2, "gg": "te2", "ff": {"f1": "984"}, "b": {"b1": 23, "b2": {"b3": "efg", "c1": {"c2": "b", "cd": 2},"bd": 2}, "b4": [1, 2, 3]}})",
            R"({"a": 3, "gg": "te3", "ff": {"f1": "983"}, "b": {"b1": 24, "b2": {"b3": "xyz", "c1": {"c2": "c", "cf": 3},"be": 3}, "b4": {"b5": 1}}})",
            R"({"a": 4, "gg": "te4", "ff": 781, "b": {"b1": 25, "b2": {"b3": "qwe", "c1": {"c2": "d", "cg": 4},"bf": 4}, "b4": {"b7": 2}}})",
            R"({"a": 5, "gg": "te5", "ff": 782, "b": {"b1": 26, "b2": {"b3": "sdf", "c1": {"c2": "e", "ch": 5},"bg": 5}, "b4": 23}})"};

    for (auto& x : json) {
        ASSIGN_OR_ABORT(auto jv, JsonValue::parse(x));
        json_col->append(jv);
    }

    ASSIGN_OR_ABORT(auto root, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_BIGINT, "b.b4.b5");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_VARCHAR, "b.b2.b3");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_BIGINT, "a");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "ff.f1");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_VARCHAR, "gg.g1");

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, root.get());

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: 'abc', a: 1, ff.f1: "985", gg.g1: NULL})", read_col->debug_item(0));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: 'efg', a: 2, ff.f1: "984", gg.g1: NULL})", read_col->debug_item(1));
    EXPECT_EQ(R"({b.b4.b5: 1, b.b2.b3: 'xyz', a: 3, ff.f1: "983", gg.g1: NULL})", read_col->debug_item(2));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: 'qwe', a: 4, ff.f1: NULL, gg.g1: NULL})", read_col->debug_item(3));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: 'sdf', a: 5, ff.f1: NULL, gg.g1: NULL})", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnRWTest, testHyperCastTypeJson) {
    config::json_flat_null_factor = 0.4;
    config::json_flat_sparsity_factor = 0.5;

    ColumnPtr write_col = JsonColumn::create();
    auto* json_col = down_cast<JsonColumn*>(write_col.get());
    std::vector<std::string> json = {
            R"({"a": 1, "gg": "te1", "ff": {"f1": "985"}, "b": {"b1": 22, "b2": {"b3": "abc", "c1": {"c2": "a", "ce": 1},"bc": 1}, "b4": 1}})",
            R"({"a": 2, "gg": "te2", "ff": {"f1": "984"}, "b": {"b1": 23, "b2": {"b3": "efg", "c1": {"c2": "b", "cd": 2},"bd": 2}, "b4": [1, 2, 3]}})",
            R"({"a": 3, "gg": "te3", "ff": {"f1": "983"}, "b": {"b1": 24, "b2": {"b3": "xyz", "c1": {"c2": "c", "cf": 3},"be": 3}, "b4": {"b5": 1}}})",
            R"({"a": 4, "gg": "te4", "ff": 781, "b": {"b1": 25, "b2": {"b3": "qwe", "c1": {"c2": "d", "cg": 4},"bf": 4}, "b4": {"b7": 2}}})",
            R"({"a": 5, "gg": "te5", "ff": 782, "b": {"b1": 26, "b2": {"b3": "sdf", "c1": {"c2": "e", "ch": 5},"bg": 5}, "b4": 23}})"};

    for (auto& x : json) {
        ASSIGN_OR_ABORT(auto jv, JsonValue::parse(x));
        json_col->append(jv);
    }

    ASSIGN_OR_ABORT(auto root, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_DOUBLE, "b.b4.b5");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_BIGINT, "b.b2");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_VARCHAR, "a");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_BIGINT, "ff.f1");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "gg.g1");

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, root.get());

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2: NULL, a: '1', ff.f1: 985, gg.g1: NULL})", read_col->debug_item(0));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2: NULL, a: '2', ff.f1: 984, gg.g1: NULL})", read_col->debug_item(1));
    EXPECT_EQ(R"({b.b4.b5: 1, b.b2: NULL, a: '3', ff.f1: 983, gg.g1: NULL})", read_col->debug_item(2));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2: NULL, a: '4', ff.f1: NULL, gg.g1: NULL})", read_col->debug_item(3));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2: NULL, a: '5', ff.f1: NULL, gg.g1: NULL})", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnRWTest, testHyperCastTypeJson2) {
    config::json_flat_null_factor = 0.4;
    config::json_flat_sparsity_factor = 0.5;

    ColumnPtr write_col = JsonColumn::create();
    auto* json_col = down_cast<JsonColumn*>(write_col.get());
    std::vector<std::string> json = {
            R"({"a": 1, "gg": "te1", "ff": {"f1": "985"}, "b": {"b1": 22, "b2": {"b3": "abc", "c1": {"c2": "a", "ce": 1},"bc": 1}, "b4": 1}})",
            R"({"a": 2, "gg": "te2", "ff": {"f1": "984"}, "b": {"b1": 23, "b2": {"b3": "efg", "c1": {"c2": "b", "cd": 2},"bd": 2}, "b4": [1, 2, 3]}})",
            R"({"a": 3, "gg": "te3", "ff": {"f1": "983"}, "b": {"b1": 24, "b2": {"b3": "xyz", "c1": {"c2": "c", "cf": 3},"be": 3}, "b4": {"b5": 1}}})",
            R"({"a": 4, "gg": "te4", "ff": 781, "b": {"b1": 25, "b2": {"b3": "qwe", "c1": {"c2": "d", "cg": 4},"bf": 4}, "b4": {"b7": 2}}})",
            R"({"a": 5, "gg": "te5", "ff": 782, "b": {"b1": 26, "b2": {"b3": "sdf", "c1": {"c2": "e", "ch": 5},"bg": 5}, "b4": 23}})"};

    for (auto& x : json) {
        ASSIGN_OR_ABORT(auto jv, JsonValue::parse(x));
        json_col->append(jv);
    }

    ASSIGN_OR_ABORT(auto root, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_DOUBLE, "b.b4.b5");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_VARCHAR, "b.b2");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_VARCHAR, "a");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_BIGINT, "ff.f1");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "gg.g1");

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, root.get());

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    EXPECT_EQ(
            R"({b.b4.b5: NULL, b.b2: '{"b3": "abc", "bc": 1, "c1": {"c2": "a", "ce": 1}}', a: '1', ff.f1: 985, gg.g1: NULL})",
            read_col->debug_item(0));
    EXPECT_EQ(
            R"({b.b4.b5: NULL, b.b2: '{"b3": "efg", "bd": 2, "c1": {"c2": "b", "cd": 2}}', a: '2', ff.f1: 984, gg.g1: NULL})",
            read_col->debug_item(1));
    EXPECT_EQ(
            R"({b.b4.b5: 1, b.b2: '{"b3": "xyz", "be": 3, "c1": {"c2": "c", "cf": 3}}', a: '3', ff.f1: 983, gg.g1: NULL})",
            read_col->debug_item(2));
    EXPECT_EQ(
            R"({b.b4.b5: NULL, b.b2: '{"b3": "qwe", "bf": 4, "c1": {"c2": "d", "cg": 4}}', a: '4', ff.f1: NULL, gg.g1: NULL})",
            read_col->debug_item(3));
    EXPECT_EQ(
            R"({b.b4.b5: NULL, b.b2: '{"b3": "sdf", "bg": 5, "c1": {"c2": "e", "ch": 5}}', a: '5', ff.f1: NULL, gg.g1: NULL})",
            read_col->debug_item(4));
}

TEST_F(FlatJsonColumnRWTest, testMergeRemainNullFlatJson) {
    config::json_flat_null_factor = 1;
    config::json_flat_sparsity_factor = 0.4;
    // clang-format off
    std::vector<std::string> jsons = {
        R"({"a": 1, "b": 21, "c": 31})",
        R"({"a": 2, "b": 22, "d": 32})",
        R"({"a": 3, "b": 23, "e": [1,2,3]})",
        R"({"a": 4, "b": 24, "g": {"x": 1}})",
        R"(NULL)"
    };
    // clang-format on
    auto write_col = create_json(jsons, true);
    ColumnPtr read_col = write_col->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, nullptr);

    EXPECT_EQ(4, writer_opts.meta->children_columns_size());
    EXPECT_TRUE(writer_opts.meta->json_meta().is_flat());
    EXPECT_TRUE(writer_opts.meta->json_meta().has_remain());
    EXPECT_EQ("nulls", writer_opts.meta->children_columns(0).name());
    EXPECT_EQ("a", writer_opts.meta->children_columns(1).name());
    EXPECT_EQ("b", writer_opts.meta->children_columns(2).name());
    EXPECT_EQ("remain", writer_opts.meta->children_columns(3).name());

    auto* read_json = down_cast<JsonColumn*>(down_cast<NullableColumn*>(read_col.get())->data_column().get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    EXPECT_EQ(R"({"a": 1, "b": 21, "c": 31})", read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 2, "b": 22, "d": 32})", read_col->debug_item(1));
    EXPECT_EQ(R"({"a": 3, "b": 23, "e": [1, 2, 3]})", read_col->debug_item(2));
    EXPECT_EQ(R"({"a": 4, "b": 24, "g": {"x": 1}})", read_col->debug_item(3));
    EXPECT_EQ(R"(NULL)", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnRWTest, testMergeRemainNullFlatJson1) {
    config::json_flat_null_factor = 0;
    config::json_flat_sparsity_factor = 0.4;
    // clang-format off
    std::vector<std::string> jsons = {
        R"({"a": 1, "b": 21, "c": 31})",
        R"({"a": 2, "b": 22, "d": 32})",
        R"({"a": 3, "b": 23, "e": [1,2,3]})",
        R"({})",
        R"(NULL)"
    };
    // clang-format on
    auto write_col = create_json(jsons, true);
    ColumnPtr read_col = write_col->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, nullptr);

    EXPECT_EQ(0, writer_opts.meta->children_columns_size());
    EXPECT_FALSE(writer_opts.meta->json_meta().is_flat());
    EXPECT_FALSE(writer_opts.meta->json_meta().has_remain());

    auto* read_json = down_cast<JsonColumn*>(down_cast<NullableColumn*>(read_col.get())->data_column().get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    EXPECT_EQ(R"({"a": 1, "b": 21, "c": 31})", read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 2, "b": 22, "d": 32})", read_col->debug_item(1));
    EXPECT_EQ(R"({"a": 3, "b": 23, "e": [1, 2, 3]})", read_col->debug_item(2));
    EXPECT_EQ(R"({})", read_col->debug_item(3));
    EXPECT_EQ(R"(NULL)", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnRWTest, testMergeRemainNullFlatJson2) {
    config::json_flat_null_factor = 1;
    config::json_flat_sparsity_factor = 0.4;

    std::vector<std::string> jsons = {
            R"({"a": 1, "b": {"b1": 22, "b2": {"b3": "abc"}, "b4": 1}, "c": 31})",
            R"({"a": 2, "b": {"b1": 23, "b2": {"b3": "efg"}, "b4": [1, 2, 3]}, "d": 32})",
            R"({"a": 3, "b": {"b1": 24, "b2": {"b3": "xyz"}, "b4": {"b5": 1}}, "e": [1, 2, 3]})", R"(NULL)",
            R"({"a": 5, "b": {}})"};
    auto write_col = create_json(jsons, true);
    ColumnPtr read_col = write_col->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, nullptr);

    EXPECT_EQ(6, writer_opts.meta->children_columns_size());
    EXPECT_TRUE(writer_opts.meta->json_meta().is_flat());
    EXPECT_TRUE(writer_opts.meta->json_meta().has_remain());
    EXPECT_EQ("nulls", writer_opts.meta->children_columns(0).name());
    EXPECT_EQ("a", writer_opts.meta->children_columns(1).name());
    EXPECT_EQ("b.b1", writer_opts.meta->children_columns(2).name());
    EXPECT_EQ("b.b2.b3", writer_opts.meta->children_columns(3).name());
    EXPECT_EQ("b.b4", writer_opts.meta->children_columns(4).name());
    EXPECT_EQ("remain", writer_opts.meta->children_columns(5).name());

    auto* read_json = down_cast<JsonColumn*>(down_cast<NullableColumn*>(read_col.get())->data_column().get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());

    for (size_t i = 0; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i], read_col->debug_item(i));
    }
}

TEST_F(FlatJsonColumnRWTest, testMergeMiddleRemainNullFlatJson) {
    config::json_flat_null_factor = 1;
    config::json_flat_sparsity_factor = 0.4;

    // clang-format off
    std::vector<std::string> jsons = {
        R"({"a": 1, "b": 21, "c": 31})",
        R"({"a": 2, "b": 22, "d": 32})",
        R"({"a": 3, "b": 23, "e": [1,2,3]})",
        R"({"a": 4, "b": 24, "g": {"x": 1}})",
        R"({})"
    };
    // clang-format on
    ColumnPtr write_col = create_json(jsons, true);

    ASSIGN_OR_ABORT(auto root_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ASSIGN_OR_ABORT(auto f1_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root.a", 0));
    ASSIGN_OR_ABORT(auto f2_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root.c", 0));
    root_path->children().emplace_back(std::move(f1_path));
    root_path->children().emplace_back(std::move(f2_path));

    ColumnPtr read_col = write_col->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, root_path.get());

    auto* read_json = down_cast<JsonColumn*>(down_cast<NullableColumn*>(read_col.get())->data_column().get());

    EXPECT_EQ(4, writer_opts.meta->children_columns_size());
    EXPECT_TRUE(writer_opts.meta->json_meta().is_flat());
    EXPECT_TRUE(writer_opts.meta->json_meta().has_remain());
    EXPECT_EQ("nulls", writer_opts.meta->children_columns(0).name());
    EXPECT_EQ("a", writer_opts.meta->children_columns(1).name());
    EXPECT_EQ("b", writer_opts.meta->children_columns(2).name());
    EXPECT_EQ("remain", writer_opts.meta->children_columns(3).name());

    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    EXPECT_EQ("{a: 1, c: 31}", read_col->debug_item(0));
    EXPECT_EQ("{a: 2, c: NULL}", read_col->debug_item(1));
    EXPECT_EQ("{a: 3, c: NULL}", read_col->debug_item(2));
    EXPECT_EQ("{a: 4, c: NULL}", read_col->debug_item(3));
    EXPECT_EQ("{a: NULL, c: NULL}", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnRWTest, testMergeMiddleRemainNullFlatJson2) {
    config::json_flat_null_factor = 1;
    config::json_flat_sparsity_factor = 0.4;
    std::vector<std::string> json = {
            R"({"a": 1, "b": {"b1": 22, "b2": {"b3": "abc"}, "b4": 1}, "c": 31})",
            R"({"a": 2, "b": {"b1": 23, "b2": {"b3": "efg"}, "b4": [1, 2, 3]}, "d": 32})",
            R"({"a": 3, "b": {"b1": 24, "b2": {"b3": "xyz"}, "b4": {"b5": 1}}, "e": [1, 2, 3]})", R"(NULL)", R"(NULL)"};
    ColumnPtr write_col = create_json(json, true);

    ColumnPtr read_col = write_col->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, nullptr);

    EXPECT_EQ(6, writer_opts.meta->children_columns_size());
    EXPECT_TRUE(writer_opts.meta->json_meta().is_flat());
    EXPECT_TRUE(writer_opts.meta->json_meta().has_remain());
    EXPECT_EQ("nulls", writer_opts.meta->children_columns(0).name());
    EXPECT_EQ("a", writer_opts.meta->children_columns(1).name());
    EXPECT_EQ("b.b1", writer_opts.meta->children_columns(2).name());
    EXPECT_EQ("b.b2.b3", writer_opts.meta->children_columns(3).name());
    EXPECT_EQ("b.b4", writer_opts.meta->children_columns(4).name());
    EXPECT_EQ("remain", writer_opts.meta->children_columns(5).name());

    auto* read_json = down_cast<JsonColumn*>(down_cast<NullableColumn*>(read_col.get())->data_column().get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    for (size_t i = 0; i < json.size(); i++) {
        EXPECT_EQ(json[i], read_col->debug_item(i));
    }
}

TEST_F(FlatJsonColumnRWTest, testMergeMiddleRemainNullFlatJson3) {
    config::json_flat_null_factor = 1;
    config::json_flat_sparsity_factor = 0.4;
    std::vector<std::string> json = {
            R"({"a": 1, "b": {"b1": 22, "b2": {"b3": "abc", "c1": {"c2": "a", "ce": 1},"bc": 1}, "b4": 1}})",
            R"({"a": 2, "b": {"b1": 23, "b2": {"b3": "efg", "c1": {"c2": "b", "cd": 2},"bd": 2}, "b4": [1, 2, 3]}})",
            R"({"a": 3, "b": {"b1": 24, "b2": {"b3": "xyz", "c1": {"c2": "c", "cf": 3},"be": 3}, "b4": {"b5": 1}}})",
            R"({"a": 4, "b": {"b1": 25, "b2": {"b3": "qwe", "c1": {"c2": "d", "cg": 4},"bf": 4}, "b4": {"b7": 2}}})",
            R"({"a": 5, "b": {"b1": 26, "b2": {"b3": "sdf", "c1": {"c2": "e", "ch": 5},"bg": 5}, "b4": 23}})"};
    ColumnPtr write_col = create_json(json, true);

    ASSIGN_OR_ABORT(auto root_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ASSIGN_OR_ABORT(auto b_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root.b", 0));
    ASSIGN_OR_ABORT(auto b2_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root.b.b2", 0));

    b_path->children().emplace_back(std::move(b2_path));
    root_path->children().emplace_back(std::move(b_path));

    ColumnPtr read_col = write_col->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, root_path.get());

    EXPECT_EQ(7, writer_opts.meta->children_columns_size());
    EXPECT_TRUE(writer_opts.meta->json_meta().is_flat());
    EXPECT_TRUE(writer_opts.meta->json_meta().has_remain());
    EXPECT_EQ("nulls", writer_opts.meta->children_columns(0).name());
    EXPECT_EQ("a", writer_opts.meta->children_columns(1).name());
    EXPECT_EQ("b.b1", writer_opts.meta->children_columns(2).name());
    EXPECT_EQ("b.b2.b3", writer_opts.meta->children_columns(3).name());
    EXPECT_EQ("b.b2.c1.c2", writer_opts.meta->children_columns(4).name());
    EXPECT_EQ("b.b4", writer_opts.meta->children_columns(5).name());
    EXPECT_EQ("remain", writer_opts.meta->children_columns(6).name());

    auto* read_json = down_cast<JsonColumn*>(down_cast<NullableColumn*>(read_col.get())->data_column().get());
    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    EXPECT_EQ(R"({b.b2: {"b3": "abc", "bc": 1, "c1": {"c2": "a", "ce": 1}}})", read_col->debug_item(0));
    EXPECT_EQ(R"({b.b2: {"b3": "efg", "bd": 2, "c1": {"c2": "b", "cd": 2}}})", read_col->debug_item(1));
    EXPECT_EQ(R"({b.b2: {"b3": "xyz", "be": 3, "c1": {"c2": "c", "cf": 3}}})", read_col->debug_item(2));
    EXPECT_EQ(R"({b.b2: {"b3": "qwe", "bf": 4, "c1": {"c2": "d", "cg": 4}}})", read_col->debug_item(3));
    EXPECT_EQ(R"({b.b2: {"b3": "sdf", "bg": 5, "c1": {"c2": "e", "ch": 5}}})", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnRWTest, testDeepNullFlatJson) {
    config::json_flat_null_factor = 1;
    config::json_flat_sparsity_factor = 0.4;

    std::vector<std::string> json = {R"({"a": 1, "b": {"b1": 22, "b2": {"b3": "abc"}, "b4": 1}, "c": 31})",
                                     R"({"a": 2, "b": {"b1": 23, "b2": {"b3": "efg"}, "b4": [1,2,3]}, "d": 32})",
                                     R"({"a": 3, "b": {"b1": 24, "b2": {"b3": "xyz"}, "b4": {"b5": 1}}, "e": [1,2,3]})",
                                     R"(NULL)", R"({"a": 5, "b": {"b1": 26, "b2": {}, "b4": 23}})"};

    ColumnPtr write_col = create_json(json, true);
    ASSIGN_OR_ABORT(auto root, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "b.b4.b5");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "b.b2.b3");

    ColumnPtr read_col = write_col->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, root.get());

    EXPECT_EQ(6, writer_opts.meta->children_columns_size());
    EXPECT_TRUE(writer_opts.meta->json_meta().is_flat());
    EXPECT_TRUE(writer_opts.meta->json_meta().has_remain());
    EXPECT_EQ("nulls", writer_opts.meta->children_columns(0).name());
    EXPECT_EQ("a", writer_opts.meta->children_columns(1).name());
    EXPECT_EQ("b.b1", writer_opts.meta->children_columns(2).name());
    EXPECT_EQ("b.b2.b3", writer_opts.meta->children_columns(3).name());
    EXPECT_EQ("b.b4", writer_opts.meta->children_columns(4).name());
    EXPECT_EQ("remain", writer_opts.meta->children_columns(5).name());

    auto* read_json = down_cast<JsonColumn*>(down_cast<NullableColumn*>(read_col.get())->data_column().get());
    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: "abc"})", read_col->debug_item(0));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: "efg"})", read_col->debug_item(1));
    EXPECT_EQ(R"({b.b4.b5: 1, b.b2.b3: "xyz"})", read_col->debug_item(2));
    EXPECT_EQ(R"(NULL)", read_col->debug_item(3));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: NULL})", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnRWTest, testHyperNullFlatJson) {
    config::json_flat_null_factor = 1;
    config::json_flat_sparsity_factor = 0.4;

    std::vector<std::string> json = {
            R"({"a": 1, "gg": "te1", "ff": {"f1": "985"}, "b": {"b1": 22, "b2": {"b3": "abc", "c1": {"c2": "a", "ce": 1},"bc": 1}, "b4": 1}})",
            R"({"a": 2, "gg": "te2", "ff": {"f1": "984"}, "b": {"b1": 23, "b2": {"b3": "efg", "c1": {"c2": "b", "cd": 2},"bd": 2}, "b4": [1, 2, 3]}})",
            R"({"a": 3, "gg": "te3", "ff": {"f1": "983"}, "b": {"b1": 24, "b2": {"b3": "xyz", "c1": {"c2": "c", "cf": 3},"be": 3}, "b4": {"b5": 1}}})",
            R"({"a": 4, "gg": "te4", "ff": 781, "b": {"b1": 25, "b2": {"b3": "qwe", "c1": {"c2": "d", "cg": 4},"bf": 4}, "b4": {"b7": 2}}})",
            R"(NULL)"};
    ColumnPtr write_col = create_json(json, true);

    ASSIGN_OR_ABORT(auto root, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_DOUBLE, "b.b4.b5");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_VARCHAR, "b.b2");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_VARCHAR, "a");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_BIGINT, "ff.f1");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "gg.g1");

    ColumnPtr read_col = write_col->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, root.get());

    EXPECT_EQ(9, writer_opts.meta->children_columns_size());
    EXPECT_TRUE(writer_opts.meta->json_meta().is_flat());
    EXPECT_TRUE(writer_opts.meta->json_meta().has_remain());
    int index = 0;
    EXPECT_EQ("nulls", writer_opts.meta->children_columns(index++).name());
    EXPECT_EQ("a", writer_opts.meta->children_columns(index++).name());
    EXPECT_EQ("b.b1", writer_opts.meta->children_columns(index++).name());
    EXPECT_EQ("b.b2.b3", writer_opts.meta->children_columns(index++).name());
    EXPECT_EQ("b.b2.c1.c2", writer_opts.meta->children_columns(index++).name());
    EXPECT_EQ("b.b4", writer_opts.meta->children_columns(index++).name());
    EXPECT_EQ("ff.f1", writer_opts.meta->children_columns(index++).name());
    EXPECT_EQ("gg", writer_opts.meta->children_columns(index++).name());
    EXPECT_EQ("remain", writer_opts.meta->children_columns(index++).name());

    auto* read_json = down_cast<JsonColumn*>(down_cast<NullableColumn*>(read_col.get())->data_column().get());
    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    EXPECT_EQ(
            R"({b.b4.b5: NULL, b.b2: '{"b3": "abc", "bc": 1, "c1": {"c2": "a", "ce": 1}}', a: '1', ff.f1: 985, gg.g1: NULL})",
            read_col->debug_item(0));
    EXPECT_EQ(
            R"({b.b4.b5: NULL, b.b2: '{"b3": "efg", "bd": 2, "c1": {"c2": "b", "cd": 2}}', a: '2', ff.f1: 984, gg.g1: NULL})",
            read_col->debug_item(1));
    EXPECT_EQ(
            R"({b.b4.b5: 1, b.b2: '{"b3": "xyz", "be": 3, "c1": {"c2": "c", "cf": 3}}', a: '3', ff.f1: 983, gg.g1: NULL})",
            read_col->debug_item(2));
    EXPECT_EQ(
            R"({b.b4.b5: NULL, b.b2: '{"b3": "qwe", "bf": 4, "c1": {"c2": "d", "cg": 4}}', a: '4', ff.f1: NULL, gg.g1: NULL})",
            read_col->debug_item(3));
    EXPECT_EQ(R"(NULL)", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnRWTest, testMergeRemainNullJson) {
    config::json_flat_null_factor = 1;
    config::json_flat_sparsity_factor = 0.4;

    // clang-format off
    std::vector<std::string> jsons = {
            R"({"a": 1, "b": 21, "c": 31})",
            R"({"a": 2, "b": 22, "d": 32})",
            R"({"a": 3, "b": 23, "e": [1,2,3]})",
            R"({})",
            R"(NULL)"
    };
    // clang-format on
    ColumnPtr write_col = create_json(jsons, true);

    ColumnPtr read_col = write_col->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, nullptr);

    EXPECT_EQ(0, writer_opts.meta->children_columns_size());
    EXPECT_FALSE(writer_opts.meta->json_meta().is_flat());
    EXPECT_FALSE(writer_opts.meta->json_meta().has_remain());

    auto* read_json = down_cast<JsonColumn*>(down_cast<NullableColumn*>(read_col.get())->data_column().get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    EXPECT_EQ(R"({"a": 1, "b": 21, "c": 31})", read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 2, "b": 22, "d": 32})", read_col->debug_item(1));
    EXPECT_EQ(R"({"a": 3, "b": 23, "e": [1, 2, 3]})", read_col->debug_item(2));
    EXPECT_EQ(R"({})", read_col->debug_item(3));
    EXPECT_EQ(R"(NULL)", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnRWTest, testMergeMiddleRemainNullJson) {
    config::json_flat_null_factor = 1;
    config::json_flat_sparsity_factor = 0.4;

    // clang-format off
    std::vector<std::string> jsons = {
            R"({"a": 1, "b": 21, "c": 31})",
            R"({"a": 2, "b": 22, "d": 32})",
            R"({"a": 3, "b": 23, "e": [1,2,3]})",
            R"({})",
            R"(NULL)"
    };
    // clang-format on
    ColumnPtr write_col = create_json(jsons, true);

    ASSIGN_OR_ABORT(auto root_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ASSIGN_OR_ABORT(auto f1_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root.a", 0));
    ASSIGN_OR_ABORT(auto f2_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root.c", 0));
    root_path->children().emplace_back(std::move(f1_path));
    root_path->children().emplace_back(std::move(f2_path));

    ColumnPtr read_col = write_col->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, root_path.get());

    EXPECT_EQ(0, writer_opts.meta->children_columns_size());
    EXPECT_FALSE(writer_opts.meta->json_meta().is_flat());
    EXPECT_FALSE(writer_opts.meta->json_meta().has_remain());

    auto* read_json = down_cast<JsonColumn*>(down_cast<NullableColumn*>(read_col.get())->data_column().get());

    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    EXPECT_EQ("{a: 1, c: 31}", read_col->debug_item(0));
    EXPECT_EQ("{a: 2, c: NULL}", read_col->debug_item(1));
    EXPECT_EQ("{a: 3, c: NULL}", read_col->debug_item(2));
    EXPECT_EQ("{a: NULL, c: NULL}", read_col->debug_item(3));
    EXPECT_EQ("NULL", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnRWTest, testMergeMiddleRemainNullJson2) {
    config::json_flat_null_factor = 1;
    config::json_flat_sparsity_factor = 0.4;

    std::vector<std::string> json = {
            R"({"a": 1, "b": {"b1": 22, "b2": {"b3": "abc", "c1": {"c2": "a", "ce": 1},"bc": 1}, "b4": 1}})",
            R"({"a": 2, "b": {"b1": 23, "b2": {"b3": "efg", "c1": {"c2": "b", "cd": 2},"bd": 2}, "b4": [1, 2, 3]}})",
            R"({"a": 3, "b": {"b1": 24, "b2": {"b3": "xyz", "c1": {"c2": "c", "cf": 3},"be": 3}, "b4": {"b5": 1}}})",
            R"({})", R"(NULL)"};

    ColumnPtr write_col = create_json(json, true);

    ASSIGN_OR_ABORT(auto root_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ASSIGN_OR_ABORT(auto b_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root.b", 0));
    ASSIGN_OR_ABORT(auto b2_path, ColumnAccessPath::create(TAccessPathType::FIELD, "root.b.b2", 0));

    b_path->children().emplace_back(std::move(b2_path));
    root_path->children().emplace_back(std::move(b_path));

    ColumnPtr read_col = write_col->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, root_path.get());

    EXPECT_EQ(0, writer_opts.meta->children_columns_size());
    EXPECT_FALSE(writer_opts.meta->json_meta().is_flat());
    EXPECT_FALSE(writer_opts.meta->json_meta().has_remain());

    auto* read_json = down_cast<JsonColumn*>(down_cast<NullableColumn*>(read_col.get())->data_column().get());
    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    EXPECT_EQ(R"({b.b2: {"b3": "abc", "bc": 1, "c1": {"c2": "a", "ce": 1}}})", read_col->debug_item(0));
    EXPECT_EQ(R"({b.b2: {"b3": "efg", "bd": 2, "c1": {"c2": "b", "cd": 2}}})", read_col->debug_item(1));
    EXPECT_EQ(R"({b.b2: {"b3": "xyz", "be": 3, "c1": {"c2": "c", "cf": 3}}})", read_col->debug_item(2));
    EXPECT_EQ(R"({b.b2: NULL})", read_col->debug_item(3));
    EXPECT_EQ(R"(NULL)", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnRWTest, testDeepNullJson) {
    config::json_flat_null_factor = 1;
    config::json_flat_sparsity_factor = 0.4;

    std::vector<std::string> json = {R"({"a": 1, "b": {"b1": 22, "b2": {"b3": "abc"}, "b4": 1}, "c": 31})",
                                     R"({"a": 2, "b": {"b1": 23, "b2": {"b3": "efg"}, "b4": [1,2,3]}, "d": 32})",
                                     R"({"a": 3, "b": {"b1": 24, "b2": {"b3": "xyz"}, "b4": {"b5": 1}}, "e": [1,2,3]})",
                                     R"({"a": 4, "b": {}, "g": {"x": 1}})", R"(NULL)"};

    ColumnPtr write_col = create_json(json, true);

    ASSIGN_OR_ABORT(auto root, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "b.b4.b5");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "b.b2.b3");

    ColumnPtr read_col = write_col->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, root.get());

    EXPECT_EQ(0, writer_opts.meta->children_columns_size());
    EXPECT_FALSE(writer_opts.meta->json_meta().is_flat());
    EXPECT_FALSE(writer_opts.meta->json_meta().has_remain());

    auto* read_json = down_cast<JsonColumn*>(down_cast<NullableColumn*>(read_col.get())->data_column().get());
    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: "abc"})", read_col->debug_item(0));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: "efg"})", read_col->debug_item(1));
    EXPECT_EQ(R"({b.b4.b5: 1, b.b2.b3: "xyz"})", read_col->debug_item(2));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: NULL})", read_col->debug_item(3));
    EXPECT_EQ(R"(NULL)", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnRWTest, testHyperNullJson) {
    config::json_flat_null_factor = 1;
    config::json_flat_sparsity_factor = 0.4;

    std::vector<std::string> json = {
            R"({"a": 1, "gg": "te1", "ff": {"f1": "985"}, "b": {"b1": 22, "b2": {"b3": "abc", "c1": {"c2": "a", "ce": 1},"bc": 1}, "b4": 1}})",
            R"({"a": 2, "gg": "te2", "ff": {"f1": "984"}, "b": {"b1": 23, "b2": {"b3": "efg", "c1": {"c2": "b", "cd": 2},"bd": 2}, "b4": [1, 2, 3]}})",
            R"({"a": 3, "gg": "te3", "ff": {"f1": "983"}, "b": {"b1": 24, "b2": {"b3": "xyz", "c1": {"c2": "c", "cf": 3},"be": 3}, "b4": {"b5": 1}}})",
            R"({"a": 4, "gg": "te4", "ff": 781, "b": {"b1": 25, "b2": {"b3": "qwe", "c1": {"c2": "d", "cg": 4},"bf": 4}, "b4": {"b7": 2}}})",
            R"(NULL)"};

    auto write_col = create_json(json, true);

    ASSIGN_OR_ABORT(auto root, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "b.b4.b5");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "b.b2.b3");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "a");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "ff.f1");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "gg.g1");

    ColumnPtr read_col = write_col->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, root.get());

    EXPECT_EQ(0, writer_opts.meta->children_columns_size());
    EXPECT_FALSE(writer_opts.meta->json_meta().is_flat());
    EXPECT_FALSE(writer_opts.meta->json_meta().has_remain());

    auto* read_json = down_cast<JsonColumn*>(down_cast<NullableColumn*>(read_col.get())->data_column().get());
    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: "abc", a: 1, ff.f1: "985", gg.g1: NULL})", read_col->debug_item(0));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: "efg", a: 2, ff.f1: "984", gg.g1: NULL})", read_col->debug_item(1));
    EXPECT_EQ(R"({b.b4.b5: 1, b.b2.b3: "xyz", a: 3, ff.f1: "983", gg.g1: NULL})", read_col->debug_item(2));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: "qwe", a: 4, ff.f1: NULL, gg.g1: NULL})", read_col->debug_item(3));
    EXPECT_EQ(R"(NULL)", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnRWTest, testHyperNullJson2) {
    config::json_flat_null_factor = 0;
    config::json_flat_sparsity_factor = 0.4;

    std::vector<std::string> json = {
            R"({"a": 1, "gg": "te1", "ff": {"f1": "985"}, "b": {"b1": 22, "b2": {"b3": "abc", "c1": {"c2": "a", "ce": 1},"bc": 1}, "b4": 1}})",
            R"({"a": 2, "gg": "te2", "ff": {"f1": "984"}, "b": {"b1": 23, "b2": {"b3": "efg", "c1": {"c2": "b", "cd": 2},"bd": 2}, "b4": [1, 2, 3]}})",
            R"({"a": 3, "gg": "te3", "ff": {"f1": "983"}, "b": {"b1": 24, "b2": {"b3": "xyz", "c1": {"c2": "c", "cf": 3},"be": 3}, "b4": {"b5": 1}}})",
            R"({"a": 4, "gg": "te4", "ff": 781, "b": {"b1": 25, "b2": {"b3": "qwe", "c1": {"c2": "d", "cg": 4},"bf": 4}, "b4": {"b7": 2}}})",
            R"(NULL)"};

    auto write_col = create_json(json, true);

    ASSIGN_OR_ABORT(auto root, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "b.b4.b5");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "b.b2.b3");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "a");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "ff.f1");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "gg.g1");

    ColumnPtr read_col = write_col->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, root.get());

    EXPECT_EQ(0, writer_opts.meta->children_columns_size());
    EXPECT_FALSE(writer_opts.meta->json_meta().is_flat());
    EXPECT_FALSE(writer_opts.meta->json_meta().has_remain());

    auto* read_json = down_cast<JsonColumn*>(down_cast<NullableColumn*>(read_col.get())->data_column().get());
    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: "abc", a: 1, ff.f1: "985", gg.g1: NULL})", read_col->debug_item(0));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: "efg", a: 2, ff.f1: "984", gg.g1: NULL})", read_col->debug_item(1));
    EXPECT_EQ(R"({b.b4.b5: 1, b.b2.b3: "xyz", a: 3, ff.f1: "983", gg.g1: NULL})", read_col->debug_item(2));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: "qwe", a: 4, ff.f1: NULL, gg.g1: NULL})", read_col->debug_item(3));
    EXPECT_EQ(R"(NULL)", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnRWTest, testHyperNoCastTypeNullJson) {
    config::json_flat_null_factor = 1;
    config::json_flat_sparsity_factor = 0.4;

    std::vector<std::string> json = {
            R"({"a": 1, "gg": "te1", "ff": {"f1": "985"}, "b": {"b1": 22, "b2": {"b3": "abc", "c1": {"c2": "a", "ce": 1},"bc": 1}, "b4": 1}})",
            R"({"a": 2, "gg": "te2", "ff": {"f1": "984"}, "b": {"b1": 23, "b2": {"b3": "efg", "c1": {"c2": "b", "cd": 2},"bd": 2}, "b4": [1, 2, 3]}})",
            R"({"a": 3, "gg": "te3", "ff": {"f1": "983"}, "b": {"b1": 24, "b2": {"b3": "xyz", "c1": {"c2": "c", "cf": 3},"be": 3}, "b4": {"b5": 1}}})",
            R"({"a": 4, "gg": "te4", "ff": 781, "b": {"b1": 25, "b2": {}, "b4": {"b7": 2}}})", R"(NULL)"};

    ColumnPtr write_col = create_json(json, true);

    ASSIGN_OR_ABORT(auto root, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_BIGINT, "b.b4.b5");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_VARCHAR, "b.b2.b3");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_BIGINT, "a");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "ff.f1");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_VARCHAR, "gg.g1");

    ColumnPtr read_col = write_col->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, root.get());

    auto* read_json = down_cast<JsonColumn*>(down_cast<NullableColumn*>(read_col.get())->data_column().get());
    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: 'abc', a: 1, ff.f1: "985", gg.g1: NULL})", read_col->debug_item(0));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: 'efg', a: 2, ff.f1: "984", gg.g1: NULL})", read_col->debug_item(1));
    EXPECT_EQ(R"({b.b4.b5: 1, b.b2.b3: 'xyz', a: 3, ff.f1: "983", gg.g1: NULL})", read_col->debug_item(2));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2.b3: NULL, a: 4, ff.f1: NULL, gg.g1: NULL})", read_col->debug_item(3));
    EXPECT_EQ(R"(NULL)", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnRWTest, testHyperCastTypeNullJson) {
    config::json_flat_null_factor = 1;
    config::json_flat_sparsity_factor = 0.4;

    std::vector<std::string> json = {
            R"({"a": 1, "gg": "te1", "ff": {"f1": "985"}, "b": {"b1": 22, "b2": {"b3": "abc", "c1": {"c2": "a", "ce": 1},"bc": 1}, "b4": 1}})",
            R"({"a": 2, "gg": "te2", "ff": {"f1": "984"}, "b": {"b1": 23, "b2": {"b3": "efg", "c1": {"c2": "b", "cd": 2},"bd": 2}, "b4": [1, 2, 3]}})",
            R"({"a": 3, "gg": "te3", "ff": {"f1": "983"}, "b": {"b1": 24, "b2": {"b3": "xyz", "c1": {"c2": "c", "cf": 3},"be": 3}, "b4": {"b5": 1}}})",
            R"({"a": 4, "gg": "te4", "ff": 781, "b": {}})", R"(NULL)"};
    ColumnPtr write_col = create_json(json, true);

    ASSIGN_OR_ABORT(auto root, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_DOUBLE, "b.b4.b5");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_BIGINT, "b.b2");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_VARCHAR, "a");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_BIGINT, "ff.f1");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "gg.g1");

    ColumnPtr read_col = write_col->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, root.get());

    auto* read_json = down_cast<JsonColumn*>(down_cast<NullableColumn*>(read_col.get())->data_column().get());
    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2: NULL, a: '1', ff.f1: 985, gg.g1: NULL})", read_col->debug_item(0));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2: NULL, a: '2', ff.f1: 984, gg.g1: NULL})", read_col->debug_item(1));
    EXPECT_EQ(R"({b.b4.b5: 1, b.b2: NULL, a: '3', ff.f1: 983, gg.g1: NULL})", read_col->debug_item(2));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2: NULL, a: '4', ff.f1: NULL, gg.g1: NULL})", read_col->debug_item(3));
    EXPECT_EQ(R"(NULL)", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnRWTest, testHyperCastTypeNullJson2) {
    config::json_flat_null_factor = 1;
    config::json_flat_sparsity_factor = 0.4;

    std::vector<std::string> json = {
            R"({"a": 1, "gg": "te1", "ff": {"f1": "985"}, "b": {"b1": 22, "b2": {"b3": "abc", "c1": {"c2": "a", "ce": 1},"bc": 1}, "b4": 1}})",
            R"({"a": 2, "gg": "te2", "ff": {"f1": "984"}, "b": {"b1": 23, "b2": {"b3": "efg", "c1": {"c2": "b", "cd": 2},"bd": 2}, "b4": [1, 2, 3]}})",
            R"({"a": 3, "gg": "te3", "ff": {"f1": "983"}, "b": {"b1": 24, "b2": {"b3": "xyz", "c1": {"c2": "c", "cf": 3},"be": 3}, "b4": {"b5": 1}}})",
            R"({"a": 4, "gg": "te4", "ff": 781, "b": {}})", R"(NULL)"};
    ColumnPtr write_col = create_json(json, true);

    ASSIGN_OR_ABORT(auto root, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_DOUBLE, "b.b4.b5");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_VARCHAR, "b.b2");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_VARCHAR, "a");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_BIGINT, "ff.f1");
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "gg.g1");

    ColumnPtr read_col = write_col->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, root.get());

    auto* read_json = down_cast<JsonColumn*>(down_cast<NullableColumn*>(read_col.get())->data_column().get());
    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(5, read_col->size());
    EXPECT_EQ(
            R"({b.b4.b5: NULL, b.b2: '{"b3": "abc", "bc": 1, "c1": {"c2": "a", "ce": 1}}', a: '1', ff.f1: 985, gg.g1: NULL})",
            read_col->debug_item(0));
    EXPECT_EQ(
            R"({b.b4.b5: NULL, b.b2: '{"b3": "efg", "bd": 2, "c1": {"c2": "b", "cd": 2}}', a: '2', ff.f1: 984, gg.g1: NULL})",
            read_col->debug_item(1));
    EXPECT_EQ(
            R"({b.b4.b5: 1, b.b2: '{"b3": "xyz", "be": 3, "c1": {"c2": "c", "cf": 3}}', a: '3', ff.f1: 983, gg.g1: NULL})",
            read_col->debug_item(2));
    EXPECT_EQ(R"({b.b4.b5: NULL, b.b2: NULL, a: '4', ff.f1: NULL, gg.g1: NULL})", read_col->debug_item(3));
    EXPECT_EQ(R"(NULL)", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnRWTest, testHyperDeepFlatternJson) {
    config::json_flat_null_factor = 0.4;
    config::json_flat_sparsity_factor = 0.5;

    ColumnPtr write_col = JsonColumn::create();
    auto* json_col = down_cast<JsonColumn*>(write_col.get());
    std::vector<std::string> json = {R"({"a": 1, "gg": "te1", "ff": {"f1": [{"e2": 1, "e3": 2}, 2, 3]}})",
                                     R"({"a": 2, "gg": "te2", "ff": 780})", R"({"a": 3, "gg": "te3", "ff": 781})",
                                     R"({"a": 5, "gg": "te5", "ff": 782})"};

    for (auto& x : json) {
        ASSIGN_OR_ABORT(auto jv, JsonValue::parse(x));
        json_col->append(jv);
    }

    ASSIGN_OR_ABORT(auto root, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_JSON, "ff.f1");

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, "/test_flat_json_rw2.data", write_col, read_col, root.get());

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_TRUE(read_json->is_flat_json());
    EXPECT_EQ(4, read_col->size());
    EXPECT_EQ(R"({ff.f1: [{"e2": 1, "e3": 2}, 2, 3]})", read_col->debug_item(0));
    EXPECT_EQ(R"({ff.f1: NULL})", read_col->debug_item(3));
}

} // namespace starrocks
