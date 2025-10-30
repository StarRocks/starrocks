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
#include <lz4/lz4.h>

#include <iostream>
#include <memory>
#include <vector>

#include "column/column_access_path.h"
#include "column/column_helper.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "common/statusor.h"
#include "fs/fs.h"
#include "fs/fs_memory.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gutil/casts.h"
#include "storage/aggregate_type.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/flat_json_config.h"
#include "storage/olap_common.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/column_writer.h"
#include "storage/rowset/json_column_iterator.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_writer.h"
#include "storage/tablet_schema_helper.h"
#include "storage/types.h"
#include "testutil/assert.h"
#include "testutil/parallel_test.h"
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
    void SetUp() override {
        config::enable_json_flat_complex_type = true;
        _meta = std::make_shared<ColumnMetaPB>();
        config::json_flat_sparsity_factor = 0.9;
    }

    void TearDown() override {
        config::enable_json_flat_complex_type = false;
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

protected:
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

TEST_F(FlatJsonColumnRWTest, testMergeRemainFlatJsonWithConfig) {
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
    FlatJsonConfig config;
    writer_opts.need_flat = true;
    writer_opts.flat_json_config = &config;
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

TEST_F(FlatJsonColumnRWTest, testMergeMiddleRemainFlatJson2WithConfig) {
    FlatJsonConfig config;
    config.set_flat_json_null_factor(0.4);
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
    writer_opts.flat_json_config = &config;
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

TEST_F(FlatJsonColumnRWTest, testMergeMiddleRemainFlatJson3WithConfig) {
    FlatJsonConfig config;
    config.set_flat_json_null_factor(0.4);
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
    writer_opts.flat_json_config = &config;
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

TEST_F(FlatJsonColumnRWTest, testHyperFlatJsonWithConfig) {
    FlatJsonConfig config;
    config.set_flat_json_null_factor(0.4);
    config.set_flat_json_sparsity_factor(0.5);
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
    writer_opts.flat_json_config = &config;
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

    index = 0;
    EXPECT_EQ(EncodingTypePB::PLAIN_ENCODING, writer_opts.meta->encoding());
    EXPECT_EQ(EncodingTypePB::BIT_SHUFFLE, writer_opts.meta->children_columns(index++).encoding());
    EXPECT_EQ(EncodingTypePB::BIT_SHUFFLE, writer_opts.meta->children_columns(index++).encoding());
    EXPECT_EQ(EncodingTypePB::DICT_ENCODING, writer_opts.meta->children_columns(index++).encoding());
    EXPECT_EQ(EncodingTypePB::DICT_ENCODING, writer_opts.meta->children_columns(index++).encoding());
    EXPECT_EQ(EncodingTypePB::DICT_ENCODING, writer_opts.meta->children_columns(index++).encoding());
    EXPECT_EQ(EncodingTypePB::DICT_ENCODING, writer_opts.meta->children_columns(index++).encoding());
    EXPECT_EQ(EncodingTypePB::DICT_ENCODING, writer_opts.meta->children_columns(index++).encoding());
    EXPECT_EQ(EncodingTypePB::PLAIN_ENCODING, writer_opts.meta->children_columns(index++).encoding());

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

TEST_F(FlatJsonColumnRWTest, testMergeMiddleRemainJson2WithConfig) {
    FlatJsonConfig config;
    config.set_flat_json_null_factor(0.4);
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

TEST_F(FlatJsonColumnRWTest, testHyperJsonWithConfig) {
    FlatJsonConfig config;
    config.set_flat_json_null_factor(0.4);
    config.set_flat_json_sparsity_factor(0.5);

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
    writer_opts.flat_json_config = &config;
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

TEST_F(FlatJsonColumnRWTest, testHyperNoCastTypeJsonWithConfig) {
    FlatJsonConfig config;
    config.set_flat_json_null_factor(0.4);
    config.set_flat_json_sparsity_factor(0.5);

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
    writer_opts.flat_json_config = &config;
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

TEST_F(FlatJsonColumnRWTest, testHyperCastTypeJsonWithConfig) {
    FlatJsonConfig config;
    config.set_flat_json_null_factor(0.4);
    config.set_flat_json_sparsity_factor(0.5);

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
    writer_opts.flat_json_config = &config;
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

TEST_F(FlatJsonColumnRWTest, testGetIORangeVec) {
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

    auto fs = std::make_shared<MemoryFileSystem>();
    ASSERT_TRUE(fs->create_dir(TEST_DIR).ok());

    TabletColumn json_tablet_column = create_with_default_value<TYPE_JSON>("");
    TypeInfoPtr type_info = get_type_info(json_tablet_column);

    const std::string fname = TEST_DIR + "/test_flat_json_rw1.data";
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

    ASSIGN_OR_ABORT(auto iter, reader->new_iterator(nullptr));
    ASSIGN_OR_ABORT(auto read_file, fs->new_random_access_file(fname));

    ColumnIteratorOptions iter_opts;
    OlapReaderStatistics stats;
    iter_opts.stats = &stats;
    iter_opts.read_file = read_file.get();
    ASSERT_TRUE(iter->init(iter_opts).ok());

    SparseRange<> range;
    range.add(Range<>(0, write_col->size()));
    auto status_or = iter->get_io_range_vec(range, read_col.get());
    ASSERT_TRUE(status_or.ok());
    ASSERT_EQ((*status_or).size(), 1);
}

GROUP_SLOW_TEST_F(FlatJsonColumnRWTest, testJsonColumnCompression) {
    constexpr size_t num_rows = 16 * 4096; // Generate several MBs of data
    // Construct JSON objects with the same schema
    auto col = ChunkHelper::column_from_field_type(TYPE_JSON, true);
    col->reserve(num_rows);
    std::string json_strings;
    std::vector<std::string> kind_dict = {"commit", "rebase", "merge"};
    std::vector<std::string> op_dict = {"create", "update", "delete"};
    std::vector<std::string> coll_dict = {"app.bsky.graph.follow", "app.bsky.feed.post", "app.bsky.actor.profile"};
    std::vector<std::string> type_dict = {"app.bsky.graph.follow", "app.bsky.feed.post", "app.bsky.actor.profile"};
    for (size_t i = 0; i < num_rows; ++i) {
        // Generate random strings
        auto rand_str = [](size_t len) {
            static const char charset[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
            std::string s;
            s.reserve(len);
            for (size_t j = 0; j < len; ++j) s += charset[rand() % (sizeof(charset) - 1)];
            return s;
        };
        std::string cid = rand_str(24);
        std::string rev = rand_str(12);
        std::string rkey = rand_str(12);
        std::string did = "did:plc:" + rand_str(20);
        std::string subject = "did:plc:" + rand_str(20);
        int64_t time_us = 1700000000000000LL + rand() % 10000000000000LL;
        std::string create_at;
        {
            starrocks::DateTimeValue dtv;
            ASSERT_TRUE(dtv.from_unixtime(time_us / 1000000, cctz::utc_time_zone()));
            create_at.resize(64);
            char* end = dtv.to_string(create_at.data());
            create_at.resize(end - create_at.data() - 1); // it's c-style string with ending \0
        }

        const std::string& op = op_dict[rand() % op_dict.size()];
        const std::string& coll = coll_dict[rand() % coll_dict.size()];
        const std::string& type = type_dict[rand() % type_dict.size()];
        std::string s = strings::Substitute(
                R"( {"commit":{"cid":"$0","collection":"$1","operation":"$2","record":{"type":"$3","createdAt":"$4","subject":"$5"},"rev":"$6","rkey":"$7"},"did":"$8","time_us":$9 } )",
                cid, coll, op, type, create_at, subject, rev, rkey, did, time_us);

        auto stv = JsonValue::parse(Slice(s));
        ASSERT_TRUE(stv.ok());

        // concat to the buffer
        json_strings += s;
        // append to the column
        col->append_datum(Datum(&stv.value()));
    }
    std::cout << "[JSON] string size: " << json_strings.size() << " bytes\n";
    std::cout << "[JSON] in-memory size: " << col->byte_size() << " bytes\n";

    // Compress the JSON string
    {
        size_t max_dst_size = LZ4_compressBound(json_strings.size());
        std::vector<char> compressed(max_dst_size);
        int compressed_size = LZ4_compress_default(reinterpret_cast<const char*>(json_strings.data()),
                                                   compressed.data(), json_strings.size(), max_dst_size);
        ASSERT_GT(compressed_size, 0);
        std::cout << "[JSON] json_string compressed size: " << compressed_size << " bytes\n";
    }

    // Directly compress the in-memory JSON data using lz4
    {
        // Get the raw data pointer and length
        size_t raw_size = 0;
        std::string serialize_buffer;
        for (int i = 0; i < col->size(); i++) {
            size_t ser_size = col->serialize_size(i);
            size_t end = serialize_buffer.size();
            serialize_buffer.resize(serialize_buffer.size() + ser_size);
            raw_size += col->serialize(i, (uint8_t*)serialize_buffer.data() + end);
        }
        std::cout << "[JSON] serialized size " << raw_size << " bytes\n";

        if (raw_size > 0) {
            // Allocate a sufficiently large buffer
            size_t max_dst_size = LZ4_compressBound(raw_size);
            ASSERT_GT(max_dst_size, 0);
            std::vector<char> compressed(max_dst_size);
            int compressed_size = LZ4_compress_default(reinterpret_cast<const char*>(serialize_buffer.data()),
                                                       compressed.data(), serialize_buffer.size(), max_dst_size);
            ASSERT_GT(compressed_size, 0);
            std::cout << "[JSON] serialized compressed size: " << compressed_size << " bytes\n";
        }
    }

    // Write two copies: uncompressed and LZ4 compressed
    auto fs = std::make_shared<MemoryFileSystem>();
    ASSERT_TRUE(fs->create_dir(TEST_DIR).ok());
    std::string fname_nocomp = TEST_DIR + "/test_json_nocomp.data";
    std::string fname_lz4 = TEST_DIR + "/test_json_lz4.data";
    TabletColumn column(STORAGE_AGGREGATE_NONE, TYPE_JSON);
    struct Params {
        CompressionTypePB compression;
        std::string file_name;
        bool need_flat;
    };

    std::vector<Params> params = {
            {NO_COMPRESSION, fname_lz4, false},   {LZ4_FRAME, fname_lz4, false}, {ZSTD, fname_lz4, false},
            {NO_COMPRESSION, fname_nocomp, true}, {LZ4_FRAME, fname_lz4, true},  {ZSTD, fname_lz4, true},
    };
    for (auto& param : params) {
        fs->delete_file(param.file_name).ok();
        ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(param.file_name));
        ColumnWriterOptions writer_opts;
        ColumnMetaPB meta;
        writer_opts.page_format = 2;
        writer_opts.meta = &meta;
        writer_opts.meta->set_column_id(0);
        writer_opts.meta->set_unique_id(0);
        writer_opts.meta->set_type(TYPE_JSON);
        writer_opts.meta->set_length(0);
        writer_opts.meta->set_encoding(DEFAULT_ENCODING);
        writer_opts.meta->set_compression(param.compression);
        writer_opts.meta->set_is_nullable(true);
        writer_opts.meta->set_compression_level(3);
        writer_opts.need_flat = param.need_flat;
        writer_opts.need_zone_map = false;
        writer_opts.need_speculate_encoding = false;
        ASSIGN_OR_ABORT(auto writer, ColumnWriter::create(writer_opts, &column, wfile.get()));
        ASSERT_OK(writer->init());
        ASSERT_TRUE(writer->append(*col).ok());
        ASSERT_TRUE(writer->finish().ok());
        ASSERT_TRUE(writer->write_data().ok());
        ASSERT_TRUE(writer->write_ordinal_index().ok());
        ASSERT_TRUE(wfile->close().ok());
        std::cout << "[JSON] "
                  << "compression=" << CompressionTypePB_Name(param.compression) << " need_flat=" << param.need_flat
                  << " file size: " << wfile->size() << " bytes\n";
    }
}

TEST_F(FlatJsonColumnRWTest, testSegmentWriterIteratorWithMixedDataTypes) {
    const std::string kJsonColumnName = "json_col";
    TabletSchemaPB schema_pb;
    {
        ColumnPB* json_column = schema_pb.add_column();
        json_column->set_unique_id(0);
        json_column->set_name(kJsonColumnName);
        json_column->set_type("json");
        json_column->set_is_nullable(true);
    }
    auto tablet_schema = TabletSchema::create(schema_pb);

    std::vector<std::string> json_strings = {
            R"({
            "id": 1001,
            "name": "Alice",
            "age": 25,
            "salary": 50000.50,
            "is_active": true,
            "tags": ["engineer", "python", "data"],
            "address": {
                "city": "Beijing",
                "country": "China",
                "postal_code": "100000"
            },
            "skills": null,
            "projects": [
                {"name": "Project A", "duration": 6},
                {"name": "Project B", "duration": 12}
            ],
            "unique1": 123
        })",
            R"({
            "id": 1002,
            "name": "Bob",
            "age": 30,
            "salary": 75000.75,
            "is_active": false,
            "tags": ["manager", "leadership"],
            "address": {
                "city": "Shanghai",
                "country": "China",
                "postal_code": "200000"
            },
            "skills": ["Java", "Spring", "MySQL"],
            "projects": [],
            "unique2": "123"
        })",
            R"({
            "id": 1003,
            "name": "Charlie",
            "age": 28,
            "salary": 60000.25,
            "is_active": true,
            "tags": ["designer", "ui", "ux"],
            "address": null,
            "skills": ["Figma", "Sketch", "Photoshop"],
            "projects": [
                {"name": "Website Redesign", "duration": 8, "completed": true}
            ],
            "unique3": 123.0
        })"};

    std::string file_name = TEST_DIR + "/test_json_segment.data";
    auto fs = std::make_shared<MemoryFileSystem>();
    ASSERT_TRUE(fs->create_dir(TEST_DIR).ok());

    {
        // write the segment data
        SegmentWriterOptions opts;
        opts.flat_json_config = std::make_shared<FlatJsonConfig>();
        opts.flat_json_config->set_flat_json_enabled(true);
        ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(file_name));
        auto segment_writer = std::make_unique<SegmentWriter>(std::move(wfile), 0, tablet_schema, opts);
        ASSERT_OK(segment_writer->init());

        auto json_column = JsonColumn::create();

        for (const auto& json_str : json_strings) {
            JsonValue json_value;
            ASSERT_OK(JsonValue::parse(json_str, &json_value));
            json_column->append(&json_value);
        }

        SchemaPtr chunk_schema = std::make_shared<Schema>(tablet_schema->schema());
        auto chunk = std::make_shared<Chunk>(Columns{json_column}, chunk_schema);
        ASSERT_OK(segment_writer->append_chunk(*chunk));
        uint64_t index_size, segment_file_size;
        ASSERT_OK(segment_writer->finalize_columns(&index_size));
        ASSERT_OK(segment_writer->finalize_footer(&segment_file_size));
    }

    {
        // read and validate the segment data
        auto segment = *Segment::open(fs, FileInfo{file_name}, 0, tablet_schema);
        OlapReaderStatistics stats;
        SegmentReadOptions read_opts;
        read_opts.fs = fs;
        read_opts.tablet_schema = tablet_schema;
        read_opts.stats = &stats;
        ASSIGN_OR_ABORT(auto segment_iterator, segment->new_iterator(*tablet_schema->schema(), read_opts));

        auto read_chunk = std::make_shared<Chunk>();
        read_chunk->append_column(ColumnHelper::create_column(TypeDescriptor::create_json_type(), true), 0);

        ASSERT_OK(segment_iterator->get_next(read_chunk.get()));
        ASSERT_EQ(read_chunk->num_rows(), json_strings.size());

        auto read_json_column = read_chunk->get_column_by_index(0);
        for (size_t i = 0; i < json_strings.size(); ++i) {
            auto json_datum = read_json_column->get(i);

            const JsonValue* read_json = json_datum.get_json();
            JsonValue expected_json;
            ASSERT_OK(JsonValue::parse(json_strings[i], &expected_json));
            ASSERT_EQ(read_json->to_string().value(), expected_json.to_string().value());
        }

        read_chunk->reset();
        ASSERT_TRUE(segment_iterator->get_next(read_chunk.get()).is_end_of_file());
    }

    {
        // read specific column
        auto segment = *Segment::open(fs, FileInfo{file_name}, 0, tablet_schema);
        OlapReaderStatistics stats;
        SegmentReadOptions read_opts;
        read_opts.fs = fs;
        read_opts.tablet_schema = tablet_schema;
        read_opts.stats = &stats;

        OlapReaderStatistics reader_stats;
        ColumnIteratorOptions column_opts;
        column_opts.stats = &reader_stats;

        ASSIGN_OR_ABORT(auto read_file, fs->new_random_access_file(file_name));
        column_opts.read_file = read_file.get();

        // clang-format off
        std::vector<std::tuple<std::string, LogicalType, std::string>> fields = {

            // extract from the FlatJSON.remain
            {"unique1", TYPE_BIGINT, "[123, NULL, NULL]"},
            {"unique1", TYPE_DOUBLE, "[123, NULL, NULL]"},
            {"unique1", TYPE_BOOLEAN, "[1, NULL, NULL]"},
            {"unique1", TYPE_VARCHAR, "['123', NULL, NULL]"},
            {"unique2", TYPE_BIGINT, "[NULL, 123, NULL]"},
            {"unique3", TYPE_VARCHAR, "[NULL, NULL, '123']"},
            {"unique3", TYPE_BIGINT, "[NULL, NULL, 123]"},
            {"unique3", TYPE_BOOLEAN, "[NULL, NULL, 1]"},
            // non-existent
            {"unique4", TYPE_BIGINT, "[NULL, NULL, NULL]"},

            // flatten columns
            {"id", TYPE_BIGINT, "[1001, 1002, 1003]"},
            {"id", TYPE_DOUBLE, "[1001, 1002, 1003]"},
            {"id", TYPE_VARCHAR, "['1001', '1002', '1003']"},
            
            {"name", TYPE_VARCHAR, R"(['Alice', 'Bob', 'Charlie'])"},
            {"name", TYPE_BIGINT, R"([NULL, NULL, NULL])"},
            
            {"age", TYPE_BIGINT, "[25, 30, 28]"},

            {"salary", TYPE_DOUBLE, "[50000.5, 75000.8, 60000.2]"},
            {"salary", TYPE_BIGINT, "[50000, 75000, 60000]"},
            {"salary", TYPE_VARCHAR, "['50000.5', '75000.75', '60000.25']"},

            {"is_active", TYPE_BOOLEAN, "[1, 0, 1]"},
            {"is_active", TYPE_BIGINT, "[1, 0, 1]"},
            {"is_active", TYPE_VARCHAR, "['true', 'false', 'true']"},

            {"address.city", TYPE_VARCHAR, "['Beijing', 'Shanghai', NULL]"},
            {"address.country", TYPE_VARCHAR, "['China', 'China', NULL]"},
            {"address.postal_code", TYPE_BIGINT, "[100000, 200000, NULL]"},
        };
        // clang-format on

        int index = 0;
        for (const auto& [field_name, field_type, result] : fields) {
            std::cerr << "running test case " << index++ << std::endl;
            ASSIGN_OR_ABORT(auto path, ColumnAccessPath::create(TAccessPathType::FIELD, "$", 0));
            ColumnAccessPath::insert_json_path(path.get(), field_type, field_name);
            // ASSERT_EQ(kJsonColumnName + "." + field_name, path->full_path());
            ASSERT_EQ("$." + field_name, path->linear_path());

            TabletColumn col(STORAGE_AGGREGATE_NONE, field_type, true);
            col.set_unique_id(123);
            col.set_extended_info(std::make_unique<ExtendedColumnInfo>(path.get(), 0));

            ASSIGN_OR_ABORT(auto column_iter, segment->new_column_iterator_or_default(col, path.get()));
            ASSERT_OK(column_iter->init(column_opts));
            ASSERT_OK(column_iter->seek_to_first());
            size_t count = 3;
            auto column = ColumnHelper::create_column(TypeDescriptor(field_type), true);
            ASSERT_OK(column_iter->next_batch(&count, column.get()));
            ASSERT_EQ(column->size(), json_strings.size());
            ASSERT_EQ(result, column->debug_string()) << fmt::format("field={} type={}", field_name, field_type);
        }
    }
}

TEST_F(FlatJsonColumnRWTest, test_json_global_dict) {
    // Test JSON global dictionary functionality
    auto fs = std::make_shared<MemoryFileSystem>();
    const std::string file_name = "/tmp/test_json_global_dict.dat";
    ASSERT_TRUE(fs->create_dir("/tmp/").ok());

    // Prepare test data with repeated JSON values to trigger dictionary encoding
    std::vector<std::string> json_strings = {
            R"({"name": "Alice", "age": 30, "city": "Beijing"})",
            R"({"name": "Bob", "age": 25, "city": "Shanghai"})",
            R"({"name": "Alice", "age": 35, "city": "Beijing"})", // repeated name and city
            R"({"name": "Charlie", "age": 28, "city": "Guangzhou"})",
            R"({"name": "Bob", "age": 30, "city": "Shanghai"})",  // repeated name and city
            R"({"name": "Alice", "age": 40, "city": "Beijing"})", // repeated name and city
    };
    TabletSchemaPB tablet_schema_pb;
    auto* column_pb = tablet_schema_pb.add_column();
    column_pb->set_name("test_json");
    column_pb->set_type("JSON");
    column_pb->set_is_key(false);
    column_pb->set_is_nullable(true);
    column_pb->set_unique_id(0);
    auto tablet_schema = TabletSchema::create(tablet_schema_pb);
    SchemaPtr chunk_schema = std::make_shared<Schema>(tablet_schema->schema());
    TabletColumn json_tablet_column = create_with_default_value<TYPE_JSON>("");

    auto write_data = [tablet_schema](std::unique_ptr<WritableFile> wfile, const std::vector<std::string>& json_strings,
                                      const SegmentWriterOptions& opts, std::unique_ptr<SegmentWriter>* out_writer) {
        auto writer = std::make_unique<SegmentWriter>(std::move(wfile), 0, tablet_schema, opts);
        ASSERT_OK(writer->init());

        auto json_column = ColumnHelper::create_column(TypeDescriptor::create_json_type(), true);
        SchemaPtr chunk_schema = std::make_shared<Schema>(tablet_schema->schema());
        auto chunk = std::make_shared<Chunk>(Columns{json_column}, chunk_schema);

        // Add JSON data to chunk
        for (const auto& json_str : json_strings) {
            JsonValue json_value;
            ASSERT_OK(JsonValue::parse(json_str, &json_value));
            json_column->append_datum(Datum(&json_value));
        }

        ASSERT_OK(writer->append_chunk(*chunk));

        uint64_t index_size = 0;
        uint64_t footer_position = 0;
        ASSERT_OK(writer->finalize_columns(&index_size));
        ASSERT_OK(writer->finalize_footer(&footer_position));
        *out_writer = std::move(writer);
    };

    // Write data with global dictionary
    {
        // Create global dictionary for sub-columns
        GlobalDictByNameMaps global_dicts;
        GlobalDictMap name_dict = {{"Alice", 0}, {"Bob", 1}, {"Charlie", 2}};
        GlobalDictMap city_dict = {{"Beijing", 0}, {"Shanghai", 1}, {"Guangzhou", 2}};

        // Store dictionaries with column names
        global_dicts["test_json.name"] = GlobalDictsWithVersion<GlobalDictMap>{name_dict, 1};
        global_dicts["test_json.city"] = GlobalDictsWithVersion<GlobalDictMap>{city_dict, 1};

        SegmentWriterOptions opts;
        opts.global_dicts = &global_dicts;
        opts.flat_json_config = std::make_shared<FlatJsonConfig>();
        opts.flat_json_config->set_flat_json_enabled(true);
        ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(file_name));

        auto json_column = ColumnHelper::create_column(TypeDescriptor::create_json_type(), true);
        auto chunk = std::make_shared<Chunk>(Columns{json_column}, chunk_schema);

        // Add JSON data to chunk
        for (const auto& json_str : json_strings) {
            JsonValue json_value;
            ASSERT_OK(JsonValue::parse(json_str, &json_value));
            json_column->append_datum(Datum(&json_value));
        }

        std::unique_ptr<SegmentWriter> writer;
        write_data(std::move(wfile), json_strings, opts, &writer);

        // Check global dictionary validity
        const auto& dict_valid_info = writer->global_dict_columns_valid_info();
        ASSERT_TRUE(dict_valid_info.count("test_json.name") > 0);
        ASSERT_TRUE(dict_valid_info.at("test_json.name")); // Should be valid
        ASSERT_TRUE(dict_valid_info.count("test_json.city") > 0);
        ASSERT_TRUE(dict_valid_info.at("test_json.city")); // Should be valid
    }

    // Another batch of JSON strings with different values
    std::vector<std::string> json_strings2 = {
            R"({"name": "David", "age": 22, "city": "Shenzhen"})", R"({"name": "Eve", "age": 29, "city": "Hangzhou"})",
            R"({"name": "Frank", "age": 33, "city": "Chengdu"})",  R"({"name": "Grace", "age": 27, "city": "Wuhan"})",
            R"({"name": "Heidi", "age": 31, "city": "Nanjing"})",  R"({"name": "Ivan", "age": 26, "city": "Suzhou"})"};

    // Test with invalid global dictionary (missing values)
    {
        ASSIGN_OR_ABORT(auto wfile2, fs->new_writable_file(file_name + "_invalid"));

        // Create incomplete dictionary that doesn't contain all values
        GlobalDictByNameMaps invalid_global_dicts;

        GlobalDictMap incomplete_name_dict;
        incomplete_name_dict["Alice"] = 0;
        incomplete_name_dict["Bob"] = 1;

        invalid_global_dicts["test_json.name"] = GlobalDictsWithVersion<GlobalDictMap>{incomplete_name_dict, 1};

        SegmentWriterOptions seg_opts;
        seg_opts.global_dicts = &invalid_global_dicts;
        seg_opts.flat_json_config = std::make_shared<FlatJsonConfig>();
        seg_opts.flat_json_config->set_flat_json_enabled(true);

        auto json_column = ColumnHelper::create_column(TypeDescriptor::create_json_type(), true);
        auto chunk = std::make_shared<Chunk>(Columns{json_column}, chunk_schema);

        // Add JSON data to chunk
        for (const auto& json_str : json_strings2) {
            JsonValue json_value;
            ASSERT_OK(JsonValue::parse(json_str, &json_value));
            json_column->append_datum(Datum(&json_value));
        }

        std::unique_ptr<SegmentWriter> writer;
        write_data(std::move(wfile2), json_strings2, seg_opts, &writer);

        const auto& dict_valid_info = writer->global_dict_columns_valid_info();
        ASSERT_TRUE(dict_valid_info.count("test_json.city") > 0);
        ASSERT_FALSE(dict_valid_info.at("test_json.city")); // Should be invalid
    }
    // Clean up test files
    ASSERT_OK(fs->delete_file(file_name));
    ASSERT_OK(fs->delete_file(file_name + "_invalid"));

    // Test case 1: Generate JSON data with same fields but different data types
    std::vector<std::string> json_strings_different_types = {
            R"({"name": "Alice", "age": 30, "city": "Beijing"})",
            R"({"name": "Bob", "age": "25", "city": "Shanghai"})", // age is string instead of number
            R"({"name": "Charlie", "age": 28, "city": "Guangzhou"})",
            R"({"name": "David", "age": 35.5, "city": "Shenzhen"})", // age is float instead of integer
            R"({"name": "Eve", "age": 29, "city": 12345})",          // city is number instead of string
            R"({"name": "Frank", "age": 33, "city": "Chengdu"})"};

    // Test with global dictionary that expects consistent data types
    {
        ASSIGN_OR_ABORT(auto wfile3, fs->new_writable_file(file_name + "_different_types"));

        // Create global dictionary expecting consistent data types
        GlobalDictByNameMaps type_consistent_global_dicts;
        GlobalDictMap name_dict = {{"Alice", 0}, {"Bob", 1}, {"Charlie", 2}, {"David", 3}, {"Eve", 4}, {"Frank", 5}};
        GlobalDictMap age_dict = {{"30", 0}, {"25", 1}, {"28", 2}, {"35", 3}, {"29", 4}, {"33", 5}}; // All as strings
        GlobalDictMap city_dict = {{"Beijing", 0}, {"Shanghai", 1}, {"Guangzhou", 2}, {"Shenzhen", 3}, {"Chengdu", 4}};

        type_consistent_global_dicts["test_json.name"] = GlobalDictsWithVersion<GlobalDictMap>{name_dict, 1};
        type_consistent_global_dicts["test_json.age"] = GlobalDictsWithVersion<GlobalDictMap>{age_dict, 1};
        type_consistent_global_dicts["test_json.city"] = GlobalDictsWithVersion<GlobalDictMap>{city_dict, 1};

        SegmentWriterOptions seg_opts3;
        seg_opts3.global_dicts = &type_consistent_global_dicts;
        seg_opts3.flat_json_config = std::make_shared<FlatJsonConfig>();
        seg_opts3.flat_json_config->set_flat_json_enabled(true);

        auto json_column = ColumnHelper::create_column(TypeDescriptor::create_json_type(), true);
        auto chunk = std::make_shared<Chunk>(Columns{json_column}, chunk_schema);

        // Add JSON data with different data types to chunk
        for (const auto& json_str : json_strings_different_types) {
            JsonValue json_value;
            ASSERT_OK(JsonValue::parse(json_str, &json_value));
            json_column->append_datum(Datum(&json_value));
        }

        std::unique_ptr<SegmentWriter> writer;
        write_data(std::move(wfile3), json_strings_different_types, seg_opts3, &writer);

        // Check that dictionaries are invalidated due to data type inconsistencies
        const auto& dict_valid_info = writer->global_dict_columns_valid_info();
        ASSERT_TRUE(dict_valid_info.count("test_json.name") > 0);
        ASSERT_TRUE(dict_valid_info.at("test_json.name")); // Name should still be valid (all strings)
        ASSERT_TRUE(dict_valid_info.count("test_json.age") > 0);
        ASSERT_FALSE(dict_valid_info.at("test_json.age")); // Age should be invalid (mixed types)
        ASSERT_TRUE(dict_valid_info.count("test_json.city") > 0);
        ASSERT_FALSE(dict_valid_info.at("test_json.city")); // City should be invalid (mixed types)
    }

    // Test case 2: Verify dict invalidation with more complex type mismatches
    std::vector<std::string> json_strings_complex_types = {
            R"({"user": {"id": 1001, "name": "Alice", "active": true, "score": 95.5}})",
            R"({"user": {"id": [1,2,3], "name": "Bob", "active": "yes", "score": 88}})", // id is array, active is string, score is int
            R"({"user": {"id": 1003, "name": "Charlie", "active": false, "score": 92.0}})",
            R"({"user": {"id": 1004, "name": "David", "active": 1, "score": "85.5"}})", // active is number, score is string
            R"({"user": {"id": 1005, "name": {"nick": "murphy"}, "active": true, "score": 90}})" // score is int, name is object
    };

    {
        ASSIGN_OR_ABORT(auto wfile4, fs->new_writable_file(file_name + "_complex_types"));

        // Create global dictionary for nested fields
        GlobalDictByNameMaps complex_global_dicts;
        GlobalDictMap user_id_dict = {{"1001", 0}, {"1002", 1}, {"1003", 2}, {"1004", 3}, {"1005", 4}};
        GlobalDictMap user_name_dict = {{"Alice", 0}, {"Bob", 1}, {"Charlie", 2}, {"David", 3}, {"Eve", 4}};
        GlobalDictMap user_active_dict = {{"true", 0}, {"false", 1}};
        GlobalDictMap user_score_dict = {{"95.5", 0}, {"88", 1}, {"92.0", 2}, {"85.5", 3}, {"90", 4}};

        complex_global_dicts["test_json.user.id"] = GlobalDictsWithVersion<GlobalDictMap>{user_id_dict, 1};
        complex_global_dicts["test_json.user.name"] = GlobalDictsWithVersion<GlobalDictMap>{user_name_dict, 1};
        complex_global_dicts["test_json.user.active"] = GlobalDictsWithVersion<GlobalDictMap>{user_active_dict, 1};
        complex_global_dicts["test_json.user.score"] = GlobalDictsWithVersion<GlobalDictMap>{user_score_dict, 1};

        SegmentWriterOptions seg_opts4;
        seg_opts4.global_dicts = &complex_global_dicts;
        seg_opts4.flat_json_config = std::make_shared<FlatJsonConfig>();
        seg_opts4.flat_json_config->set_flat_json_enabled(true);

        auto json_column = ColumnHelper::create_column(TypeDescriptor::create_json_type(), true);
        auto chunk = std::make_shared<Chunk>(Columns{json_column}, chunk_schema);

        // Add JSON data with complex type mismatches to chunk
        for (const auto& json_str : json_strings_complex_types) {
            JsonValue json_value;
            ASSERT_OK(JsonValue::parse(json_str, &json_value));
            json_column->append_datum(Datum(&json_value));
        }

        std::unique_ptr<SegmentWriter> writer;
        write_data(std::move(wfile4), json_strings_complex_types, seg_opts4, &writer);

        // Check that dictionaries are invalidated due to complex type mismatches
        const auto& dict_valid_info = writer->global_dict_columns_valid_info();
        ASSERT_TRUE(dict_valid_info.count("test_json.user.name") > 0);
        ASSERT_FALSE(dict_valid_info.at("test_json.user.name")); // Name should be invalid(mixed object/string)
        ASSERT_TRUE(dict_valid_info.count("test_json.user.id") > 0);
        ASSERT_FALSE(dict_valid_info.at("test_json.user.id")); // ID should be invalid (mixed int/string)
        ASSERT_TRUE(dict_valid_info.count("test_json.user.active") > 0);
        ASSERT_FALSE(
                dict_valid_info.at("test_json.user.active")); // Active should be invalid (mixed bool/string/number)
        ASSERT_TRUE(dict_valid_info.count("test_json.user.score") > 0);
        ASSERT_FALSE(dict_valid_info.at("test_json.user.score")); // Score should be invalid (mixed float/int/string)
    }

    // Clean up additional test files
    ASSERT_OK(fs->delete_file(file_name + "_different_types"));
    ASSERT_OK(fs->delete_file(file_name + "_complex_types"));
}

} // namespace starrocks
