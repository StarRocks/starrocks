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

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

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
#include "util/json_flattener.h"

namespace starrocks {

// NOLINTNEXTLINE
static const std::string TEST_DIR = "/flat_json_column_rw_test";

class FlatJsonColumnCompactTest : public testing::Test {
public:
    FlatJsonColumnCompactTest() = default;

    ~FlatJsonColumnCompactTest() override = default;

protected:
    void SetUp() override { _meta.reset(new ColumnMetaPB()); }

    void TearDown() override {}

    std::shared_ptr<Segment> create_dummy_segment(const std::shared_ptr<FileSystem>& fs, const std::string& fname) {
        return std::make_shared<Segment>(fs, FileInfo{fname}, 1, _dummy_segment_schema, nullptr);
    }

    ColumnPtr normal_json(const std::string& json, bool is_nullable) {
        auto json_col = JsonColumn::create();
        if ("NULL" != json) {
            auto* json_column = down_cast<JsonColumn*>(json_col.get());
            ASSIGN_OR_ABORT(auto jv, JsonValue::parse(json));
            json_column->append(&jv);
        }

        if (is_nullable) {
            auto null_col = NullColumn::create();
            null_col->append("NULL" == json);
            return NullableColumn::create(json_col, null_col);
        }
        return json_col;
    }

    ColumnPtr flat_json(const std::string& json, bool is_nullable) {
        auto json_col = JsonColumn::create();
        if ("NULL" != json) {
            auto flat_col = JsonColumn::create();
            auto* flat_column = down_cast<JsonColumn*>(flat_col.get());
            ASSIGN_OR_ABORT(auto jv, JsonValue::parse(json));
            flat_column->append(&jv);

            JsonPathDeriver deriver;
            deriver.derived({flat_column});
            JsonFlattener flattener(deriver);
            flattener.flatten(flat_column);
            json_col->set_flat_columns(deriver.flat_paths(), deriver.flat_types(), flattener.mutable_result());
        }

        if (is_nullable) {
            auto null_col = NullColumn::create();
            null_col->append("NULL" == json);
            return NullableColumn::create(json_col, null_col);
        }
        return json_col;
    }

    void test_json(ColumnWriterOptions& writer_opts, std::vector<ColumnPtr>& jsons, ColumnPtr& read_col) {
        auto fs = std::make_shared<MemoryFileSystem>();
        ASSERT_TRUE(fs->create_dir(TEST_DIR).ok());

        TabletColumn json_tablet_column = create_with_default_value<TYPE_JSON>("");
        TypeInfoPtr type_info = get_type_info(json_tablet_column);

        const std::string fname = TEST_DIR + "/test_flat_json_compact1.data";
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
            writer_opts.meta->set_is_nullable(jsons[0]->is_nullable());
            writer_opts.need_zone_map = false;
            writer_opts.is_compaction = true;

            ASSIGN_OR_ABORT(auto writer, ColumnWriter::create(writer_opts, &json_tablet_column, wfile.get()));
            ASSERT_OK(writer->init());

            for (auto& json : jsons) {
                ASSERT_TRUE(writer->append(*json).ok());
            }

            ASSERT_TRUE(writer->finish().ok());
            ASSERT_TRUE(writer->write_data().ok());
            ASSERT_TRUE(writer->write_ordinal_index().ok());

            // close the file
            ASSERT_TRUE(wfile->close().ok());
        }
        LOG(INFO) << "Finish writing";

        auto res = ColumnReader::create(_meta.get(), segment.get(), nullptr);
        ASSERT_TRUE(res.ok());
        auto reader = std::move(res).value();

        {
            ASSIGN_OR_ABORT(auto iter, reader->new_iterator(nullptr));
            ASSIGN_OR_ABORT(auto read_file, fs->new_random_access_file(fname));

            ColumnIteratorOptions iter_opts;
            OlapReaderStatistics stats;
            iter_opts.stats = &stats;
            iter_opts.read_file = read_file.get();
            ASSERT_TRUE(iter->init(iter_opts).ok());

            // sequence read
            auto st = iter->seek_to_first();
            ASSERT_TRUE(st.ok()) << st.to_string();

            size_t rows_read;
            std::for_each(jsons.begin(), jsons.end(), [&](ColumnPtr& json) { rows_read += json->size(); });
            st = iter->next_batch(&rows_read, read_col.get());
            ASSERT_TRUE(st.ok());
        }
    }

private:
    std::shared_ptr<TabletSchema> _dummy_segment_schema;
    std::shared_ptr<ColumnMetaPB> _meta;
};

TEST_F(FlatJsonColumnCompactTest, testJsonCompactToJson) {
    // clang-format off
    Columns jsons = {
            normal_json(R"({"a": 1, "b": 21})", false),
            normal_json(R"({"a": 2, "b": 22})", false),
            normal_json(R"({"a": 3, "b": 23})", false),
            normal_json(R"({"a": 4, "b": 24})", false),
            normal_json(R"({"a": 5, "b": 25})", false)
    };
    // clang-format on

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    for (size_t i = 0; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i], read_json->debug_item(i));
    }
}

TEST_F(FlatJsonColumnCompactTest, testNullJsonCompactToJson) {
    // clang-format off
    Columns jsons = {
            normal_json(R"({"a": 1, "b": 21})", true),
            normal_json(R"({"a": 2, "b": 22})", true),
            normal_json(R"({"a": 3, "b": 23})", true),
            normal_json(R"({"a": 4, "b": 24})", true),
            normal_json(R"(NULL)", true)
    };
    // clang-format on

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    for (size_t i = 0; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i], read_json->debug_item(i));
    }
}

TEST_F(FlatJsonColumnCompactTest, testFlatJsonCompactToJson) {
    // clang-format off
    Columns jsons = {
            flat_json(R"({"a": 1, "b": 21})", false),
            flat_json(R"({"a": 2, "b": 22})", false),
            flat_json(R"({"a": 3, "b": 23})", false),
            flat_json(R"({"a": 4, "b": 24})", false),
            flat_json(R"({"a": 5, "b": 25})", false),
    };
    // clang-format on

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    for (size_t i = 0; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i], read_json->debug_item(i));
    }
}

TEST_F(FlatJsonColumnCompactTest, testFlatJsonCompactToJson2) {
    // clang-format off
    Columns jsons = {
            flat_json(R"({"a": 1, "b": 21})", false),
            flat_json(R"({"a": 2, "b": 22})", false),
            flat_json(R"({"a": 3, "b": 23, "c": 33})", false),
            flat_json(R"({"a": 4, "b": 24, "c": 34})", false),
            flat_json(R"({"a": 5, "b": 25, "c": 35})", false),
    };
    // clang-format on

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    for (size_t i = 0; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i], read_json->debug_item(i));
    }
}

TEST_F(FlatJsonColumnCompactTest, testFlatJsonCompactToJson3) {
    // clang-format off
    Columns jsons = {
            flat_json(R"({"a": 1, "b": 21, "g": {}})", false),
            flat_json(R"({"a": 2, "b": 22, "k": "abc"})", false),
            flat_json(R"({"a": 3, "b": 23, "c": 33})", false),
            flat_json(R"({"a": 4, "b": 24, "d": 34})", false),
            flat_json(R"({"a": 5, "b": 25, "e": 35})", false),
    };
    // clang-format on

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    for (size_t i = 0; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i], read_json->debug_item(i));
    }
}

TEST_F(FlatJsonColumnCompactTest, testFlatJsonCompactToJson4) {
    // clang-format off
    Columns jsons = {
            flat_json(R"({"a": 1, "b": 21, "b1": {"b2": 1, "b3": {"b4": "ab1", "b5": [1, 2, 3]}}, "g": {}})", false),
            flat_json(R"({"a": 2, "b": 22, "b1": {"b2": 2, "b3": {"b4": "ab2", "b5": {}}}, "k": "abc"})", false),
            flat_json(R"({"a": 3, "b": 23, "b1": {"b2": 3, "b3": {"b4": "ab3", "b5": "a"}}, "c": 33})", false),
            flat_json(R"({"a": 4, "b": 24, "b1": {"b2": 4, "b3": {"b4": "ab4", "b5": 1}}, "d": 34})", false),
            flat_json(R"({"a": 5, "b": 25, "b1": {"b2": 5, "b3": {"b4": "ab5", "b5": false}}, "e": 35})", false),
    };
    // clang-format on

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    for (size_t i = 0; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i], read_json->debug_item(i));
    }
}

TEST_F(FlatJsonColumnCompactTest, testHyperJsonCompactToJson) {
    // clang-format off
    Columns jsons = {
            flat_json(R"({"a": 1, "b": 21, "b1": {"b2": 1, "b3": {"b4": "ab1", "b5": [1, 2, 3]}}, "g": {}})", false),
            flat_json(R"({"a": 2, "b": 22, "b1": {"b2": 2, "b3": {"b4": "ab2", "b5": {}}}, "k": "abc"})", false),
            normal_json(R"({"a": 3, "b": 23, "b1": {"b2": 3, "b3": {"b4": "ab3", "b5": "a"}}, "c": 33})", false),
            normal_json(R"({"a": 4, "b": 24, "b1": {"b2": 4, "b3": {"b4": "ab4", "b5": 1}}, "d": 34})", false),
            normal_json(R"({"a": 5, "b": 25, "b1": {"b2": 5, "b3": {"b4": "ab5", "b5": false}}, "e": 35})", false),
    };
    // clang-format on

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    for (size_t i = 0; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i], read_json->debug_item(i));
    }
}

TEST_F(FlatJsonColumnCompactTest, testNullFlatJsonCompactToJson) {
    // clang-format off
    Columns jsons = {
            flat_json(R"({"a": 1, "b": 21})", true),
            flat_json(R"({"a": 2, "b": 22})", true),
            flat_json(R"({"a": 3, "b": 23})", true),
            flat_json(R"({"a": 4, "b": 24})", true),
            flat_json(R"(NULL)", true),
    };
    // clang-format on

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    for (size_t i = 0; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i], read_json->debug_item(i));
    }
}

TEST_F(FlatJsonColumnCompactTest, testNullFlatJsonCompactToJson2) {
    // clang-format off
    Columns jsons = {
            flat_json(R"({"a": 1, "b": 21})", true),
            flat_json(R"({"a": 2, "b": 22})", true),
            flat_json(R"({"a": 3, "b": 23, "c": 33})", true),
            flat_json(R"({"a": 4, "b": 24, "c": 34})", true),
            flat_json(R"(NULL)", true),
    };
    // clang-format on

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    for (size_t i = 0; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i], read_json->debug_item(i));
    }
}

TEST_F(FlatJsonColumnCompactTest, testNullFlatJsonCompactToJson3) {
    // clang-format off
    Columns jsons = {
            flat_json(R"({"a": 1, "b": 21, "g": {}})", true),
            flat_json(R"({"a": 2, "b": 22, "k": "abc"})", true),
            flat_json(R"({"a": 3, "b": 23, "c": 33})", true),
            flat_json(R"({"a": 4, "b": 24, "d": 34})", true),
            flat_json(R"(NULL)", true),
    };
    // clang-format on

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    for (size_t i = 0; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i], read_json->debug_item(i));
    }
}

TEST_F(FlatJsonColumnCompactTest, testNullFlatJsonCompactToJson4) {
    // clang-format off
    Columns jsons = {
            flat_json(R"({"a": 1, "b": 21, "b1": {"b2": 1, "b3": {"b4": "ab1", "b5": [1, 2, 3]}}, "g": {}})", true),
            flat_json(R"({"a": 2, "b": 22, "b1": {"b2": 2, "b3": {"b4": "ab2", "b5": {}}}, "k": "abc"})", true),
            flat_json(R"({"a": 3, "b": 23, "b1": {"b2": 3, "b3": {"b4": "ab3", "b5": "a"}}, "c": 33})", true),
            flat_json(R"({"a": 4, "b": 24, "b1": {"b2": 4, "b3": {"b4": "ab4", "b5": 1}}, "d": 34})", true),
            flat_json(R"(NULL)", true),
    };
    // clang-format on

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    for (size_t i = 0; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i], read_json->debug_item(i));
    }
}

TEST_F(FlatJsonColumnCompactTest, testNullHyperJsonCompactToJson) {
    // clang-format off
    Columns jsons = {
            flat_json(R"({"a": 1, "b": 21, "b1": {"b2": 1, "b3": {"b4": "ab1", "b5": [1, 2, 3]}}, "g": {}})", true),
            flat_json(R"({"a": 2, "b": 22, "b1": {"b2": 2, "b3": {"b4": "ab2", "b5": {}}}, "k": "abc"})", true),
            normal_json(R"({"a": 3, "b": 23, "b1": {"b2": 3, "b3": {"b4": "ab3", "b5": "a"}}, "c": 33})", true),
            normal_json(R"({"a": 4, "b": 24, "b1": {"b2": 4, "b3": {"b4": "ab4", "b5": 1}}, "d": 34})", true),
            normal_json(R"({"a": 5, "b": 25, "b1": {"b2": 5, "b3": {"b4": "ab5", "b5": false}}, "e": 35})", true),
    };
    // clang-format on

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    for (size_t i = 0; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i], read_json->debug_item(i));
    }
}

TEST_F(FlatJsonColumnCompactTest, testJsonCompactToFlatJson) {
    // clang-format off
    Columns jsons = {
            normal_json(R"({"a": 1, "b": 21})", false),
            normal_json(R"({"a": 2, "b": 22})", false),
            normal_json(R"({"a": 3, "b": 23})", false),
            normal_json(R"({"a": 4, "b": 24})", false),
            normal_json(R"({"a": 5, "b": 25})", false)
    };
    // clang-format on

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    for (size_t i = 0; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i], read_json->debug_item(i));
    }
}

TEST_F(FlatJsonColumnCompactTest, testNullJsonCompactToFlatJson) {
    // clang-format off
    Columns jsons = {
            normal_json(R"({"a": 1, "b": 21})", true),
            normal_json(R"({"a": 2, "b": 22})", true),
            normal_json(R"({"a": 3, "b": 23})", true),
            normal_json(R"({"a": 4, "b": 24})", true),
            normal_json(R"(NULL)", true)
    };
    // clang-format on

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    for (size_t i = 0; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i], read_json->debug_item(i));
    }
}

TEST_F(FlatJsonColumnCompactTest, testFlatJsonCompactToFlatJson) {
    // clang-format off
    Columns jsons = {
            flat_json(R"({"a": 1, "b": 21})", false),
            flat_json(R"({"a": 2, "b": 22})", false),
            flat_json(R"({"a": 3, "b": 23})", false),
            flat_json(R"({"a": 4, "b": 24})", false),
            flat_json(R"({"a": 5, "b": 25})", false),
    };
    // clang-format on

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    for (size_t i = 0; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i], read_json->debug_item(i));
    }
}

TEST_F(FlatJsonColumnCompactTest, testFlatJsonCompactToFlatJson2) {
    // clang-format off
    Columns jsons = {
            flat_json(R"({"a": 1, "b": 21})", false),
            flat_json(R"({"a": 2, "b": 22})", false),
            flat_json(R"({"a": 3, "b": 23, "c": 33})", false),
            flat_json(R"({"a": 4, "b": 24, "c": 34})", false),
            flat_json(R"({"a": 5, "b": 25, "c": 35})", false),
    };
    // clang-format on

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    for (size_t i = 0; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i], read_json->debug_item(i));
    }
}

TEST_F(FlatJsonColumnCompactTest, testFlatJsonCompactToFlatJson3) {
    // clang-format off
    Columns jsons = {
            flat_json(R"({"a": 1, "b": 21, "g": {}})", false),
            flat_json(R"({"a": 2, "b": 22, "k": "abc"})", false),
            flat_json(R"({"a": 3, "b": 23, "c": 33})", false),
            flat_json(R"({"a": 4, "b": 24, "d": 34})", false),
            flat_json(R"({"a": 5, "b": 25, "e": 35})", false),
    };
    // clang-format on

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    for (size_t i = 0; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i], read_json->debug_item(i));
    }
}

TEST_F(FlatJsonColumnCompactTest, testFlatJsonCompactToFlatJson4) {
    // clang-format off
    Columns jsons = {
            flat_json(R"({"a": 1, "b": 21, "b1": {"b2": 1, "b3": {"b4": "ab1", "b5": [1, 2, 3]}}, "g": {}})", false),
            flat_json(R"({"a": 2, "b": 22, "b1": {"b2": 2, "b3": {"b4": "ab2", "b5": {}}}, "k": "abc"})", false),
            flat_json(R"({"a": 3, "b": 23, "b1": {"b2": 3, "b3": {"b4": "ab3", "b5": "a"}}, "c": 33})", false),
            flat_json(R"({"a": 4, "b": 24, "b1": {"b2": 4, "b3": {"b4": "ab4", "b5": 1}}, "d": 34})", false),
            flat_json(R"({"a": 5, "b": 25, "b1": {"b2": 5, "b3": {"b4": "ab5", "b5": false}}, "e": 35})", false),
    };
    // clang-format on

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    for (size_t i = 0; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i], read_json->debug_item(i));
    }
}

TEST_F(FlatJsonColumnCompactTest, testHyperJsonCompactToFlatJson) {
    // clang-format off
    Columns jsons = {
            flat_json(R"({"a": 1, "b": 21, "b1": {"b2": 1, "b3": {"b4": "ab1", "b5": [1, 2, 3]}}, "g": {}})", false),
            flat_json(R"({"a": 2, "b": 22, "b1": {"b2": 2, "b3": {"b4": "ab2", "b5": {}}}, "k": "abc"})", false),
            normal_json(R"({"a": 3, "b": 23, "b1": {"b2": 3, "b3": {"b4": "ab3", "b5": "a"}}, "c": 33})", false),
            normal_json(R"({"a": 4, "b": 24, "b1": {"b2": 4, "b3": {"b4": "ab4", "b5": 1}}, "d": 34})", false),
            normal_json(R"({"a": 5, "b": 25, "b1": {"b2": 5, "b3": {"b4": "ab5", "b5": false}}, "e": 35})", false),
    };
    // clang-format on

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    for (size_t i = 0; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i], read_json->debug_item(i));
    }
}

TEST_F(FlatJsonColumnCompactTest, testNullFlatJsonCompactToFlatJson) {
    // clang-format off
    Columns jsons = {
            flat_json(R"({"a": 1, "b": 21})", true),
            flat_json(R"({"a": 2, "b": 22})", true),
            flat_json(R"({"a": 3, "b": 23})", true),
            flat_json(R"({"a": 4, "b": 24})", true),
            flat_json(R"(NULL)", true),
    };
    // clang-format on

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    for (size_t i = 0; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i], read_json->debug_item(i));
    }
}

TEST_F(FlatJsonColumnCompactTest, testNullFlatJsonCompactToFlatJson2) {
    // clang-format off
    Columns jsons = {
            flat_json(R"({"a": 1, "b": 21})", true),
            flat_json(R"({"a": 2, "b": 22})", true),
            flat_json(R"({"a": 3, "b": 23, "c": 33})", true),
            flat_json(R"({"a": 4, "b": 24, "c": 34})", true),
            flat_json(R"(NULL)", true),
    };
    // clang-format on

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    for (size_t i = 0; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i], read_json->debug_item(i));
    }
}

TEST_F(FlatJsonColumnCompactTest, testNullFlatJsonCompactToFlatJson3) {
    // clang-format off
    Columns jsons = {
            flat_json(R"({"a": 1, "b": 21, "g": {}})", true),
            flat_json(R"({"a": 2, "b": 22, "k": "abc"})", true),
            flat_json(R"({"a": 3, "b": 23, "c": 33})", true),
            flat_json(R"({"a": 4, "b": 24, "d": 34})", true),
            flat_json(R"(NULL)", true),
    };
    // clang-format on

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    for (size_t i = 0; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i], read_json->debug_item(i));
    }
}

TEST_F(FlatJsonColumnCompactTest, testNullFlatJsonCompactToFlatJson4) {
    // clang-format off
    Columns jsons = {
            flat_json(R"({"a": 1, "b": 21, "b1": {"b2": 1, "b3": {"b4": "ab1", "b5": [1, 2, 3]}}, "g": {}})", true),
            flat_json(R"({"a": 2, "b": 22, "b1": {"b2": 2, "b3": {"b4": "ab2", "b5": {}}}, "k": "abc"})", true),
            flat_json(R"({"a": 3, "b": 23, "b1": {"b2": 3, "b3": {"b4": "ab3", "b5": "a"}}, "c": 33})", true),
            flat_json(R"({"a": 4, "b": 24, "b1": {"b2": 4, "b3": {"b4": "ab4", "b5": 1}}, "d": 34})", true),
            flat_json(R"(NULL)", true),
    };
    // clang-format on

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    for (size_t i = 0; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i], read_json->debug_item(i));
    }
}

TEST_F(FlatJsonColumnCompactTest, testNullHyperJsonCompactToFlatJson) {
    // clang-format off
    Columns jsons = {
            flat_json(R"({"a": 1, "b": 21, "b1": {"b2": 1, "b3": {"b4": "ab1", "b5": [1, 2, 3]}}, "g": {}})", true),
            flat_json(R"({"a": 2, "b": 22, "b1": {"b2": 2, "b3": {"b4": "ab2", "b5": {}}}, "k": "abc"})", true),
            normal_json(R"({"a": 3, "b": 23, "b1": {"b2": 3, "b3": {"b4": "ab3", "b5": "a"}}, "c": 33})", true),
            normal_json(R"({"a": 4, "b": 24, "b1": {"b2": 4, "b3": {"b4": "ab4", "b5": 1}}, "d": 34})", true),
            normal_json(R"({"a": 5, "b": 25, "b1": {"b2": 5, "b3": {"b4": "ab5", "b5": false}}, "e": 35})", true),
    };
    // clang-format on

    ColumnPtr read_col = JsonColumn::create();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = down_cast<JsonColumn*>(read_col.get());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    for (size_t i = 0; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i], read_json->debug_item(i));
    }
}


} // namespace starrocks
