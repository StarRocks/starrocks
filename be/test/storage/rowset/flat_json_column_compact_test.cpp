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

    void TearDown() override {
        config::json_flat_null_factor = 0.3;
        config::json_flat_sparsity_factor = 0.9;
    }

    std::shared_ptr<Segment> create_dummy_segment(const std::shared_ptr<FileSystem>& fs, const std::string& fname) {
        return std::make_shared<Segment>(fs, FileInfo{fname}, 1, _dummy_segment_schema, nullptr);
    }

    ColumnPtr normal_json(const std::string& json, bool is_nullable) {
        auto json_col = JsonColumn::create();
        auto* json_column = down_cast<JsonColumn*>(json_col.get());
        if ("NULL" != json) {
            ASSIGN_OR_ABORT(auto jv, JsonValue::parse(json));
            json_column->append(&jv);
        } else {
            json_column->append(JsonValue());
        }

        if (is_nullable) {
            auto null_col = NullColumn::create();
            null_col->append("NULL" == json);
            return NullableColumn::create(std::move(json_col), std::move(null_col));
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
        } else {
            return normal_json("NULL", is_nullable);
        }

        if (is_nullable) {
            auto null_col = NullColumn::create();
            null_col->append("NULL" == json);
            return NullableColumn::create(std::move(json_col), std::move(null_col));
        }
        return json_col;
    }

    ColumnPtr more_flat_json(const std::vector<std::string>& jsons, bool is_nullable) {
        auto json_col = JsonColumn::create();

        auto flat_col = JsonColumn::create();
        auto* flat_column = down_cast<JsonColumn*>(flat_col.get());
        auto null_col = NullColumn::create();

        for (const auto& json : jsons) {
            if ("NULL" != json) {
                ASSIGN_OR_ABORT(auto jv, JsonValue::parse(json));
                flat_column->append(&jv);
            } else {
                flat_column->append_default();
            }
            null_col->append("NULL" == json);
        }

        JsonPathDeriver deriver;
        deriver.derived({flat_column});
        JsonFlattener flattener(deriver);
        flattener.flatten(flat_column);
        json_col->set_flat_columns(deriver.flat_paths(), deriver.flat_types(), flattener.mutable_result());

        if (is_nullable) {
            return NullableColumn::create(std::move(json_col), std::move(null_col));
        }
        return json_col;
    }

    void test_json(ColumnWriterOptions& writer_opts, std::vector<ColumnPtr>& jsons, ColumnPtr& read_col,
                   ColumnAccessPath* path = nullptr) {
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

            size_t rows_read;
            std::for_each(jsons.begin(), jsons.end(), [&](ColumnPtr& json) { rows_read += json->size(); });
            st = iter->next_batch(&rows_read, read_col.get());
            ASSERT_TRUE(st.ok());
        }
    }

    void test_compact_path(std::vector<ColumnPtr>& jsons, JsonPathDeriver* deriver) {
        auto fs = std::make_shared<MemoryFileSystem>();
        ASSERT_TRUE(fs->create_dir(TEST_DIR).ok());

        TabletColumn json_tablet_column = create_with_default_value<TYPE_JSON>("");
        TypeInfoPtr type_info = get_type_info(json_tablet_column);

        std::vector<std::shared_ptr<Segment>> segments;
        std::vector<std::unique_ptr<ColumnReader>> unique_readers;
        std::vector<const ColumnReader*> readers;
        for (size_t k = 0; k < jsons.size(); k++) {
            const std::string fname = TEST_DIR + fmt::format("/test_flat_json_compact{}.data", k);
            auto segment = create_dummy_segment(fs, fname);
            segments.push_back(segment);
            ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(fname));
            // write data
            ColumnWriterOptions writer_opts;
            writer_opts.need_flat = true;
            ColumnMetaPB column_meta;
            writer_opts.meta = &column_meta;
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
            ASSERT_TRUE(writer->append(*jsons[k]).ok());

            ASSERT_TRUE(writer->finish().ok());
            ASSERT_TRUE(writer->write_data().ok());
            ASSERT_TRUE(writer->write_ordinal_index().ok());

            // mock segment rows
            segment->set_num_rows(jsons[k]->size());

            // close the file
            ASSERT_TRUE(wfile->close().ok());

            auto res = ColumnReader::create(&column_meta, segment.get(), nullptr);
            ASSERT_TRUE(res.ok());
            auto reader = std::move(res).value();
            unique_readers.emplace_back(std::move(reader));
            readers.push_back(unique_readers.back().get());
        }
        deriver->derived(readers);
    }

    JsonColumn* get_json_column(ColumnPtr& col) {
        if (col->is_nullable()) {
            return down_cast<JsonColumn*>(down_cast<NullableColumn*>(col.get())->data_column().get());
        }
        return down_cast<JsonColumn*>(col.get());
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

    ColumnPtr read_col = jsons[0]->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = get_json_column(read_col);
    EXPECT_FALSE(_meta->json_meta().is_flat());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    for (size_t i = 0; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i]->debug_item(0), read_col->debug_item(i));
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

    ColumnPtr read_col = jsons[0]->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = get_json_column(read_col);
    EXPECT_FALSE(_meta->json_meta().is_flat());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    for (size_t i = 0; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i]->debug_item(0), read_col->debug_item(i));
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

    ColumnPtr read_col = jsons[0]->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, jsons, read_col);

    EXPECT_FALSE(_meta->json_meta().is_flat());
    auto* read_json = get_json_column(read_col);
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    EXPECT_EQ(R"({"a": 1, "b": 21})", read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 2, "b": 22})", read_col->debug_item(1));
    EXPECT_EQ(R"({"a": 3, "b": 23})", read_col->debug_item(2));
    EXPECT_EQ(R"({"a": 4, "b": 24})", read_col->debug_item(3));
    EXPECT_EQ(R"({"a": 5, "b": 25})", read_col->debug_item(4));
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

    ColumnPtr read_col = jsons[0]->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = get_json_column(read_col);
    EXPECT_FALSE(_meta->json_meta().is_flat());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());

    EXPECT_EQ(R"({"a": 1, "b": 21})", read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 2, "b": 22})", read_col->debug_item(1));
    EXPECT_EQ(R"({"a": 3, "b": 23, "c": 33})", read_col->debug_item(2));
    EXPECT_EQ(R"({"a": 4, "b": 24, "c": 34})", read_col->debug_item(3));
    EXPECT_EQ(R"({"a": 5, "b": 25, "c": 35})", read_col->debug_item(4));
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

    ColumnPtr read_col = jsons[0]->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = get_json_column(read_col);
    EXPECT_FALSE(_meta->json_meta().is_flat());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());

    EXPECT_EQ(R"({"a": 1, "b": 21, "g": {}})", read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 2, "b": 22, "k": "abc"})", read_col->debug_item(1));
    EXPECT_EQ(R"({"a": 3, "b": 23, "c": 33})", read_col->debug_item(2));
    EXPECT_EQ(R"({"a": 4, "b": 24, "d": 34})", read_col->debug_item(3));
    EXPECT_EQ(R"({"a": 5, "b": 25, "e": 35})", read_col->debug_item(4));
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

    ColumnPtr read_col = jsons[0]->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = get_json_column(read_col);
    EXPECT_FALSE(_meta->json_meta().is_flat());

    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());

    EXPECT_EQ(R"({"a": 1, "b": 21, "b1": {"b2": 1, "b3": {"b4": "ab1", "b5": [1, 2, 3]}}, "g": {}})",
              read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 2, "b": 22, "b1": {"b2": 2, "b3": {"b4": "ab2", "b5": {}}}, "k": "abc"})",
              read_col->debug_item(1));
    EXPECT_EQ(R"({"a": 3, "b": 23, "b1": {"b2": 3, "b3": {"b4": "ab3", "b5": "a"}}, "c": 33})",
              read_col->debug_item(2));
    EXPECT_EQ(R"({"a": 4, "b": 24, "b1": {"b2": 4, "b3": {"b4": "ab4", "b5": 1}}, "d": 34})", read_col->debug_item(3));
    EXPECT_EQ(R"({"a": 5, "b": 25, "b1": {"b2": 5, "b3": {"b4": "ab5", "b5": false}}, "e": 35})",
              read_col->debug_item(4));
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

    ColumnPtr read_col = jsons[0]->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = get_json_column(read_col);
    EXPECT_FALSE(_meta->json_meta().is_flat());

    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());

    EXPECT_EQ(R"({"a": 1, "b": 21, "b1": {"b2": 1, "b3": {"b4": "ab1", "b5": [1, 2, 3]}}, "g": {}})",
              read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 2, "b": 22, "b1": {"b2": 2, "b3": {"b4": "ab2", "b5": {}}}, "k": "abc"})",
              read_col->debug_item(1));
    for (size_t i = 2; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i]->debug_item(0), read_col->debug_item(i));
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

    ColumnPtr read_col = jsons[0]->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = get_json_column(read_col);
    EXPECT_FALSE(_meta->json_meta().is_flat());

    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    EXPECT_EQ(R"({"a": 1, "b": 21})", read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 2, "b": 22})", read_col->debug_item(1));
    EXPECT_EQ(R"({"a": 3, "b": 23})", read_col->debug_item(2));
    EXPECT_EQ(R"({"a": 4, "b": 24})", read_col->debug_item(3));
    EXPECT_EQ(R"(NULL)", read_col->debug_item(4));
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

    ColumnPtr read_col = jsons[0]->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = get_json_column(read_col);
    EXPECT_FALSE(_meta->json_meta().is_flat());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());

    EXPECT_EQ(R"({"a": 1, "b": 21})", read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 2, "b": 22})", read_col->debug_item(1));
    EXPECT_EQ(R"({"a": 3, "b": 23, "c": 33})", read_col->debug_item(2));
    EXPECT_EQ(R"({"a": 4, "b": 24, "c": 34})", read_col->debug_item(3));
    EXPECT_EQ(R"(NULL)", read_col->debug_item(4));
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

    ColumnPtr read_col = jsons[0]->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = get_json_column(read_col);
    EXPECT_FALSE(_meta->json_meta().is_flat());
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    EXPECT_EQ(R"({"a": 1, "b": 21, "g": {}})", read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 2, "b": 22, "k": "abc"})", read_col->debug_item(1));
    EXPECT_EQ(R"({"a": 3, "b": 23, "c": 33})", read_col->debug_item(2));
    EXPECT_EQ(R"({"a": 4, "b": 24, "d": 34})", read_col->debug_item(3));
    EXPECT_EQ(R"(NULL)", read_col->debug_item(4));
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

    ColumnPtr read_col = jsons[0]->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, jsons, read_col);

    auto* read_json = get_json_column(read_col);
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());

    EXPECT_EQ(R"({"a": 1, "b": 21, "b1": {"b2": 1, "b3": {"b4": "ab1", "b5": [1, 2, 3]}}, "g": {}})",
              read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 2, "b": 22, "b1": {"b2": 2, "b3": {"b4": "ab2", "b5": {}}}, "k": "abc"})",
              read_col->debug_item(1));
    EXPECT_EQ(R"({"a": 3, "b": 23, "b1": {"b2": 3, "b3": {"b4": "ab3", "b5": "a"}}, "c": 33})",
              read_col->debug_item(2));
    EXPECT_EQ(R"({"a": 4, "b": 24, "b1": {"b2": 4, "b3": {"b4": "ab4", "b5": 1}}, "d": 34})", read_col->debug_item(3));
    EXPECT_EQ(R"(NULL)", read_col->debug_item(4));
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

    ColumnPtr read_col = jsons[0]->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = false;
    test_json(writer_opts, jsons, read_col);
    EXPECT_FALSE(_meta->json_meta().is_flat());

    auto* read_json = get_json_column(read_col);
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());

    EXPECT_EQ(R"({"a": 1, "b": 21, "b1": {"b2": 1, "b3": {"b4": "ab1", "b5": [1, 2, 3]}}, "g": {}})",
              read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 2, "b": 22, "b1": {"b2": 2, "b3": {"b4": "ab2", "b5": {}}}, "k": "abc"})",
              read_col->debug_item(1));
    for (size_t i = 2; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i]->debug_item(0), read_col->debug_item(i));
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

    ColumnPtr read_col = jsons[0]->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, jsons, read_col);
    EXPECT_TRUE(_meta->json_meta().is_flat());
    EXPECT_FALSE(_meta->json_meta().has_remain());
    EXPECT_EQ(2, _meta->children_columns_size());

    auto* read_json = get_json_column(read_col);
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    EXPECT_EQ(R"({"a": 1, "b": 21})", read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 2, "b": 22})", read_col->debug_item(1));
    EXPECT_EQ(R"({"a": 3, "b": 23})", read_col->debug_item(2));
    EXPECT_EQ(R"({"a": 4, "b": 24})", read_col->debug_item(3));
    EXPECT_EQ(R"({"a": 5, "b": 25})", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnCompactTest, testNullJsonCompactToFlatJson) {
    config::json_flat_null_factor = 0.1;
    // clang-format off
    Columns jsons = {
            normal_json(R"({"a": 1, "b": 21})", true),
            normal_json(R"({"a": 2, "b": 22})", true),
            normal_json(R"({"a": 3, "b": 23})", true),
            normal_json(R"({"a": 4, "b": 24})", true),
            normal_json(R"(NULL)", true)
    };
    // clang-format on

    ColumnPtr read_col = jsons[0]->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, jsons, read_col);
    EXPECT_FALSE(_meta->json_meta().is_flat());
    EXPECT_FALSE(_meta->json_meta().has_remain());

    auto* read_json = get_json_column(read_col);
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    EXPECT_EQ(R"({"a": 1, "b": 21})", read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 2, "b": 22})", read_col->debug_item(1));
    EXPECT_EQ(R"({"a": 3, "b": 23})", read_col->debug_item(2));
    EXPECT_EQ(R"({"a": 4, "b": 24})", read_col->debug_item(3));
    EXPECT_EQ(R"(NULL)", read_col->debug_item(4));
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

    ColumnPtr read_col = jsons[0]->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, jsons, read_col);
    EXPECT_TRUE(_meta->json_meta().is_flat());
    EXPECT_FALSE(_meta->json_meta().has_remain());
    EXPECT_EQ(2, _meta->children_columns_size());

    auto* read_json = get_json_column(read_col);
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    EXPECT_EQ(R"({"a": 1, "b": 21})", read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 2, "b": 22})", read_col->debug_item(1));
    EXPECT_EQ(R"({"a": 3, "b": 23})", read_col->debug_item(2));
    EXPECT_EQ(R"({"a": 4, "b": 24})", read_col->debug_item(3));
    EXPECT_EQ(R"({"a": 5, "b": 25})", read_col->debug_item(4));
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

    ColumnPtr read_col = jsons[0]->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, jsons, read_col);
    EXPECT_TRUE(_meta->json_meta().is_flat());
    EXPECT_TRUE(_meta->json_meta().has_remain());
    EXPECT_EQ(3, _meta->children_columns_size());

    auto* read_json = get_json_column(read_col);
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    EXPECT_EQ(R"({"a": 1, "b": 21})", read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 2, "b": 22})", read_col->debug_item(1));
    EXPECT_EQ(R"({"a": 3, "b": 23, "c": 33})", read_col->debug_item(2));
    EXPECT_EQ(R"({"a": 4, "b": 24, "c": 34})", read_col->debug_item(3));
    EXPECT_EQ(R"({"a": 5, "b": 25, "c": 35})", read_col->debug_item(4));
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

    ColumnPtr read_col = jsons[0]->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, jsons, read_col);
    EXPECT_TRUE(_meta->json_meta().is_flat());
    EXPECT_TRUE(_meta->json_meta().has_remain());
    EXPECT_EQ(3, _meta->children_columns_size());

    auto* read_json = get_json_column(read_col);
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    EXPECT_EQ(R"({"a": 1, "b": 21, "g": {}})", read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 2, "b": 22, "k": "abc"})", read_col->debug_item(1));
    EXPECT_EQ(R"({"a": 3, "b": 23, "c": 33})", read_col->debug_item(2));
    EXPECT_EQ(R"({"a": 4, "b": 24, "d": 34})", read_col->debug_item(3));
    EXPECT_EQ(R"({"a": 5, "b": 25, "e": 35})", read_col->debug_item(4));
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

    ColumnPtr read_col = jsons[0]->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, jsons, read_col);
    EXPECT_TRUE(_meta->json_meta().is_flat());
    EXPECT_TRUE(_meta->json_meta().has_remain());
    EXPECT_EQ(6, _meta->children_columns_size());
    EXPECT_EQ("b1.b2", _meta->children_columns(2).name());
    EXPECT_EQ("b1.b3.b4", _meta->children_columns(3).name());
    EXPECT_EQ("b1.b3.b5", _meta->children_columns(4).name());
    EXPECT_EQ("remain", _meta->children_columns(5).name());

    auto* read_json = get_json_column(read_col);
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    EXPECT_EQ(R"({"a": 1, "b": 21, "b1": {"b2": 1, "b3": {"b4": "ab1", "b5": [1, 2, 3]}}, "g": {}})",
              read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 2, "b": 22, "b1": {"b2": 2, "b3": {"b4": "ab2", "b5": {}}}, "k": "abc"})",
              read_col->debug_item(1));
    EXPECT_EQ(R"({"a": 3, "b": 23, "b1": {"b2": 3, "b3": {"b4": "ab3", "b5": "a"}}, "c": 33})",
              read_col->debug_item(2));
    EXPECT_EQ(R"({"a": 4, "b": 24, "b1": {"b2": 4, "b3": {"b4": "ab4", "b5": 1}}, "d": 34})", read_col->debug_item(3));
    EXPECT_EQ(R"({"a": 5, "b": 25, "b1": {"b2": 5, "b3": {"b4": "ab5", "b5": false}}, "e": 35})",
              read_col->debug_item(4));
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

    ColumnPtr read_col = jsons[0]->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, jsons, read_col);
    EXPECT_TRUE(_meta->json_meta().is_flat());
    EXPECT_TRUE(_meta->json_meta().has_remain());
    EXPECT_EQ(6, _meta->children_columns_size());
    EXPECT_EQ("b1.b2", _meta->children_columns(2).name());
    EXPECT_EQ("b1.b3.b4", _meta->children_columns(3).name());
    EXPECT_EQ("b1.b3.b5", _meta->children_columns(4).name());
    EXPECT_EQ("remain", _meta->children_columns(5).name());

    auto* read_json = get_json_column(read_col);
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    EXPECT_EQ(R"({"a": 1, "b": 21, "b1": {"b2": 1, "b3": {"b4": "ab1", "b5": [1, 2, 3]}}, "g": {}})",
              read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 2, "b": 22, "b1": {"b2": 2, "b3": {"b4": "ab2", "b5": {}}}, "k": "abc"})",
              read_col->debug_item(1));
    for (size_t i = 2; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i]->debug_item(0), read_col->debug_item(i));
    }
}

TEST_F(FlatJsonColumnCompactTest, testNullFlatJsonCompactToFlatJson) {
    config::json_flat_null_factor = 1;
    config::json_flat_sparsity_factor = 0.7;
    // clang-format off
    Columns jsons = {
            flat_json(R"({"a": 1, "b": 21})", true),
            flat_json(R"({"a": 2, "b": 22})", true),
            flat_json(R"({"a": 3, "b": 23})", true),
            flat_json(R"({"a": 4, "b": 24})", true),
            flat_json(R"(NULL)", true),
    };
    // clang-format on

    ColumnPtr read_col = jsons[0]->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, jsons, read_col);
    EXPECT_TRUE(_meta->json_meta().is_flat());
    EXPECT_FALSE(_meta->json_meta().has_remain());
    EXPECT_EQ(3, _meta->children_columns_size());

    auto* read_json = get_json_column(read_col);
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    EXPECT_EQ(R"({"a": 1, "b": 21})", read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 2, "b": 22})", read_col->debug_item(1));
    EXPECT_EQ(R"({"a": 3, "b": 23})", read_col->debug_item(2));
    EXPECT_EQ(R"({"a": 4, "b": 24})", read_col->debug_item(3));
    EXPECT_EQ(R"(NULL)", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnCompactTest, testNullFlatJsonCompactToFlatJson2) {
    config::json_flat_null_factor = 1;
    config::json_flat_sparsity_factor = 0.7;
    // clang-format off
    Columns jsons = {
            flat_json(R"({"a": 1, "b": 21})", true),
            flat_json(R"({"a": 2, "b": 22})", true),
            flat_json(R"({"a": 3, "b": 23, "c": 33})", true),
            flat_json(R"({"a": 4, "b": 24, "c": 34})", true),
            flat_json(R"(NULL)", true),
    };
    // clang-format on

    ColumnPtr read_col = jsons[0]->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, jsons, read_col);
    EXPECT_TRUE(_meta->json_meta().is_flat());
    EXPECT_TRUE(_meta->json_meta().has_remain());
    EXPECT_EQ(4, _meta->children_columns_size());

    auto* read_json = get_json_column(read_col);
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    EXPECT_EQ(R"({"a": 1, "b": 21})", read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 2, "b": 22})", read_col->debug_item(1));
    EXPECT_EQ(R"({"a": 3, "b": 23, "c": 33})", read_col->debug_item(2));
    EXPECT_EQ(R"({"a": 4, "b": 24, "c": 34})", read_col->debug_item(3));
    EXPECT_EQ(R"(NULL)", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnCompactTest, testNullFlatJsonCompactToFlatJson3) {
    config::json_flat_null_factor = 1;
    config::json_flat_sparsity_factor = 0.7;
    // clang-format off
    Columns jsons = {
            flat_json(R"({"a": 1, "b": 21, "g": {}})", true),
            flat_json(R"({"a": 2, "b": 22, "k": "abc"})", true),
            flat_json(R"({"a": 3, "b": 23, "c": 33})", true),
            flat_json(R"({"a": 4, "b": 24, "d": 34})", true),
            flat_json(R"(NULL)", true),
    };
    // clang-format on

    ColumnPtr read_col = jsons[0]->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, jsons, read_col);

    EXPECT_TRUE(_meta->json_meta().is_flat());
    EXPECT_TRUE(_meta->json_meta().has_remain());
    EXPECT_EQ(4, _meta->children_columns_size());

    auto* read_json = get_json_column(read_col);
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    EXPECT_EQ(R"({"a": 1, "b": 21, "g": {}})", read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 2, "b": 22, "k": "abc"})", read_col->debug_item(1));
    EXPECT_EQ(R"({"a": 3, "b": 23, "c": 33})", read_col->debug_item(2));
    EXPECT_EQ(R"({"a": 4, "b": 24, "d": 34})", read_col->debug_item(3));
    EXPECT_EQ(R"(NULL)", read_col->debug_item(4));
}

TEST_F(FlatJsonColumnCompactTest, testNullFlatJsonCompactToFlatJson4) {
    config::json_flat_null_factor = 1;
    config::json_flat_sparsity_factor = 0.7;
    // clang-format off
    Columns jsons = {
            flat_json(R"({"a": 1, "b": 21, "b1": {"b2": 1, "b3": {"b4": "ab1", "b5": [1, 2, 3]}}, "g": {}})", true),
            flat_json(R"({"a": 2, "b": 22, "b1": {"b2": 2, "b3": {"b4": "ab2", "b5": {}}}, "k": "abc"})", true),
            flat_json(R"({"a": 3, "b": 23, "b1": {"b2": 3, "b3": {"b4": "ab3", "b5": "a"}}, "c": 33})", true),
            flat_json(R"({"a": 4, "b": 24, "b1": {"b2": 4, "b3": {"b4": "ab4", "b5": 1}}, "d": 34})", true),
            flat_json(R"(NULL)", true),
    };
    // clang-format on

    ColumnPtr read_col = jsons[0]->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, jsons, read_col);
    EXPECT_TRUE(_meta->json_meta().is_flat());
    EXPECT_TRUE(_meta->json_meta().has_remain());
    EXPECT_EQ(7, _meta->children_columns_size());
    EXPECT_EQ("nulls", _meta->children_columns(0).name());
    EXPECT_EQ("b1.b2", _meta->children_columns(3).name());
    EXPECT_EQ("b1.b3.b4", _meta->children_columns(4).name());
    EXPECT_EQ("b1.b3.b5", _meta->children_columns(5).name());
    EXPECT_EQ("remain", _meta->children_columns(6).name());

    auto* read_json = get_json_column(read_col);
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    EXPECT_EQ(R"({"a": 1, "b": 21, "b1": {"b2": 1, "b3": {"b4": "ab1", "b5": [1, 2, 3]}}, "g": {}})",
              read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 2, "b": 22, "b1": {"b2": 2, "b3": {"b4": "ab2", "b5": {}}}, "k": "abc"})",
              read_col->debug_item(1));
    EXPECT_EQ(R"({"a": 3, "b": 23, "b1": {"b2": 3, "b3": {"b4": "ab3", "b5": "a"}}, "c": 33})",
              read_col->debug_item(2));
    EXPECT_EQ(R"({"a": 4, "b": 24, "b1": {"b2": 4, "b3": {"b4": "ab4", "b5": 1}}, "d": 34})", read_col->debug_item(3));
    EXPECT_EQ(R"(NULL)", read_col->debug_item(4));
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

    ColumnPtr read_col = jsons[0]->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, jsons, read_col);
    EXPECT_TRUE(_meta->json_meta().is_flat());
    EXPECT_TRUE(_meta->json_meta().has_remain());
    EXPECT_EQ(7, _meta->children_columns_size());
    EXPECT_EQ("nulls", _meta->children_columns(0).name());
    EXPECT_EQ("b1.b2", _meta->children_columns(3).name());
    EXPECT_EQ("b1.b3.b4", _meta->children_columns(4).name());
    EXPECT_EQ("b1.b3.b5", _meta->children_columns(5).name());
    EXPECT_EQ("remain", _meta->children_columns(6).name());

    auto* read_json = get_json_column(read_col);
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    EXPECT_EQ(R"({"a": 1, "b": 21, "b1": {"b2": 1, "b3": {"b4": "ab1", "b5": [1, 2, 3]}}, "g": {}})",
              read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 2, "b": 22, "b1": {"b2": 2, "b3": {"b4": "ab2", "b5": {}}}, "k": "abc"})",
              read_col->debug_item(1));
    for (size_t i = 2; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i]->debug_item(0), read_col->debug_item(i));
    }
}

TEST_F(FlatJsonColumnCompactTest, testNullHyperJsonCompactToFlatJson2) {
    // clang-format off
    Columns jsons = {
            flat_json(R"({"a": 1,            "b1": {"b2": 1, "b3": {"b4": "ab1", "b5": [1, 2, 3]}}, "g": {}})", true),
            flat_json(R"({"a": 2,            "b1": {"b2": 2, "b3": {"b4": "ab2", "b5": {}}}, "k": "abc"})", true),
            normal_json(R"({"a": 3,          "b1": {"b2": 3, "b3": {"b4": "ab3", "b5": "a"}}, "c": 33})", true),
            normal_json(R"({"a": 4,          "b1": {"b2": 4, "b3": {"b4": "ab4", "b5": 1}}, "d": 34})", true),
            normal_json(R"({"a": 5, "b": 25, "b1": {"b2": 5, "b3": {"b4": "ab5", "b5": false}}, "e": 35})", true),
    };
    // clang-format on

    ColumnPtr read_col = jsons[0]->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, jsons, read_col);
    EXPECT_TRUE(_meta->json_meta().is_flat());
    EXPECT_TRUE(_meta->json_meta().has_remain());
    EXPECT_EQ(6, _meta->children_columns_size());
    EXPECT_EQ("nulls", _meta->children_columns(0).name());
    EXPECT_EQ("b1.b2", _meta->children_columns(2).name());
    EXPECT_EQ("b1.b3.b4", _meta->children_columns(3).name());
    EXPECT_EQ("b1.b3.b5", _meta->children_columns(4).name());
    EXPECT_EQ("remain", _meta->children_columns(5).name());

    auto* read_json = get_json_column(read_col);
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    EXPECT_EQ(R"({"a": 1, "b1": {"b2": 1, "b3": {"b4": "ab1", "b5": [1, 2, 3]}}, "g": {}})", read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 2, "b1": {"b2": 2, "b3": {"b4": "ab2", "b5": {}}}, "k": "abc"})", read_col->debug_item(1));
    for (size_t i = 2; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i]->debug_item(0), read_col->debug_item(i));
    }
}

TEST_F(FlatJsonColumnCompactTest, testNullHyperJsonCompactToFlatJson3) {
    // clang-format off
    Columns jsons = {
            flat_json(R"({"a": 1,                   "b1": {"b2": 1, "b3": {"b4": "ab1", "b5": [1, 2, 3]}}, "g": {}})", true),
            flat_json(R"({"a": 2,                   "b1": {"b2": 2, "b3": {"b4": 123, "b5": {}}}, "k": "abc"})", true),
            normal_json(R"({"a": 3,                 "b1": {"b2": 3, "b3": {"b4": "ab3", "b5": "a"}}, "c": 33})", true),
            normal_json(R"({"a": 4, "b": [7, 8, 9], "b1": {"b2": 4, "b3": {"b4": 234, "b5": 1}}, "d": 34})", true),
            normal_json(R"({"a": 5, "b": 25,        "b1": {"b2": 5, "b3": {"b4": "ab5", "b5": false}}, "e": 35})", true),
    };
    // clang-format on

    ColumnPtr read_col = jsons[0]->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, jsons, read_col);
    EXPECT_TRUE(_meta->json_meta().is_flat());
    EXPECT_TRUE(_meta->json_meta().has_remain());
    EXPECT_EQ(6, _meta->children_columns_size());
    EXPECT_EQ("b1.b2", _meta->children_columns(2).name());
    EXPECT_EQ("b1.b3.b4", _meta->children_columns(3).name());
    EXPECT_EQ("b1.b3.b5", _meta->children_columns(4).name());
    EXPECT_EQ("remain", _meta->children_columns(5).name());

    auto* read_json = get_json_column(read_col);
    EXPECT_FALSE(read_json->is_flat_json());
    EXPECT_EQ(5, read_json->size());
    EXPECT_EQ(0, read_json->get_flat_fields().size());
    EXPECT_EQ(R"({"a": 1, "b1": {"b2": 1, "b3": {"b4": "ab1", "b5": [1, 2, 3]}}, "g": {}})", read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 2, "b1": {"b2": 2, "b3": {"b4": 123, "b5": {}}}, "k": "abc"})", read_col->debug_item(1));
    for (size_t i = 2; i < jsons.size(); i++) {
        EXPECT_EQ(jsons[i]->debug_item(0), read_col->debug_item(i));
    }
}

TEST_F(FlatJsonColumnCompactTest, testNullHyperJsonCompactToFlatJson4) {
    // clang-format off
    Columns jsons = {
            flat_json(R"({"a": 1,                   "b1": {"b2": 1, "b3": {"b4": "ab1", "b5": [1, 2, 3]}}, "g": {}})", true),
            flat_json(R"({"a": 2,                   "b1": {"b2": 2, "b3": {"b4": 123, "b5": {}}}, "k": "abc"})", true),
            normal_json(R"({"a": 3,                 "b1": {"b2": 3, "b3": "abc"}, "c": 33})", true),
            normal_json(R"({"a": 4, "b": [6, 5, 4], "b1": {"b2": 4, "b3": 123}, "d": 34})", true),
            normal_json(R"({"a": 5, "b": 25,        "b1": {"b2": 5, "b3": {"b4": "ab5", "b5": false}}, "e": 35})", true),
    };
    // clang-format on
    JsonPathDeriver deriver;
    test_compact_path(jsons, &deriver);

    EXPECT_EQ(2, deriver.flat_paths().size());
    EXPECT_EQ(R"([a(BIGINT), b1.b2(BIGINT)])",
              JsonFlatPath::debug_flat_json(deriver.flat_paths(), deriver.flat_types(), deriver.has_remain_json()));
}

TEST_F(FlatJsonColumnCompactTest, testHyperJsonCompactToFlatJsonRemain) {
    config::json_flat_sparsity_factor = 0.6;
    // clang-format off
    Columns jsons = {
        more_flat_json({
            R"({"a": 11, "b": "abc1", "c": 11})",
            R"({"a": 12, "b": "abc2", "f": 12})",
            R"({"a": 13, "b": "abc3", "e": 13})",
        }, true),
        more_flat_json({
            R"({"a": 21, "b": "efg1", "c": 21})",
            R"({"a": 22, "b": "efg2", "c": 22})",
            R"({"a": 23, "b": "efg3", "c": 23})",
            R"({"a": 24, "b": "efg4", "c": 24})",
            R"({"a": 25, "b": "efg5", "c": 25})",
            R"({"a": 26, "b": "efg6", "c": 26})",
            R"({"a": 27, "b": "efg7", "c": 27})",
            R"({"a": 28, "b": "efg8", "c": 28})",
            R"({"a": 29, "b": "efg9", "c": 29})",
            R"({"a": 20, "b": "qwe1", "c": 20})",
            R"({"a": 31, "b": "qwe2", "c": 30})",
            R"({"a": 32, "b": "qwe3", "c": 31})",
            R"({"a": 33, "b": "qwe4", "c": 32})",
        }, true),
        more_flat_json({
            R"({"a": 31, "b": "xwy1", "x": 31})",
            R"({"a": 32, "b": "xwy2", "x": 32})",
            R"({"a": 33, "b": "xwy3", "x": 33})",
            R"({"a": 34, "b": "xwy4", "x": 34})",
            R"({"a": 35, "b": "xwy5", "x": 35})",
        }, true),
    };
    // clang-format on

    ColumnPtr read_col = jsons[0]->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, jsons, read_col);

    EXPECT_TRUE(_meta->json_meta().is_flat());
    EXPECT_TRUE(_meta->json_meta().has_remain());
    EXPECT_EQ(5, _meta->children_columns_size());
    EXPECT_EQ("nulls", _meta->children_columns(0).name());
    EXPECT_EQ("a", _meta->children_columns(1).name());
    EXPECT_EQ("c", _meta->children_columns(3).name());
    EXPECT_EQ("remain", _meta->children_columns(4).name());

    EXPECT_EQ(R"({"a": 11, "b": "abc1", "c": 11})", read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 12, "b": "abc2", "f": 12})", read_col->debug_item(1));
    EXPECT_EQ(R"({"a": 25, "b": "efg5", "c": 25})", read_col->debug_item(7));
    EXPECT_EQ(R"({"a": 33, "b": "qwe4", "c": 32})", read_col->debug_item(15));
    EXPECT_EQ(R"({"a": 33, "b": "xwy3", "x": 33})", read_col->debug_item(18));
}

TEST_F(FlatJsonColumnCompactTest, testHyperJsonCompactToFlatJsonRemain3) {
    config::json_flat_sparsity_factor = 0.6;
    // clang-format off
    Columns jsons = {
        more_flat_json({
            R"({"a": 11, "b": "abc1", "c": {"d": 21, "e": 211}})",
            R"({"a": 12, "b": "abc2", "c": {"d": 22, "e": 221}})",
            R"({"a": 13, "b": "abc3", "c": {"d": 23, "e": 231}})",
        }, true),
        more_flat_json({
             R"({"a": 21, "b": "efg1", "c": "c21"})",
             R"({"a": 22, "b": "efg2", "c": "c22"})",
             R"({"a": 23, "b": "efg3", "c": "c23"})",
             R"({"a": 24, "b": "efg4", "c": "c24"})",
             R"({"a": 25, "b": "efg5", "c": "c25"})",
             R"({"a": 26, "b": "efg6", "c": "c26"})",
             R"({"a": 27, "b": "efg7", "c": "c27"})",
             R"({"a": 28, "b": "efg8", "c": "c28"})",
             R"({"a": 29, "b": "efg9", "c": "c29"})",
             R"({"a": 20, "b": "qwe1", "c": "c20"})",
            R"({"a": 31, "b": "qwe2", "c": "c30"})",
            R"({"a": 32, "b": "qwe3", "c": "c31"})",
            R"({"a": 33, "b": "qwe4", "c": "c32"})",
        }, true),
        more_flat_json({
            R"({"a": 31, "b": "xwy1", "c": "d31"})",
            R"({"a": 32, "b": "xwy2", "c": "d32"})",
            R"({"a": 33, "b": "xwy3", "c": "d33"})",
            R"({"a": 34, "b": "xwy4", "c": "d34"})",
            R"({"a": 35, "b": "xwy5", "c": "d35"})",
        }, true),
    };
    // clang-format on
    JsonPathDeriver deriver;
    test_compact_path(jsons, &deriver);

    EXPECT_EQ(2, deriver.flat_paths().size());
    EXPECT_EQ(R"([a(BIGINT), b(VARCHAR)])",
              JsonFlatPath::debug_flat_json(deriver.flat_paths(), deriver.flat_types(), deriver.has_remain_json()));
}

TEST_F(FlatJsonColumnCompactTest, testHyperJsonCompactToFlatJsonRemainRead) {
    config::json_flat_sparsity_factor = 0.6;
    // clang-format off
    Columns jsons = {
        more_flat_json({
            R"({"a": 11, "b": "abc1", "c": 11})",
            R"({"a": 12, "b": "abc2", "f": 12})",
            R"({"a": 13, "b": "abc3", "e": 13})",
        }, true),
        more_flat_json({
            R"({"a": 21, "b": "efg1", "c": 21})",
            R"({"a": 22, "b": "efg2", "c": 22})",
            R"({"a": 23, "b": "efg3", "c": 23})",
            R"({"a": 24, "b": "efg4", "c": 24})",
            R"({"a": 25, "b": "efg5", "c": 25})",
            R"({"a": 26, "b": "efg6", "c": 26})",
            R"({"a": 27, "b": "efg7", "c": 27})",
            R"({"a": 28, "b": "efg8", "c": 28})",
            R"({"a": 29, "b": "efg9", "c": 29})",
            R"({"a": 20, "b": "qwe1", "c": 20})",
            R"({"a": 31, "b": "qwe2", "c": 30})",
            R"({"a": 32, "b": "qwe3", "c": 31})",
            R"({"a": 33, "b": "qwe4", "c": 32})",
        }, true),
        more_flat_json({
            R"({"a": 31, "b": "xwy1", "x": 31})",
            R"({"a": 32, "b": "xwy2", "x": 32})",
            R"({"a": 33, "b": "xwy3", "x": 33})",
            R"({"a": 34, "b": "xwy4", "x": 34})",
            R"({"a": 35, "b": "xwy5", "x": 35})",
        }, true),
    };
    // clang-format on

    ASSIGN_OR_ABORT(auto root, ColumnAccessPath::create(TAccessPathType::FIELD, "root", 0));
    ColumnAccessPath::insert_json_path(root.get(), LogicalType::TYPE_BIGINT, "c");

    ColumnPtr read_col = jsons[0]->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, jsons, read_col, root.get());

    EXPECT_EQ(5, _meta->children_columns_size());
    EXPECT_EQ("nulls", _meta->children_columns(0).name());
    EXPECT_EQ("a", _meta->children_columns(1).name());
    EXPECT_EQ("c", _meta->children_columns(3).name());
    EXPECT_EQ("remain", _meta->children_columns(4).name());

    EXPECT_EQ(R"({c: 11})", read_col->debug_item(0));
    EXPECT_EQ(R"({c: NULL})", read_col->debug_item(1));
    EXPECT_EQ(R"({c: 25})", read_col->debug_item(7));
    EXPECT_EQ(R"({c: 32})", read_col->debug_item(15));
    EXPECT_EQ(R"({c: NULL})", read_col->debug_item(18));
}

TEST_F(FlatJsonColumnCompactTest, testHyperJsonCompactToFlatJsonRemainReadPaths) {
    config::json_flat_sparsity_factor = 0.6;
    // clang-format off
    Columns jsons = {
        more_flat_json({
            R"({"a": 11, "b": "abc1", "c": {"d": 21, "e": 211}})",
            R"({"a": 12, "b": "abc2", "c": {"d": 22, "e": 221}})",
            R"({"a": 13, "b": "abc3", "c": {"d": 23, "e": 231}})",
        }, true),
        more_flat_json({
             R"({"a": 21, "b": "efg1", "c": "c21"})",
             R"({"a": 22, "b": "efg2", "c": "c22"})",
             R"({"a": 23, "b": "efg3", "c": "c23"})",
             R"({"a": 24, "b": "efg4", "c": "c24"})",
             R"({"a": 25, "b": "efg5", "c": "c25"})",
             R"({"a": 26, "b": "efg6", "c": "c26"})",
             R"({"a": 27, "b": "efg7", "c": "c27"})",
             R"({"a": 28, "b": "efg8", "c": "c28"})",
             R"({"a": 29, "b": "efg9", "c": "c29"})",
             R"({"a": 20, "b": "qwe1", "c": "c20"})",
            R"({"a": 31, "b": "qwe2", "c": "c30"})",
            R"({"a": 32, "b": "qwe3", "c": "c31"})",
            R"({"a": 33, "b": "qwe4", "c": "c32"})",
        }, true),
        more_flat_json({
            R"({"a": 31, "b": "xwy1", "c": "d31"})",
            R"({"a": 32, "b": "xwy2", "c": "d32"})",
            R"({"a": 33, "b": "xwy3", "c": "d33"})",
            R"({"a": 34, "b": "xwy4", "c": "d34"})",
            R"({"a": 35, "b": "xwy5", "c": "d35"})",
        }, true),
    };
    // clang-format on
    JsonPathDeriver deriver;
    test_compact_path(jsons, &deriver);
    EXPECT_EQ(2, deriver.flat_paths().size());
    EXPECT_EQ(R"([a(BIGINT), b(VARCHAR)])",
              JsonFlatPath::debug_flat_json(deriver.flat_paths(), deriver.flat_types(), deriver.has_remain_json()));
}

TEST_F(FlatJsonColumnCompactTest, testHyperJsonCompactPaths) {
    config::json_flat_sparsity_factor = 0.6;
    // clang-format off
    Columns jsons = {
        more_flat_json({
            R"({"a": 11, "b": "abc1", "c": 11})",
            R"({"a": 12, "b": "abc2", "f": 12})",
            R"({"a": 13, "b": "abc3", "e": 13})",
        }, true),
        more_flat_json({
            R"({"a": 21, "b": "efg1", "c": 21})",
            R"({"a": 22, "b": "efg2", "c": 22})",
            R"({"a": 23, "b": "efg3", "c": 23})",
            R"({"a": 24, "b": "efg4", "c": 24})",
            R"({"a": 25, "b": "efg5", "c": 25})",
            R"({"a": 26, "b": "efg6", "c": 26})",
            R"({"a": 27, "b": "efg7", "c": 27})",
            R"({"a": 28, "b": "efg8", "c": 28})",
            R"({"a": 29, "b": "efg9", "c": 29})",
            R"({"a": 20, "b": "qwe1", "c": 20})",
            R"({"a": 31, "b": "qwe2", "c": 30})",
            R"({"a": 32, "b": "qwe3", "c": 31})",
            R"({"a": 33, "b": "qwe4", "c": 32})",
        }, true),
        more_flat_json({
            R"({"a": 31, "b": "xwy1", "x": 31})",
            R"({"a": 32, "b": "xwy2", "x": 32})",
            R"({"a": 33, "b": "xwy3", "x": 33})",
            R"({"a": 34, "b": "xwy4", "x": 34})",
            R"({"a": 35, "b": "xwy5", "x": 35})",
        }, true),
    };
    // clang-format on

    JsonPathDeriver deriver;
    test_compact_path(jsons, &deriver);

    EXPECT_EQ(2, deriver.flat_paths().size());
    EXPECT_EQ(R"([a(BIGINT), b(VARCHAR)])",
              JsonFlatPath::debug_flat_json(deriver.flat_paths(), deriver.flat_types(), deriver.has_remain_json()));
}

TEST_F(FlatJsonColumnCompactTest, testHyperJsonCompactPaths2) {
    config::json_flat_sparsity_factor = 0.6;
    // clang-format off
    Columns jsons = {
        more_flat_json({
            R"({"a": 11, "b": "abc1", "c": 11})",
            R"({"a": 12, "b": "abc2", "c": 12})",
            R"({"a": 13, "b": "abc3", "c": 13})",
        }, true),
        more_flat_json({
             R"({"a": 21, "b": "efg1", "c": {"d": 21, "e": 211}})",
             R"({"a": 22, "b": "efg2", "c": {"d": 22, "e": 221}})",
             R"({"a": 23, "b": "efg3", "c": {"d": 23, "e": 231}})",
             R"({"a": 24, "b": "efg4", "c": {"d": 24, "e": 241}})",
             R"({"a": 25, "b": "efg5", "c": {"d": 25, "e": 251}})",
             R"({"a": 26, "b": "efg6", "c": {"d": 26, "e": 261}})",
             R"({"a": 27, "b": "efg7", "c": {"d": 27, "e": 271}})",
             R"({"a": 28, "b": "efg8", "c": {"d": 28, "e": 281}})",
             R"({"a": 29, "b": "efg9", "c": {"d": 29, "e": 291}})",
             R"({"a": 20, "b": "qwe1", "c": {"d": 20, "e": 201}})",
            R"({"a": 31, "b": "qwe2", "c": {"d": 30, "e": 301}})",
            R"({"a": 32, "b": "qwe3", "c": {"d": 31, "e": 311}})",
            R"({"a": 33, "b": "qwe4", "c": {"d": 32, "e": 321}})",
        }, true),
        more_flat_json({
            R"({"a": 31, "b": "xwy1", "c": {"d": 31, "e": 311, "f": 312}})",
            R"({"a": 32, "b": "xwy2", "c": {"d": 32, "e": 321, "f": 322}})",
            R"({"a": 33, "b": "xwy3", "c": {"d": 33, "e": 331, "f": 332}})",
            R"({"a": 34, "b": "xwy4", "c": {"d": 34, "e": 341, "f": 342}})",
            R"({"a": 35, "b": "xwy5", "c": {"d": 35, "e": 351, "f": 352}})",
        }, true),
    };
    // clang-format on

    JsonPathDeriver deriver;
    test_compact_path(jsons, &deriver);

    EXPECT_EQ(2, deriver.flat_paths().size());
    EXPECT_EQ(R"([a(BIGINT), b(VARCHAR)])",
              JsonFlatPath::debug_flat_json(deriver.flat_paths(), deriver.flat_types(), deriver.has_remain_json()));
}

TEST_F(FlatJsonColumnCompactTest, testHyperJsonCompactPaths3) {
    config::json_flat_sparsity_factor = 0.6;
    // clang-format off
    Columns jsons = {
        more_flat_json({
            R"({"a": 11, "b": "abc1", "c": {"d": 21, "e": 211}})",
            R"({"a": 12, "b": "abc2", "c": {"d": 22, "e": 221}})",
            R"({"a": 13, "b": "abc3", "c": {"d": 23, "e": 231}})",
        }, true),
        more_flat_json({
             R"({"a": 21, "b": "efg1", "c": "c21"})",
             R"({"a": 22, "b": "efg2", "c": "c22"})",
             R"({"a": 23, "b": "efg3", "c": "c23"})",
             R"({"a": 24, "b": "efg4", "c": "c24"})",
             R"({"a": 25, "b": "efg5", "c": "c25"})",
             R"({"a": 26, "b": "efg6", "c": "c26"})",
             R"({"a": 27, "b": "efg7", "c": "c27"})",
             R"({"a": 28, "b": "efg8", "c": "c28"})",
             R"({"a": 29, "b": "efg9", "c": "c29"})",
             R"({"a": 20, "b": "qwe1", "c": "c20"})",
            R"({"a": 31, "b": "qwe2", "c": "c30"})",
            R"({"a": 32, "b": "qwe3", "c": "c31"})",
            R"({"a": 33, "b": "qwe4", "c": "c32"})",
        }, true),
        more_flat_json({
            R"({"a": 31, "b": "xwy1", "c": "d31"})",
            R"({"a": 32, "b": "xwy2", "c": "d32"})",
            R"({"a": 33, "b": "xwy3", "c": "d33"})",
            R"({"a": 34, "b": "xwy4", "c": "d34"})",
            R"({"a": 35, "b": "xwy5", "c": "d35"})",
        }, true),
    };
    // clang-format on

    JsonPathDeriver deriver;
    test_compact_path(jsons, &deriver);

    EXPECT_EQ(2, deriver.flat_paths().size());
    EXPECT_EQ(R"([a(BIGINT), b(VARCHAR)])",
              JsonFlatPath::debug_flat_json(deriver.flat_paths(), deriver.flat_types(), deriver.has_remain_json()));
}

TEST_F(FlatJsonColumnCompactTest, testHyperJsonCompactRemainLevel) {
    config::json_flat_sparsity_factor = 0.6;
    // clang-format off
    Columns jsons = {
        more_flat_json({
            R"({"a": 11, "b": "abc1", "c": {"d": 21, "e": 211}, "f4": {"m": 141, "n": 341}})",
            R"({"a": 12, "b": "abc2", "c": {"d": 22, "e": 221}, "f4": {"m": 142, "n": 342}})",
            R"({"a": 13, "b": "abc3", "c": {"d": 23, "e": 231}, "f4": {"m": 143, "n": 343}})",
        }, true),
        more_flat_json({
            R"({"a": 14, "b": "xwy4", "c": {"d": 24, "e": 241}, "g4": {"x": 143, "y": 434}})",
            R"({"a": 15, "b": "xwy5", "c": {"d": 25, "e": 251}, "g5": {"x": 153, "y": 435}})",
            R"({"a": 16, "b": "xwy6", "c": {"d": 26, "e": 261}, "g6": {"x": 163, "y": 436}})",
            R"({"a": 17, "b": "xwy7", "c": {"d": 27, "e": 271}, "g7": {"x": 173, "y": 437}})",
            R"({"a": 18, "b": "xwy8", "c": {"d": 28, "e": 281}, "g8": {"x": 183, "y": 438}})",
            R"({"a": 19, "b": "xwy9", "c": {"d": 29, "e": 291}, "g9": {"x": 193, "y": 439}})",
            R"({"a": 10, "b": "xwy0", "c": {"d": 20, "e": 201}, "g0": {"x": 103, "y": 430}})",
        }, true),
        more_flat_json({
             R"({"a": 20, "b": "qwe1", "c": {"d": 30, "e": 301}, "f4": {"m": 540, "n": 240}})",
             R"({"a": 21, "b": "efg1", "c": {"d": 31, "e": 311}, "f4": {"m": 541, "n": 241}})",
             R"({"a": 22, "b": "efg2", "c": {"d": 32, "e": 321}, "f4": {"m": 542, "n": 242}})",
             R"({"a": 23, "b": "efg3", "c": {"d": 33, "e": 331}, "f4": {"m": 543, "n": 243}})",
             R"({"a": 24, "b": "efg4", "c": {"d": 34, "e": 341}, "f4": {"m": 544, "n": 244}})",
             R"({"a": 25, "b": "efg5", "c": {"d": 35, "e": 351}, "f4": {"m": 545, "n": 245}})",
             R"({"a": 26, "b": "efg6", "c": {"d": 36, "e": 361}, "f4": {"m": 546, "n": 246}})",
             R"({"a": 27, "b": "efg7", "c": {"d": 37, "e": 371}, "f4": {"m": 547, "n": 247}})",
             R"({"a": 28, "b": "efg8", "c": {"d": 38, "e": 381}, "f4": {"m": 548, "n": 248}})",
             R"({"a": 29, "b": "efg9", "c": {"d": 39, "e": 391}, "f4": {"m": 549, "n": 249}})",
        }, true),
    };
    // clang-format on

    JsonPathDeriver deriver;
    test_compact_path(jsons, &deriver);

    EXPECT_EQ(4, deriver.flat_paths().size());
    EXPECT_EQ(R"([a(BIGINT), b(VARCHAR), c.d(BIGINT), c.e(BIGINT)])",
              JsonFlatPath::debug_flat_json(deriver.flat_paths(), deriver.flat_types(), deriver.has_remain_json()));

    ColumnPtr read_col = jsons[0]->clone_empty();
    ColumnWriterOptions writer_opts;
    writer_opts.need_flat = true;
    test_json(writer_opts, jsons, read_col, nullptr);

    EXPECT_EQ(R"({"a": 11, "b": "abc1", "c": {"d": 21, "e": 211}, "f4": {"m": 141, "n": 341}})",
              read_col->debug_item(0));
    EXPECT_EQ(R"({"a": 18, "b": "xwy8", "c": {"d": 28, "e": 281}, "g8": {"x": 183, "y": 438}})",
              read_col->debug_item(7));
    EXPECT_EQ(R"({"a": 28, "b": "efg8", "c": {"d": 38, "e": 381}, "f4": {"m": 548, "n": 248}})",
              read_col->debug_item(18));
}
} // namespace starrocks
