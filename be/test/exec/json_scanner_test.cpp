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

#include "exec/json_scanner.h"

#include <gtest/gtest.h>

#include <utility>

#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "gen_cpp/Descriptors_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"
#include "testutil/parallel_test.h"

namespace starrocks {

class JsonScannerTest : public ::testing::Test {
protected:
    std::unique_ptr<JsonScanner> create_json_scanner(const std::vector<TypeDescriptor>& types,
                                                     const std::vector<TBrokerRangeDesc>& ranges,
                                                     const std::vector<std::string>& col_names) {
        /// Init DescriptorTable
        TDescriptorTableBuilder desc_tbl_builder;
        TTupleDescriptorBuilder tuple_desc_builder;
        for (int i = 0; i < types.size(); ++i) {
            TSlotDescriptorBuilder slot_desc_builder;
            slot_desc_builder.type(types[i]).column_name(col_names[i]).length(types[i].len).nullable(true);
            tuple_desc_builder.add_slot(slot_desc_builder.build());
        }
        tuple_desc_builder.build(&desc_tbl_builder);

        DescriptorTbl* desc_tbl = nullptr;
        Status st = DescriptorTbl::create(_state, &_pool, desc_tbl_builder.desc_tbl(), &desc_tbl,
                                          config::vector_chunk_size);
        CHECK(st.ok()) << st.to_string();

        /// Init RuntimeState
        _state->set_desc_tbl(desc_tbl);
        _state->init_instance_mem_tracker();

        /// TBrokerScanRangeParams
        TBrokerScanRangeParams* params = _pool.add(new TBrokerScanRangeParams());
        params->strict_mode = true;
        params->dest_tuple_id = 0;
        params->src_tuple_id = 0;
        for (int i = 0; i < types.size(); i++) {
            params->expr_of_dest_slot[i] = TExpr();
            params->expr_of_dest_slot[i].nodes.emplace_back(TExprNode());
            params->expr_of_dest_slot[i].nodes[0].__set_type(types[i].to_thrift());
            params->expr_of_dest_slot[i].nodes[0].__set_node_type(TExprNodeType::SLOT_REF);
            params->expr_of_dest_slot[i].nodes[0].__set_is_nullable(true);
            params->expr_of_dest_slot[i].nodes[0].__set_slot_ref(TSlotRef());
            params->expr_of_dest_slot[i].nodes[0].slot_ref.__set_slot_id(i);
            params->expr_of_dest_slot[i].nodes[0].__set_type(types[i].to_thrift());
        }

        for (int i = 0; i < types.size(); i++) {
            params->src_slot_ids.emplace_back(i);
        }

        TBrokerScanRange* broker_scan_range = _pool.add(new TBrokerScanRange());
        broker_scan_range->params = *params;
        broker_scan_range->ranges = ranges;
        return std::make_unique<JsonScanner>(_state, _profile, *broker_scan_range, _counter);
    }

    ChunkPtr test_whole_row_json(int columns, const std::string& input_data, std::string jsonpath,
                                 const std::vector<std::string>& jsonpaths) {
        std::vector<TypeDescriptor> types;
        for (int i = 0; i < columns; i++) {
            types.emplace_back(TypeDescriptor::create_varchar_type(1024));
        }

        std::vector<TBrokerRangeDesc> ranges;
        TBrokerRangeDesc range;
        range.format_type = TFileFormatType::FORMAT_JSON;
        range.file_type = TFileType::FILE_LOCAL;
        range.strip_outer_array = false;
        range.__isset.strip_outer_array = false;
        range.__isset.jsonpaths = true;
        range.jsonpaths = std::move(jsonpath);
        range.__isset.json_root = false;
        range.__set_path(input_data);
        ranges.emplace_back(range);

        auto scanner = create_json_scanner(types, ranges, jsonpaths);

        EXPECT_OK(scanner->open());

        ChunkPtr chunk = scanner->get_next().value();
        return chunk;
    }

    ChunkPtr test_with_jsonpath(int columns, const std::string& input_data, std::string jsonpath,
                                const std::vector<std::string>& jsonpaths) {
        std::vector<TypeDescriptor> types;
        for (int i = 0; i < columns; i++) {
            types.emplace_back(TypeDescriptor::create_json_type());
        }

        std::vector<TBrokerRangeDesc> ranges;
        TBrokerRangeDesc range;
        range.format_type = TFileFormatType::FORMAT_JSON;
        range.file_type = TFileType::FILE_LOCAL;
        range.strip_outer_array = false;
        range.__isset.strip_outer_array = false;
        range.__isset.jsonpaths = true;
        range.jsonpaths = std::move(jsonpath);
        range.__isset.json_root = false;
        range.__set_path(input_data);
        ranges.emplace_back(range);

        auto scanner = create_json_scanner(types, ranges, jsonpaths);

        EXPECT_OK(scanner->open());

        ChunkPtr chunk = scanner->get_next().value();
        return chunk;
    }

    void SetUp() override {
        config::vector_chunk_size = 4096;
        _profile = _pool.add(new RuntimeProfile("test"));
        _counter = _pool.add(new ScannerCounter());
        _state = _pool.add(new RuntimeState(TQueryGlobals()));
        std::string starrocks_home = getenv("STARROCKS_HOME");
    }

    void TearDown() override {}

private:
    RuntimeProfile* _profile = nullptr;
    ScannerCounter* _counter = nullptr;
    RuntimeState* _state = nullptr;
    ObjectPool _pool;
};

// NOLINTNEXTLINE
TEST_F(JsonScannerTest, test_array_json) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);

    TypeDescriptor array_type(TYPE_ARRAY);
    array_type.children.emplace_back(TypeDescriptor::create_json_type());
    types.emplace_back(array_type);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test9.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"c1", "c2"});

    Status st = scanner->open();
    ASSERT_TRUE(st.ok());

    auto st2 = scanner->get_next();
    ASSERT_TRUE(st2.ok());

    ChunkPtr chunk = st2.value();
    ASSERT_EQ(chunk->columns()[0]->debug_string(), "[1]");
    ASSERT_EQ(chunk->columns()[1]->debug_string(), "[[{\"k2\": \"v2\"}]]");
}

TEST_F(JsonScannerTest, test_json_without_path) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TYPE_DOUBLE);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test1.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"category", "author", "title", "price"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_columns());
    EXPECT_EQ(2, chunk->num_rows());

    EXPECT_EQ("['reference', 'NigelRees', 'SayingsoftheCentury', 8.95]", chunk->debug_row(0));
    EXPECT_EQ("['fiction', 'EvelynWaugh', 'SwordofHonour', 12.99]", chunk->debug_row(1));
}

TEST_F(JsonScannerTest, test_json_path_with_asterisk_basic) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.keyname.*", "$.k1", "$.kind"])";
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test10.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"keyname", "k1", "kind"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(3, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[20, 'v2', 'server']", chunk->debug_row(0));
}

TEST_F(JsonScannerTest, test_json_path_with_asterisk_null) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.keyname.*", "$.k1", "$.kind"])";
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test11.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"keyname", "k1", "kind"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(3, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[NULL, 'v2', 'server']", chunk->debug_row(0));
}

TEST_F(JsonScannerTest, test_json_path_with_asterisk_record) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.keyname.*.a", "$.k1", "$.kind"])";
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test12.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"keyname", "k1", "kind"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(3, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[1, 'v2', 'server']", chunk->debug_row(0));
}

TEST_F(JsonScannerTest, test_json_path_with_asterisk_array) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.keyname.*[0]", "$.k1", "$.kind"])";
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test13.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"keyname", "k1", "kind"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(3, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[25, 'v2', 'server']", chunk->debug_row(0));
}

TEST_F(JsonScannerTest, test_json_with_path) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.k1", "$.kind", "$.keyname.ip", "$.keyname.value"])";
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test2.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"k1", "kind", "ip", "value"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_columns());
    EXPECT_EQ(2, chunk->num_rows());

    EXPECT_EQ("['v1', 'server', '10.10.0.1', 20]", chunk->debug_row(0));
    EXPECT_EQ("['v2', 'server', '10.20.1.1', 20]", chunk->debug_row(1));
}

TEST_F(JsonScannerTest, test_one_level_array) {
    std::vector<TypeDescriptor> types;
    TypeDescriptor t1(TYPE_ARRAY);
    t1.children.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(t1);

    TypeDescriptor t2(TYPE_ARRAY);
    t2.children.emplace_back(TYPE_INT);
    types.emplace_back(t2);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.keyname.ip", "$.keyname.value"])";
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test3.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"k1", "kind", "ip", "value"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(2, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[['10.10.0.1','10.20.1.1'], [10,20]]", chunk->debug_row(0));
}

TEST_F(JsonScannerTest, test_two_level_array) {
    std::vector<TypeDescriptor> types;
    TypeDescriptor t1(TYPE_ARRAY);
    t1.children.emplace_back(TYPE_ARRAY);
    t1.children.back().children.emplace_back(TYPE_BIGINT);
    types.emplace_back(t1);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test4.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"value"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(1, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[[[10,20],[30,40]]]", chunk->debug_row(0));
}

TEST_F(JsonScannerTest, test_invalid_column_in_array) {
    std::vector<TypeDescriptor> types;
    TypeDescriptor t1(TYPE_ARRAY);
    t1.children.emplace_back(TYPE_ARRAY);
    t1.children.back().children.emplace_back(TYPE_SMALLINT);
    types.emplace_back(t1);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test5.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"value"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(1, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[NULL]", chunk->debug_row(0));
}

TEST_F(JsonScannerTest, test_invalid_nested_level1) {
    // the nested level in schema is larger than json
    std::vector<TypeDescriptor> types;
    TypeDescriptor t1(TYPE_ARRAY);
    t1.children.emplace_back(TYPE_ARRAY);
    t1.children.back().children.emplace_back(TYPE_TINYINT);
    types.emplace_back(t1);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test6.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"value"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();

    EXPECT_EQ(1, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[NULL]", chunk->debug_row(0));
}

TEST_F(JsonScannerTest, test_invalid_nested_level2) {
    // the nested level in schema is less than json
    std::vector<TypeDescriptor> types;
    TypeDescriptor t1(TYPE_ARRAY);
    t1.children.emplace_back(TYPE_LARGEINT);
    types.emplace_back(t1);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test7.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"value"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(1, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[[NULL,NULL]]", chunk->debug_row(0));
}

TEST_F(JsonScannerTest, test_json_with_long_string) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TypeDescriptor::create_varchar_type(100));
    types.emplace_back(TypeDescriptor::create_varchar_type(100));

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test8.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"request", "ids"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(2, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());

    EXPECT_EQ("['{\"area\":\"beijing\",\"country\":\"china\"}', '[\"478472290\",\"478473274\"]']", chunk->debug_row(0));
}

TEST_F(JsonScannerTest, test_ndjson) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test_ndjson.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"k1", "kind", "ip", "value"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_columns());
    EXPECT_EQ(5, chunk->num_rows());

    EXPECT_EQ("['v1', 'server', NULL, NULL]", chunk->debug_row(0));
    EXPECT_EQ("['v2', 'server', NULL, NULL]", chunk->debug_row(1));
    EXPECT_EQ("['v3', 'server', NULL, NULL]", chunk->debug_row(2));
    EXPECT_EQ("['v4', 'server', NULL, NULL]", chunk->debug_row(3));
    EXPECT_EQ("['v5', 'server', NULL, NULL]", chunk->debug_row(4));
}

TEST_F(JsonScannerTest, test_ndjson_with_jsonpath) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.k1", "$.kind", "$.keyname.ip", "$.keyname.value"])";
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test_ndjson.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"k1", "kind", "ip", "value"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_columns());
    EXPECT_EQ(5, chunk->num_rows());

    EXPECT_EQ("['v1', 'server', '10.10.0.1', 10]", chunk->debug_row(0));
    EXPECT_EQ("['v2', 'server', '10.10.0.2', 20]", chunk->debug_row(1));
    EXPECT_EQ("['v3', 'server', '10.10.0.3', 30]", chunk->debug_row(2));
    EXPECT_EQ("['v4', 'server', '10.10.0.4', 40]", chunk->debug_row(3));
    EXPECT_EQ("['v5', 'server', '10.10.0.5', 50]", chunk->debug_row(4));
}

TEST_F(JsonScannerTest, test_json_new_parser) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.k1", "$.kind", "$.ip", "$.value"])";
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test_json_new_parser.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"k1", "kind", "ip", "value"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_columns());
    EXPECT_EQ(12, chunk->num_rows());

    EXPECT_EQ("['v1', 'server', '10.10.0.1', NULL]", chunk->debug_row(0));
    EXPECT_EQ("['v2', NULL, '10.10.0.2', 20]", chunk->debug_row(1));
    EXPECT_EQ("[NULL, 'server', '10.10.0.3', 30]", chunk->debug_row(2));
    EXPECT_EQ("[NULL, 'server', NULL, NULL]", chunk->debug_row(3));
    EXPECT_EQ("[NULL, NULL, NULL, 50]", chunk->debug_row(4));
    EXPECT_EQ("['v1', 'server', '10.10.0.1', 10]", chunk->debug_row(5));
    EXPECT_EQ("[NULL, NULL, NULL, 50]", chunk->debug_row(6));
    EXPECT_EQ("['v34', 'server2', '10.10.0.22', NULL]", chunk->debug_row(7));
    EXPECT_EQ("['v3', NULL, '10.10.0.33', 20]", chunk->debug_row(8));
    EXPECT_EQ("[NULL, 'server', '10.10.0.3', 30]", chunk->debug_row(9));
    EXPECT_EQ("[NULL, 'server', NULL, NULL]", chunk->debug_row(10));
    EXPECT_EQ("['v1', 'server', '10.10.0.1', 10]", chunk->debug_row(11));
}

TEST_F(JsonScannerTest, test_adaptive_nullable_column) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.k1", "$.kind", "$.ip", "$.value"])";
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test_adaptive_nullable_column.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"k1", "kind", "ip", "value"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_columns());
    EXPECT_EQ(6, chunk->num_rows());

    EXPECT_EQ("[NULL, NULL, NULL, NULL]", chunk->debug_row(0));
    EXPECT_EQ("['v2', NULL, NULL, NULL]", chunk->debug_row(1));
    EXPECT_EQ("[NULL, 'server', NULL, NULL]", chunk->debug_row(2));
    EXPECT_EQ("[NULL, NULL, '10.10.0.1', NULL]", chunk->debug_row(3));
    EXPECT_EQ("[NULL, NULL, NULL, 50]", chunk->debug_row(4));
    EXPECT_EQ("['v1', 'server', '10.10.0.1', 10]", chunk->debug_row(5));
}

TEST_F(JsonScannerTest, test_adaptive_nullable_column_2) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.k1", "$.kind", "$.ip", "$.value"])";
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test_adaptive_nullable_column.json");
    ranges.emplace_back(range);
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"k1", "kind", "ip", "value"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_columns());
    EXPECT_EQ(6, chunk->num_rows());

    EXPECT_EQ("[NULL, NULL, NULL, NULL]", chunk->debug_row(0));
    EXPECT_EQ("['v2', NULL, NULL, NULL]", chunk->debug_row(1));
    EXPECT_EQ("[NULL, 'server', NULL, NULL]", chunk->debug_row(2));
    EXPECT_EQ("[NULL, NULL, '10.10.0.1', NULL]", chunk->debug_row(3));
    EXPECT_EQ("[NULL, NULL, NULL, 50]", chunk->debug_row(4));
    EXPECT_EQ("['v1', 'server', '10.10.0.1', 10]", chunk->debug_row(5));

    chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_columns());
    EXPECT_EQ(6, chunk->num_rows());
    EXPECT_EQ("[NULL, NULL, NULL, NULL]", chunk->debug_row(0));
    EXPECT_EQ("['v2', NULL, NULL, NULL]", chunk->debug_row(1));
    EXPECT_EQ("[NULL, 'server', NULL, NULL]", chunk->debug_row(2));
    EXPECT_EQ("[NULL, NULL, '10.10.0.1', NULL]", chunk->debug_row(3));
    EXPECT_EQ("[NULL, NULL, NULL, 50]", chunk->debug_row(4));
    EXPECT_EQ("['v1', 'server', '10.10.0.1', 10]", chunk->debug_row(5));
}

TEST_F(JsonScannerTest, test_multi_type) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_BOOLEAN);

    types.emplace_back(TYPE_TINYINT);
    types.emplace_back(TYPE_SMALLINT);
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_BIGINT);
    // Numbers beyond range of uint64_t is not supported by json scanner.
    // Hence, we skip the test of LARGEINT.

    types.emplace_back(TYPE_FLOAT);
    types.emplace_back(TYPE_DOUBLE);

    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TYPE_DATE);
    types.emplace_back(TYPE_DATETIME);
    types.emplace_back(TypeDescriptor::create_varchar_type(20));

    types.emplace_back(TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 27, 9));
    types.emplace_back(TypeDescriptor::create_char_type(20));
    types.emplace_back(TYPE_TIME);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test_multi_type.json");
    ranges.emplace_back(range);

    auto scanner =
            create_json_scanner(types, ranges,
                                {"f_bool", "f_tinyint", "f_smallint", "f_int", "f_bigint", "f_float", "f_double",
                                 "f_varchar", "f_date", "f_datetime", "f_array", "f_decimal", "f_char", "f_time"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(14, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());

    auto expected =
            "[1, 127, 32767, 2147483647, 9223372036854775807, 3.14, 3.14, 'starrocks', 2021-12-09, 2021-12-09 "
            "10:00:00, '[1,3,5]', 1234565789012345678901234567.123456789, 'starrocks', 36000]";

    EXPECT_EQ(expected, chunk->debug_row(0));
}

TEST_F(JsonScannerTest, test_cast_type) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TYPE_DOUBLE);
    types.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test_cast_type.json");
    ranges.emplace_back(range);

    auto scanner =
            create_json_scanner(types, ranges, {"f_float", "f_bool", "f_int", "f_float_in_string", "f_int_in_string"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(5, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());

    EXPECT_EQ("['3.14', '1', '123', 3.14, 123]", chunk->debug_row(0));
}

TEST_F(JsonScannerTest, test_load_native_json) {
    std::vector<TypeDescriptor> types;
    constexpr int kNumColumns = 16;
    for (int i = 0; i < kNumColumns; i++) {
        types.emplace_back(TypeDescriptor::create_json_type());
    }

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test_json_load.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(
            types, ranges,
            {"f_bool", "f_tinyint", "f_smallint", "f_int", "f_bigint", "f_float", "f_double", "f_varchar", "f_date",
             "f_datetime", "f_array", "f_decimal", "f_char", "f_time", "f_nestarray", "f_object"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(kNumColumns, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());

    auto expected =
            "[true, 127, 32767, 2147483647, 9223372036854775807, 3.14, 3.14, \"starrocks\", "
            "\"2021-12-09\", \"2021-12-09 10:00:00\", [1, 3, 5], "
            "\"1234565789012345678901234567.123456789\", \"starrocks\", \"10:00:00\", "
            "[[{\"k1\": 1, \"k2\": 2}, {\"k3\": 3}], [{\"k1\": 4, \"k2\": 5}, {\"k3\": 6}]], "
            "{\"n1\": {\"n2\": {\"n3\": 1.1}}}]";

    EXPECT_EQ(expected, chunk->debug_row(0));
}

TEST_F(JsonScannerTest, test_native_json_ndjson_with_jsonpath) {
    std::vector<TypeDescriptor> types;
    constexpr int kColumns = 4;
    for (int i = 0; i < kColumns; i++) {
        types.emplace_back(TypeDescriptor::create_json_type());
    }

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.k1", "$.kind", "$.keyname.ip", "$.keyname.value"])";
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test_ndjson.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"k1", "kind", "ip", "value"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_columns());
    EXPECT_EQ(5, chunk->num_rows());

    EXPECT_EQ(R"(["v1", "server", "10.10.0.1", "10"])", chunk->debug_row(0));
    EXPECT_EQ(R"(["v2", "server", "10.10.0.2", "20"])", chunk->debug_row(1));
    EXPECT_EQ(R"(["v3", "server", "10.10.0.3", "30"])", chunk->debug_row(2));
    EXPECT_EQ(R"(["v4", "server", "10.10.0.4", "40"])", chunk->debug_row(3));
    EXPECT_EQ(R"(["v5", "server", "10.10.0.5", "50"])", chunk->debug_row(4));
}

TEST_F(JsonScannerTest, test_string_single_column) {
    std::string datapath = "./be/test/exec/test_data/json_scanner/test_ndjson_chinese.json";
    {
        std::string jsonpath1 = R"(["$"])";
        ChunkPtr result1 = test_whole_row_json(1, datapath, jsonpath1, {"$"});
        EXPECT_EQ(1, result1->num_columns());
        EXPECT_EQ(5, result1->num_rows());

        EXPECT_EQ(R"(['{"k1":"v1",   "kind":"中文", "keyname":{"ip地址":"10.10.0.1", "value":"10"}}'])",
                  result1->debug_row(0));
        EXPECT_EQ(R"(['{"k1":"v2", "kind":"英文", "keyname":{"ip地址":"10.10.0.2", "value":"20"}}'])",
                  result1->debug_row(1));
        EXPECT_EQ(R"(['{"k1":"v3", "kind":"法文", "keyname":{"ip地址":"10.10.0.3", "value":"30"}}'])",
                  result1->debug_row(2));
        EXPECT_EQ(R"(['{"k1":"v4",   "kind":"德文", "keyname":{"ip地址":"10.10.0.4", "value":"40"}}'])",
                  result1->debug_row(3));
        EXPECT_EQ(R"(['{"k1":"v5",   "kind":"西班牙", "keyname":{"ip地址":"10.10.0.5", "value":"50"}}'])",
                  result1->debug_row(4));
    }
}

TEST_F(JsonScannerTest, test_native_json_single_column) {
    std::string datapath = "./be/test/exec/test_data/json_scanner/test_ndjson_chinese.json";

    {
        std::string jsonpath1 = R"(["$"])";
        ChunkPtr result1 = test_with_jsonpath(1, datapath, jsonpath1, {"$"});
        EXPECT_EQ(1, result1->num_columns());
        EXPECT_EQ(5, result1->num_rows());

        EXPECT_EQ(R"([{"k1": "v1", "keyname": {"ip地址": "10.10.0.1", "value": "10"}, "kind": "中文"}])",
                  result1->debug_row(0));
        EXPECT_EQ(R"([{"k1": "v2", "keyname": {"ip地址": "10.10.0.2", "value": "20"}, "kind": "英文"}])",
                  result1->debug_row(1));
        EXPECT_EQ(R"([{"k1": "v3", "keyname": {"ip地址": "10.10.0.3", "value": "30"}, "kind": "法文"}])",
                  result1->debug_row(2));
        EXPECT_EQ(R"([{"k1": "v4", "keyname": {"ip地址": "10.10.0.4", "value": "40"}, "kind": "德文"}])",
                  result1->debug_row(3));
        EXPECT_EQ(R"([{"k1": "v5", "keyname": {"ip地址": "10.10.0.5", "value": "50"}, "kind": "西班牙"}])",
                  result1->debug_row(4));
    }
    {
        std::string jsonpath1 = R"(["$", "$.k1"])";
        ChunkPtr result1 = test_with_jsonpath(2, datapath, jsonpath1, {"$", "$.k1"});
        EXPECT_EQ(2, result1->num_columns());
        EXPECT_EQ(5, result1->num_rows());

        EXPECT_EQ(R"([{"k1": "v1", "keyname": {"ip地址": "10.10.0.1", "value": "10"}, "kind": "中文"}, "v1"])",
                  result1->debug_row(0));
        EXPECT_EQ(R"([{"k1": "v2", "keyname": {"ip地址": "10.10.0.2", "value": "20"}, "kind": "英文"}, "v2"])",
                  result1->debug_row(1));
        EXPECT_EQ(R"([{"k1": "v3", "keyname": {"ip地址": "10.10.0.3", "value": "30"}, "kind": "法文"}, "v3"])",
                  result1->debug_row(2));
        EXPECT_EQ(R"([{"k1": "v4", "keyname": {"ip地址": "10.10.0.4", "value": "40"}, "kind": "德文"}, "v4"])",
                  result1->debug_row(3));
        EXPECT_EQ(R"([{"k1": "v5", "keyname": {"ip地址": "10.10.0.5", "value": "50"}, "kind": "西班牙"}, "v5"])",
                  result1->debug_row(4));
    }
    {
        std::string jsonpath1 = R"(["$.k1","$"])";
        ChunkPtr result1 = test_with_jsonpath(2, datapath, jsonpath1, {"$.k1", "$"});
        EXPECT_EQ(2, result1->num_columns());
        EXPECT_EQ(5, result1->num_rows());

        EXPECT_EQ(R"(["v1", {"k1": "v1", "keyname": {"ip地址": "10.10.0.1", "value": "10"}, "kind": "中文"}])",
                  result1->debug_row(0));
        EXPECT_EQ(R"(["v2", {"k1": "v2", "keyname": {"ip地址": "10.10.0.2", "value": "20"}, "kind": "英文"}])",
                  result1->debug_row(1));
        EXPECT_EQ(R"(["v3", {"k1": "v3", "keyname": {"ip地址": "10.10.0.3", "value": "30"}, "kind": "法文"}])",
                  result1->debug_row(2));
        EXPECT_EQ(R"(["v4", {"k1": "v4", "keyname": {"ip地址": "10.10.0.4", "value": "40"}, "kind": "德文"}])",
                  result1->debug_row(3));
        EXPECT_EQ(R"(["v5", {"k1": "v5", "keyname": {"ip地址": "10.10.0.5", "value": "50"}, "kind": "西班牙"}])",
                  result1->debug_row(4));
    }
    {
        std::string jsonpath1 = R"(["$.k1", "$.kind", "$"])";
        ChunkPtr result1 = test_with_jsonpath(3, datapath, jsonpath1, {"$.k1", "$.kind", "$"});
        EXPECT_EQ(3, result1->num_columns());
        EXPECT_EQ(5, result1->num_rows());

        EXPECT_EQ(R"(["v1", "中文", {"k1": "v1", "keyname": {"ip地址": "10.10.0.1", "value": "10"}, "kind": "中文"}])",
                  result1->debug_row(0));
        EXPECT_EQ(R"(["v2", "英文", {"k1": "v2", "keyname": {"ip地址": "10.10.0.2", "value": "20"}, "kind": "英文"}])",
                  result1->debug_row(1));
        EXPECT_EQ(R"(["v3", "法文", {"k1": "v3", "keyname": {"ip地址": "10.10.0.3", "value": "30"}, "kind": "法文"}])",
                  result1->debug_row(2));
        EXPECT_EQ(R"(["v4", "德文", {"k1": "v4", "keyname": {"ip地址": "10.10.0.4", "value": "40"}, "kind": "德文"}])",
                  result1->debug_row(3));
        EXPECT_EQ(
                R"(["v5", "西班牙", {"k1": "v5", "keyname": {"ip地址": "10.10.0.5", "value": "50"}, "kind": "西班牙"}])",
                result1->debug_row(4));
    }
    {
        std::string jsonpath1 = R"(["$.k1", "$.kind", "$", "$.k2"])";
        ChunkPtr result1 = test_with_jsonpath(4, datapath, jsonpath1, {"$.k1", "$.kind", "$", "$.k2"});
        EXPECT_EQ(4, result1->num_columns());
        EXPECT_EQ(5, result1->num_rows());

        EXPECT_EQ(
                R"(["v1", "中文", {"k1": "v1", "keyname": {"ip地址": "10.10.0.1", "value": "10"}, "kind": "中文"}, NULL])",
                result1->debug_row(0));
        EXPECT_EQ(
                R"(["v2", "英文", {"k1": "v2", "keyname": {"ip地址": "10.10.0.2", "value": "20"}, "kind": "英文"}, NULL])",
                result1->debug_row(1));
        EXPECT_EQ(
                R"(["v3", "法文", {"k1": "v3", "keyname": {"ip地址": "10.10.0.3", "value": "30"}, "kind": "法文"}, NULL])",
                result1->debug_row(2));
        EXPECT_EQ(
                R"(["v4", "德文", {"k1": "v4", "keyname": {"ip地址": "10.10.0.4", "value": "40"}, "kind": "德文"}, NULL])",
                result1->debug_row(3));
        EXPECT_EQ(
                R"(["v5", "西班牙", {"k1": "v5", "keyname": {"ip地址": "10.10.0.5", "value": "50"}, "kind": "西班牙"}, NULL])",
                result1->debug_row(4));
    }
}

TEST_F(JsonScannerTest, test_expanded_with_json_root) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_json_type());

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.jsonpaths = "";
    range.json_root = "$.data";

    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = true;

    range.__set_path("./be/test/exec/test_data/json_scanner/test_expanded_array.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"k1", "kind", "keyname"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(3, chunk->num_columns());
    EXPECT_EQ(5, chunk->num_rows());

    EXPECT_EQ("['v1', 'server', {\"ip\": \"10.10.0.1\", \"value\": \"10\"}]", chunk->debug_row(0));
    EXPECT_EQ("['v2', 'server', {\"ip\": \"10.10.0.2\", \"value\": \"20\"}]", chunk->debug_row(1));
    EXPECT_EQ("['v3', 'server', {\"ip\": \"10.10.0.3\", \"value\": \"30\"}]", chunk->debug_row(2));
    EXPECT_EQ("['v4', 'server', {\"ip\": \"10.10.0.4\", \"value\": \"40\"}]", chunk->debug_row(3));
    EXPECT_EQ("['v5', 'server', {\"ip\": \"10.10.0.5\", \"value\": \"50\"}]", chunk->debug_row(4));
}

TEST_F(JsonScannerTest, test_ndjson_expanded_with_json_root) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_json_type());

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.jsonpaths = "";
    range.json_root = "$.data";

    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = true;

    range.__set_path("./be/test/exec/test_data/json_scanner/test_ndjson_expanded_array.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"k1", "kind", "keyname"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(3, chunk->num_columns());
    EXPECT_EQ(5, chunk->num_rows());

    EXPECT_EQ("['v1', 'server', {\"ip\": \"10.10.0.1\", \"value\": \"10\"}]", chunk->debug_row(0));
    EXPECT_EQ("['v2', 'server', {\"ip\": \"10.10.0.2\", \"value\": \"20\"}]", chunk->debug_row(1));
    EXPECT_EQ("['v3', 'server', {\"ip\": \"10.10.0.3\", \"value\": \"30\"}]", chunk->debug_row(2));
    EXPECT_EQ("['v4', 'server', {\"ip\": \"10.10.0.4\", \"value\": \"40\"}]", chunk->debug_row(3));
    EXPECT_EQ("['v5', 'server', {\"ip\": \"10.10.0.5\", \"value\": \"50\"}]", chunk->debug_row(4));
}

// this test covers json_scanner.cpp:_construct_row_in_object_order.
TEST_F(JsonScannerTest, test_construct_row_in_object_order) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TYPE_DOUBLE);
    types.emplace_back(TYPE_DOUBLE);
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/test_cast_type.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges,
                                       {"f_dummy_0", "f_float", "f_dummy_1", "f_bool", "f_dummy_2", "f_int",
                                        "f_dummy_3", "f_float_in_string", "f_dummy_4", "f_int_in_string", "f_dummy_5"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(11, chunk->num_columns());
    EXPECT_EQ(1, chunk->num_rows());

    EXPECT_EQ("[NULL, '3.14', NULL, '1', NULL, '123', NULL, 3.14, NULL, 123, NULL]", chunk->debug_row(0));
}

TEST_F(JsonScannerTest, test_jsonroot_with_jsonpath) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = true;
    range.__isset.json_root = true;

    range.jsonpaths = R"(["$.ip", "$.value"])";
    range.json_root = "$.keyname";

    range.__set_path("./be/test/exec/test_data/json_scanner/test_ndjson.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"ip", "value"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(2, chunk->num_columns());
    EXPECT_EQ(5, chunk->num_rows());

    EXPECT_EQ("['10.10.0.1', '10']", chunk->debug_row(0));
    EXPECT_EQ("['10.10.0.2', '20']", chunk->debug_row(1));
    EXPECT_EQ("['10.10.0.3', '30']", chunk->debug_row(2));
    EXPECT_EQ("['10.10.0.4', '40']", chunk->debug_row(3));
    EXPECT_EQ("['10.10.0.5', '50']", chunk->debug_row(4));
}

TEST_F(JsonScannerTest, test_expanded_with_jsonroot_and_extracted_by_jsonpath) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.jsonpaths = R"(["$.keyname.ip", "$.keyname.value"])";
    range.json_root = "$.data";

    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = true;
    range.__isset.json_root = true;

    range.__set_path("./be/test/exec/test_data/json_scanner/test_expanded_array.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"ip", "value"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(2, chunk->num_columns());
    EXPECT_EQ(5, chunk->num_rows());

    EXPECT_EQ("['10.10.0.1', '10']", chunk->debug_row(0));
    EXPECT_EQ("['10.10.0.2', '20']", chunk->debug_row(1));
    EXPECT_EQ("['10.10.0.3', '30']", chunk->debug_row(2));
    EXPECT_EQ("['10.10.0.4', '40']", chunk->debug_row(3));
    EXPECT_EQ("['10.10.0.5', '50']", chunk->debug_row(4));
}

TEST_F(JsonScannerTest, test_ndjson_expaned_with_jsonroot_and_extracted_by_jsonpath) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.jsonpaths = R"(["$.keyname.ip", "$.keyname.value"])";
    range.json_root = "$.data";

    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = true;
    range.__isset.json_root = true;

    range.__set_path("./be/test/exec/test_data/json_scanner/test_ndjson_expanded_array.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"ip", "value"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(2, chunk->num_columns());
    EXPECT_EQ(5, chunk->num_rows());

    EXPECT_EQ("['10.10.0.1', '10']", chunk->debug_row(0));
    EXPECT_EQ("['10.10.0.2', '20']", chunk->debug_row(1));
    EXPECT_EQ("['10.10.0.3', '30']", chunk->debug_row(2));
    EXPECT_EQ("['10.10.0.4', '40']", chunk->debug_row(3));
    EXPECT_EQ("['10.10.0.5', '50']", chunk->debug_row(4));
}

TEST_F(JsonScannerTest, test_illegal_input) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TYPE_DOUBLE);
    types.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/illegal.json");
    ranges.emplace_back(range);

    auto scanner =
            create_json_scanner(types, ranges, {"f_float", "f_bool", "f_int", "f_float_in_string", "f_int_in_string"});

    ASSERT_OK(scanner->open());
    ASSERT_TRUE(scanner->get_next().status().is_data_quality_error());
}

TEST_F(JsonScannerTest, test_illegal_input_with_jsonpath) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TYPE_DOUBLE);
    types.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.f_float", "$.f_bool", "$.f_int", "$.f_float_in_string", "$.f_int_in_string"])";
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/illegal.json");
    ranges.emplace_back(range);

    auto scanner =
            create_json_scanner(types, ranges, {"f_float", "f_bool", "f_int", "f_float_in_string", "f_int_in_string"});

    ASSERT_OK(scanner->open());
    ASSERT_TRUE(scanner->get_next().status().is_data_quality_error());
}

// See more: https://github.com/StarRocks/starrocks/issues/11054
TEST_F(JsonScannerTest, test_column_2x_than_columns_with_json_root) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_json_type());
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = true;
    range.jsonpaths = "";
    range.json_root = "$.data";

    range.__isset.strip_outer_array = true;
    range.__isset.jsonpaths = false;
    range.__isset.json_root = true;

    range.__set_path("./be/test/exec/test_data/json_scanner/test_ndjson_expanded_array.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges,
                                       {"k1", "kind", "keyname", "null_col1", "null_col2", "null_col3", "null_col4"});

    Status st;
    st = scanner->open();
    ASSERT_TRUE(st.ok());

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(7, chunk->num_columns());
    EXPECT_EQ(5, chunk->num_rows());

    EXPECT_EQ("['v1', 'server', {\"ip\": \"10.10.0.1\", \"value\": \"10\"}, NULL, NULL, NULL, NULL]",
              chunk->debug_row(0));
    EXPECT_EQ("['v2', 'server', {\"ip\": \"10.10.0.2\", \"value\": \"20\"}, NULL, NULL, NULL, NULL]",
              chunk->debug_row(1));
    EXPECT_EQ("['v3', 'server', {\"ip\": \"10.10.0.3\", \"value\": \"30\"}, NULL, NULL, NULL, NULL]",
              chunk->debug_row(2));
    EXPECT_EQ("['v4', 'server', {\"ip\": \"10.10.0.4\", \"value\": \"40\"}, NULL, NULL, NULL, NULL]",
              chunk->debug_row(3));
    EXPECT_EQ("['v5', 'server', {\"ip\": \"10.10.0.5\", \"value\": \"50\"}, NULL, NULL, NULL, NULL]",
              chunk->debug_row(4));
}

TEST_F(JsonScannerTest, test_null_with_jsonpath) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TypeDescriptor::create_varchar_type(20));
    types.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.format_type = TFileFormatType::FORMAT_JSON;
    range.file_type = TFileType::FILE_LOCAL;
    range.strip_outer_array = false;
    range.__isset.strip_outer_array = false;
    range.__isset.jsonpaths = true;
    range.jsonpaths = R"(["$.k1", "$.kind", "$.keyname.ip", "$.keyname.value"])";
    range.__isset.json_root = false;
    range.__set_path("./be/test/exec/test_data/json_scanner/null.json");
    ranges.emplace_back(range);

    auto scanner = create_json_scanner(types, ranges, {"k1", "kind", "ip", "value"});

    ASSERT_OK(scanner->open());
    ASSERT_TRUE(scanner->get_next().status().is_data_quality_error());
}

} // namespace starrocks
