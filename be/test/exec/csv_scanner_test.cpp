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

#include "exec/csv_scanner.h"

#include <gtest/gtest.h>

#include <iostream>

#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "fs/fs_memory.h"
#include "fs/fs_util.h"
#include "gen_cpp/Descriptors_types.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"

namespace starrocks {

using ::testing::TestWithParam;
using ::testing::Values;

class CSVScannerTest : public TestWithParam<bool> {
protected:
    virtual void SetUp() { _use_v2 = GetParam(); }
    void TearDown() override {}

    std::unique_ptr<CSVScanner> create_csv_scanner(const std::vector<TypeDescriptor>& types,
                                                   const std::vector<TBrokerRangeDesc>& ranges,
                                                   const std::string& multi_row_delimiter = "\n",
                                                   const std::string& multi_column_separator = "|",
                                                   const int64_t skip_header = 0, const bool trim_space = false,
                                                   const char enclose = 0, const char escape = 0) {
        /// Init DescriptorTable
        TDescriptorTableBuilder desc_tbl_builder;
        TTupleDescriptorBuilder tuple_desc_builder;
        for (auto& t : types) {
            TSlotDescriptorBuilder slot_desc_builder;
            slot_desc_builder.type(t).length(t.len).precision(t.precision).scale(t.scale).nullable(true);
            tuple_desc_builder.add_slot(slot_desc_builder.build());
        }
        tuple_desc_builder.build(&desc_tbl_builder);

        DescriptorTbl* desc_tbl = nullptr;
        Status st = DescriptorTbl::create(&_runtime_state, &_obj_pool, desc_tbl_builder.desc_tbl(), &desc_tbl,
                                          config::vector_chunk_size);
        CHECK(st.ok()) << st.to_string();

        /// Init RuntimeState
        RuntimeState* state = _obj_pool.add(new RuntimeState(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr));
        state->set_desc_tbl(desc_tbl);
        state->init_instance_mem_tracker();
        state->_query_options.query_type = TQueryType::LOAD;

        /// TBrokerScanRangeParams
        TBrokerScanRangeParams* params = _obj_pool.add(new TBrokerScanRangeParams());
        params->__set_multi_row_delimiter(multi_row_delimiter);
        params->__set_multi_column_separator(multi_column_separator);
        params->strict_mode = true;
        params->dest_tuple_id = 0;
        params->src_tuple_id = 0;
        params->__set_skip_header(skip_header);
        params->__set_trim_space(trim_space);
        params->__set_enclose(enclose);
        params->__set_escape(escape);
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

        RuntimeProfile* profile = _obj_pool.add(new RuntimeProfile("test_prof", true));

        ScannerCounter* counter = _obj_pool.add(new ScannerCounter());

        TBrokerScanRange* broker_scan_range = _obj_pool.add(new TBrokerScanRange());
        broker_scan_range->params = *params;
        broker_scan_range->ranges = ranges;
        return std::make_unique<CSVScanner>(state, profile, *broker_scan_range, counter);
    }

    bool _use_v2;

private:
    RuntimeState _runtime_state;
    ObjectPool _obj_pool;
};

class CSVScannerTrimSpaceTest : public CSVScannerTest {};

TEST_P(CSVScannerTest, test_scalar_types) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_DOUBLE);
    types.emplace_back(TYPE_VARCHAR);
    types.emplace_back(TYPE_DATE);
    types.emplace_back(TYPE_VARCHAR);

    types[2].len = 10;
    types[4].len = 6;

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range_one;
    range_one.__set_path("./be/test/exec/test_data/csv_scanner/csv_file1");
    range_one.__set_start_offset(0);
    range_one.__set_num_of_columns_from_file(types.size());
    ranges.push_back(range_one);

    TBrokerRangeDesc range_second;
    range_second.__set_path("./be/test/exec/test_data/csv_scanner/csv_file2");
    range_second.__set_start_offset(0);
    range_second.__set_num_of_columns_from_file(types.size());
    ranges.push_back(range_second);

    auto scanner = create_csv_scanner(types, ranges);
    EXPECT_NE(scanner, nullptr);

    auto st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    scanner->use_v2(_use_v2);

    auto res = scanner->get_next();
    ASSERT_TRUE(res.ok()) << res.status().to_string();
    auto chunk = res.value();

    auto chunk2 = scanner->get_next().value();

    chunk->append(*chunk2);

    EXPECT_EQ(5, chunk->num_columns());
    EXPECT_EQ(4, chunk->num_rows());

    // int column
    EXPECT_EQ(1, chunk->get(0)[0].get_int32());
    EXPECT_EQ(-1, chunk->get(1)[0].get_int32());
    EXPECT_EQ(10, chunk->get(2)[0].get_int32());
    EXPECT_EQ(10, chunk->get(3)[0].get_int32());

    // double column
    EXPECT_FLOAT_EQ(1.1, chunk->get(0)[1].get_double());
    EXPECT_FLOAT_EQ(-0.1, chunk->get(1)[1].get_double());
    EXPECT_TRUE(chunk->get(2)[1].is_null());
    EXPECT_FLOAT_EQ(10.1, chunk->get(3)[1].get_double());

    // string column
    EXPECT_EQ("apple", chunk->get(0)[2].get_slice());
    EXPECT_EQ("banana", chunk->get(1)[2].get_slice());
    EXPECT_EQ("grapefruit", chunk->get(2)[2].get_slice());
    EXPECT_EQ("orange", chunk->get(3)[2].get_slice());

    // date column
    EXPECT_EQ("2020-01-01", chunk->get(0)[3].get_date().to_string());
    EXPECT_EQ("1998-09-01", chunk->get(1)[3].get_date().to_string());
    EXPECT_EQ("2021-02-19", chunk->get(2)[3].get_date().to_string());
    EXPECT_EQ("2021-01-01", chunk->get(3)[3].get_date().to_string());

    // string column with size limit.
    // len(apple) == 5 < 6
    EXPECT_EQ(false, chunk->get(0)[4].is_null());
    EXPECT_EQ("apple", chunk->get(0)[4].get_slice());
    // len(banana) == 6
    EXPECT_EQ(false, chunk->get(1)[4].is_null());
    EXPECT_EQ("banana", chunk->get(1)[4].get_slice());
    // len(grapefruit) == 10 > 6
    EXPECT_EQ(true, chunk->get(2)[4].is_null());
    // len(oranges) == 7 > 6
    EXPECT_EQ(true, chunk->get(3)[4].is_null());
}

TEST_P(CSVScannerTest, test_adaptive_nullable_column1) {
    std::vector<TypeDescriptor> types{TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_INT)};

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_num_of_columns_from_file(3);
    range.__set_path("./be/test/exec/test_data/csv_scanner/csv_file20");
    ranges.push_back(range);

    auto scanner = create_csv_scanner(types, ranges, "\n", ",", 0, true, '\'', '\\');
    Status st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_rows());

    EXPECT_EQ(true, chunk->get(0)[0].is_null());
    EXPECT_EQ(2, chunk->get(1)[0].get_int32());
    EXPECT_EQ(true, chunk->get(2)[0].is_null());
    EXPECT_EQ(4, chunk->get(3)[0].get_int32());

    EXPECT_EQ(true, chunk->get(0)[1].is_null());
    EXPECT_EQ(true, chunk->get(1)[1].is_null());
    EXPECT_EQ(true, chunk->get(2)[1].is_null());
    EXPECT_EQ("Julia", chunk->get(3)[1].get_slice());

    EXPECT_EQ(true, chunk->get(0)[2].is_null());
    EXPECT_EQ(true, chunk->get(1)[2].is_null());
    EXPECT_EQ(25, chunk->get(2)[2].get_int32());
    EXPECT_EQ(25, chunk->get(3)[2].get_int32());
}

TEST_P(CSVScannerTest, test_adaptive_nullable_column2) {
    std::vector<TypeDescriptor> types{TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_INT)};

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_num_of_columns_from_file(3);
    range.__set_path("./be/test/exec/test_data/csv_scanner/csv_file21");
    ranges.push_back(range);

    auto scanner = create_csv_scanner(types, ranges, "\n", ",", 0, true, '\'', '\\');
    Status st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_rows());

    EXPECT_EQ(1, chunk->get(0)[0].get_int32());
    EXPECT_EQ(2, chunk->get(1)[0].get_int32());
    EXPECT_EQ(3, chunk->get(2)[0].get_int32());
    EXPECT_EQ(true, chunk->get(3)[0].is_null());

    EXPECT_EQ("Julia", chunk->get(0)[1].get_slice());
    EXPECT_EQ("Andy", chunk->get(1)[1].get_slice());
    EXPECT_EQ("Joke", chunk->get(2)[1].get_slice());
    EXPECT_EQ(true, chunk->get(3)[1].is_null());

    EXPECT_EQ(20, chunk->get(0)[2].get_int32());
    EXPECT_EQ(21, chunk->get(1)[2].get_int32());
    EXPECT_EQ(22, chunk->get(2)[2].get_int32());
    EXPECT_EQ(25, chunk->get(3)[2].get_int32());
}

TEST_P(CSVScannerTest, test_adaptive_nullable_column3) {
    std::vector<TypeDescriptor> types{TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_INT)};

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_num_of_columns_from_file(3);
    range.__set_path("./be/test/exec/test_data/csv_scanner/csv_file20");
    ranges.push_back(range);

    TBrokerRangeDesc range2;
    range2.__set_num_of_columns_from_file(3);
    range2.__set_path("./be/test/exec/test_data/csv_scanner/csv_file21");
    ranges.push_back(range2);

    auto scanner = create_csv_scanner(types, ranges, "\n", ",", 0, true, '\'', '\\');
    Status st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_rows());

    EXPECT_EQ(true, chunk->get(0)[0].is_null());
    EXPECT_EQ(2, chunk->get(1)[0].get_int32());
    EXPECT_EQ(true, chunk->get(2)[0].is_null());
    EXPECT_EQ(4, chunk->get(3)[0].get_int32());

    EXPECT_EQ(true, chunk->get(0)[1].is_null());
    EXPECT_EQ(true, chunk->get(1)[1].is_null());
    EXPECT_EQ(true, chunk->get(2)[1].is_null());
    EXPECT_EQ("Julia", chunk->get(3)[1].get_slice());

    EXPECT_EQ(true, chunk->get(0)[2].is_null());
    EXPECT_EQ(true, chunk->get(1)[2].is_null());
    EXPECT_EQ(25, chunk->get(2)[2].get_int32());
    EXPECT_EQ(25, chunk->get(3)[2].get_int32());

    chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_rows());

    EXPECT_EQ(1, chunk->get(0)[0].get_int32());
    EXPECT_EQ(2, chunk->get(1)[0].get_int32());
    EXPECT_EQ(3, chunk->get(2)[0].get_int32());
    EXPECT_EQ(true, chunk->get(3)[0].is_null());

    EXPECT_EQ("Julia", chunk->get(0)[1].get_slice());
    EXPECT_EQ("Andy", chunk->get(1)[1].get_slice());
    EXPECT_EQ("Joke", chunk->get(2)[1].get_slice());
    EXPECT_EQ(true, chunk->get(3)[1].is_null());

    EXPECT_EQ(20, chunk->get(0)[2].get_int32());
    EXPECT_EQ(21, chunk->get(1)[2].get_int32());
    EXPECT_EQ(22, chunk->get(2)[2].get_int32());
    EXPECT_EQ(25, chunk->get(3)[2].get_int32());
}

TEST_P(CSVScannerTest, test_multi_seprator) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_DOUBLE);
    types.emplace_back(TYPE_VARCHAR);
    types.emplace_back(TYPE_DATE);
    types.emplace_back(TYPE_VARCHAR);

    types[2].len = 10;
    types[4].len = 6;

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range_one;
    range_one.__set_path("./be/test/exec/test_data/csv_scanner/csv_file14");
    range_one.__set_start_offset(0);
    range_one.__set_num_of_columns_from_file(types.size());
    ranges.push_back(range_one);

    auto scanner = create_csv_scanner(types, ranges, "<br>", "^^");
    EXPECT_NE(scanner, nullptr);

    auto st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    scanner->use_v2(_use_v2);

    auto res = scanner->get_next();
    ASSERT_TRUE(res.ok()) << res.status().to_string();
    auto chunk = res.value();

    EXPECT_EQ(5, chunk->num_columns());
    EXPECT_EQ(2, chunk->num_rows());

    // int column
    EXPECT_EQ(1, chunk->get(0)[0].get_int32());
    EXPECT_EQ(-1, chunk->get(1)[0].get_int32());

    // double column
    EXPECT_FLOAT_EQ(1.1, chunk->get(0)[1].get_double());
    EXPECT_FLOAT_EQ(-0.1, chunk->get(1)[1].get_double());

    // string column
    EXPECT_EQ("ap", chunk->get(0)[2].get_slice());
    EXPECT_EQ("br", chunk->get(1)[2].get_slice());

    // date column
    EXPECT_EQ("2020-01-01", chunk->get(0)[3].get_date().to_string());
    EXPECT_EQ("1998-09-01", chunk->get(1)[3].get_date().to_string());
}

TEST_P(CSVScannerTest, test_array_of_int) {
    TypeDescriptor t;
    t.type = TYPE_ARRAY;
    t.children.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_path("./be/test/exec/test_data/csv_scanner/csv_file3");
    range.__set_start_offset(0);
    range.__set_num_of_columns_from_file(1);
    ranges.push_back(range);

    auto scanner = create_csv_scanner({t}, ranges);
    EXPECT_NE(scanner, nullptr);

    auto st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    scanner->use_v2(_use_v2);

    auto chunk = scanner->get_next().value();
    EXPECT_EQ(1, chunk->num_columns());
    EXPECT_EQ(5, chunk->num_rows());

    // 1st row
    EXPECT_EQ(0, chunk->get(0)[0].get_array().size());

    // 2nd row
    EXPECT_EQ(1, chunk->get(1)[0].get_array().size());
    EXPECT_TRUE(chunk->get(1)[0].get_array()[0].is_null());

    // 3rd row
    EXPECT_TRUE(chunk->get(2)[0].is_null());

    // 4th row
    EXPECT_EQ(2, chunk->get(3)[0].get_array().size());
    EXPECT_EQ(1, chunk->get(3)[0].get_array()[0].get_int32());
    EXPECT_EQ(2, chunk->get(3)[0].get_array()[1].get_int32());

    // 5th row
    EXPECT_EQ(2, chunk->get(4)[0].get_array().size());
    EXPECT_EQ(1, chunk->get(4)[0].get_array()[0].get_int32());
    EXPECT_TRUE(chunk->get(4)[0].get_array()[1].is_null());
}

TEST_P(CSVScannerTest, test_array_of_string) {
    std::vector<TypeDescriptor> types;
    // ARRAY<VARCHAR(10)>
    TypeDescriptor t;
    t.type = TYPE_ARRAY;
    t.children.emplace_back(TYPE_VARCHAR);
    t.children.back().len = 10;

    types.emplace_back(t);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_path("./be/test/exec/test_data/csv_scanner/csv_file4");
    range.__set_start_offset(0);
    range.__set_num_of_columns_from_file(types.size());
    ranges.push_back(range);

    auto scanner = create_csv_scanner(types, ranges);
    EXPECT_NE(scanner, nullptr);

    Status st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();
    scanner->use_v2(_use_v2);
    auto res = scanner->get_next();
    ASSERT_TRUE(res.ok()) << res.status().to_string();
    auto chunk = res.value();
    EXPECT_EQ(1, chunk->num_columns());
    EXPECT_EQ(6, chunk->num_rows());

    // []
    EXPECT_EQ(0, chunk->get(0)[0].get_array().size());

    // [null]
    EXPECT_EQ(1, chunk->get(1)[0].get_array().size());
    EXPECT_TRUE(chunk->get(1)[0].get_array()[0].is_null());

    // \N
    EXPECT_TRUE(chunk->get(2)[0].is_null());

    // ["apple",null,"pear"]
    EXPECT_EQ(3, chunk->get(3)[0].get_array().size());
    EXPECT_EQ("apple", chunk->get(3)[0].get_array()[0].get_slice());
    EXPECT_TRUE(chunk->get(3)[0].get_array()[1].is_null());
    EXPECT_EQ("pear", chunk->get(3)[0].get_array()[2].get_slice());

    // ["str with left bracket([)","str with dot(,)"]
    EXPECT_EQ(2, chunk->get(4)[0].get_array().size());
    EXPECT_EQ("str with left bracket([)", chunk->get(4)[0].get_array()[0].get_slice());
    EXPECT_EQ("str with dot(,)", chunk->get(4)[0].get_array()[1].get_slice());

    // ["I""m hungry!",""]
    EXPECT_EQ(2, chunk->get(5)[0].get_array().size());
    EXPECT_EQ("I\"m hungry!", chunk->get(5)[0].get_array()[0].get_slice());
    EXPECT_EQ("", chunk->get(5)[0].get_array()[1].get_slice());
}

TEST_P(CSVScannerTest, test_array_of_date) {
    TypeDescriptor t;
    t.type = TYPE_ARRAY;
    t.children.emplace_back(TYPE_DATE);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_path("./be/test/exec/test_data/csv_scanner/csv_file5");
    range.__set_start_offset(0);
    range.__set_num_of_columns_from_file(1);
    ranges.push_back(range);

    auto scanner = create_csv_scanner({t}, ranges);
    EXPECT_NE(scanner, nullptr);

    Status st;

    st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();
    scanner->use_v2(_use_v2);
    auto res = scanner->get_next();
    ASSERT_TRUE(res.ok()) << res.status().to_string();
    auto chunk = res.value();
    EXPECT_EQ(1, chunk->num_columns());
    EXPECT_EQ(5, chunk->num_rows());

    // []
    EXPECT_EQ(0, chunk->get(0)[0].get_array().size());

    // [null]
    EXPECT_EQ(1, chunk->get(1)[0].get_array().size());
    EXPECT_TRUE(chunk->get(1)[0].get_array()[0].is_null());

    // \N
    EXPECT_TRUE(chunk->get(2)[0].is_null());

    // ["2020-01-01","2021-01-01"]
    EXPECT_EQ(2, chunk->get(3)[0].get_array().size());
    EXPECT_EQ("2020-01-01", chunk->get(3)[0].get_array()[0].get_date().to_string());
    EXPECT_EQ("2021-01-01", chunk->get(3)[0].get_array()[1].get_date().to_string());

    // ["2022-01-01",null]
    EXPECT_EQ(2, chunk->get(4)[0].get_array().size());
    EXPECT_EQ("2022-01-01", chunk->get(4)[0].get_array()[0].get_date().to_string());
    EXPECT_TRUE(chunk->get(4)[0].get_array()[1].is_null());
}

TEST_P(CSVScannerTest, test_nested_array_of_int) {
    TypeDescriptor t(TYPE_ARRAY);
    t.children.emplace_back(TYPE_ARRAY);
    t.children.back().children.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_path("./be/test/exec/test_data/csv_scanner/csv_file6");
    range.__set_start_offset(0);
    range.__set_num_of_columns_from_file(1);
    ranges.push_back(range);

    auto scanner = create_csv_scanner({t}, ranges);
    EXPECT_NE(scanner, nullptr);

    Status st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();
    scanner->use_v2(_use_v2);
    ChunkPtr chunk = scanner->get_next().value();

    EXPECT_EQ(8, chunk->num_rows());

    // []
    EXPECT_EQ(0, chunk->get(0)[0].get_array().size());

    // [[]]
    EXPECT_EQ(1, chunk->get(1)[0].get_array().size());
    EXPECT_EQ(0, chunk->get(1)[0].get_array()[0].get_array().size());

    // \N
    EXPECT_TRUE(chunk->get(2)[0].is_null());

    // [[1,2,3]]
    EXPECT_EQ(1, chunk->get(3)[0].get_array().size());
    //  -> [1,2,3]
    EXPECT_EQ(3, chunk->get(3)[0].get_array()[0].get_array().size());
    EXPECT_EQ(1, chunk->get(3)[0].get_array()[0].get_array()[0].get_int32());
    EXPECT_EQ(2, chunk->get(3)[0].get_array()[0].get_array()[1].get_int32());
    EXPECT_EQ(3, chunk->get(3)[0].get_array()[0].get_array()[2].get_int32());

    // [[1],[2],[3]]
    EXPECT_EQ(3, chunk->get(4)[0].get_array().size());
    //  -> [1]
    EXPECT_EQ(1, chunk->get(4)[0].get_array()[0].get_array().size());
    EXPECT_EQ(1, chunk->get(4)[0].get_array()[0].get_array()[0].get_int32());
    //  -> [2]
    EXPECT_EQ(1, chunk->get(4)[0].get_array()[1].get_array().size());
    EXPECT_EQ(2, chunk->get(4)[0].get_array()[1].get_array()[0].get_int32());
    //  -> [3]
    EXPECT_EQ(1, chunk->get(4)[0].get_array()[2].get_array().size());
    EXPECT_EQ(3, chunk->get(4)[0].get_array()[2].get_array()[0].get_int32());

    // [[1,2],[3]]
    EXPECT_EQ(2, chunk->get(5)[0].get_array().size());
    //  -> [1,2]
    EXPECT_EQ(2, chunk->get(5)[0].get_array()[0].get_array().size());
    EXPECT_EQ(1, chunk->get(5)[0].get_array()[0].get_array()[0].get_int32());
    EXPECT_EQ(2, chunk->get(5)[0].get_array()[0].get_array()[1].get_int32());
    //  -> [3]
    EXPECT_EQ(1, chunk->get(5)[0].get_array()[1].get_array().size());
    EXPECT_EQ(3, chunk->get(5)[0].get_array()[1].get_array()[0].get_int32());

    // [null]
    EXPECT_EQ(1, chunk->get(6)[0].get_array().size());
    EXPECT_TRUE(chunk->get(6)[0].get_array()[0].is_null());

    // [[null]]
    EXPECT_EQ(1, chunk->get(7)[0].get_array().size());
    EXPECT_EQ(1, chunk->get(7)[0].get_array()[0].get_array().size());
    EXPECT_TRUE(chunk->get(7)[0].get_array()[0].get_array()[0].is_null());
}

TEST_P(CSVScannerTest, test_invalid_field_as_null) {
    std::vector<TypeDescriptor> types{TypeDescriptor(TYPE_INT)};

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_path("./be/test/exec/test_data/csv_scanner/csv_file7");
    range.__set_start_offset(0);
    range.__set_num_of_columns_from_file(types.size());
    ranges.push_back(range);

    auto scanner = create_csv_scanner({types}, ranges);
    EXPECT_NE(scanner, nullptr);

    Status st;

    st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    scanner->use_v2(_use_v2);

    ChunkPtr chunk = scanner->get_next().value();
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(3, chunk->num_rows());

    EXPECT_TRUE(chunk->get(0)[0].is_null());
    EXPECT_TRUE(chunk->get(1)[0].is_null());
    EXPECT_TRUE(chunk->get(2)[0].is_null());
}

TEST_P(CSVScannerTest, test_invalid_field_of_array_as_null) {
    std::vector<TypeDescriptor> types{TypeDescriptor(TYPE_ARRAY)};
    types[0].children.emplace_back(TYPE_INT);

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_path("./be/test/exec/test_data/csv_scanner/csv_file8");
    range.__set_start_offset(0);
    range.__set_num_of_columns_from_file(types.size());
    ranges.push_back(range);

    auto scanner = create_csv_scanner(types, ranges);
    EXPECT_NE(scanner, nullptr);

    Status st;

    st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    scanner->use_v2(_use_v2);

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(3, chunk->num_rows());

    EXPECT_EQ(1, chunk->get(0)[0].is_null());
    EXPECT_EQ(1, chunk->get(1)[0].is_null());
    EXPECT_EQ(1, chunk->get(2)[0].is_null());
}

TEST_P(CSVScannerTest, test_start_offset) {
    std::vector<TypeDescriptor> types{TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_INT)};

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_num_of_columns_from_file(2);
    range.__set_start_offset(4);
    range.__set_size(10);
    range.__set_path("./be/test/exec/test_data/csv_scanner/csv_file9");
    ranges.push_back(range);

    auto scanner = create_csv_scanner(types, ranges);
    Status st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    scanner->use_v2(_use_v2);

    ChunkPtr chunk = scanner->get_next().value();
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(2, chunk->num_rows());

    EXPECT_EQ(5, chunk->get(0)[0].get_int32());
    EXPECT_EQ(7, chunk->get(1)[0].get_int32());

    EXPECT_EQ(6, chunk->get(0)[1].get_int32());
    EXPECT_EQ(8, chunk->get(1)[1].get_int32());
}

TEST_P(CSVScannerTest, test_skip_header) {
    std::vector<TypeDescriptor> types{TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_INT)};

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_num_of_columns_from_file(2);
    range.__set_path("./be/test/exec/test_data/csv_scanner/csv_file15");
    ranges.push_back(range);

    auto scanner = create_csv_scanner(types, ranges, "\n", "|", 4);
    Status st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    scanner->use_v2(_use_v2);

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(5, chunk->num_rows());

    EXPECT_EQ(1, chunk->get(0)[0].get_int32());
    EXPECT_EQ(3, chunk->get(1)[0].get_int32());
    EXPECT_EQ(5, chunk->get(2)[0].get_int32());
    EXPECT_EQ(7, chunk->get(3)[0].get_int32());
    EXPECT_EQ(9, chunk->get(4)[0].get_int32());

    EXPECT_EQ(2, chunk->get(0)[1].get_int32());
    EXPECT_EQ(4, chunk->get(1)[1].get_int32());
    EXPECT_EQ(6, chunk->get(2)[1].get_int32());
    EXPECT_EQ(8, chunk->get(3)[1].get_int32());
    EXPECT_EQ(0, chunk->get(4)[1].get_int32());
}

TEST_P(CSVScannerTrimSpaceTest, test_trim_space) {
    std::vector<TypeDescriptor> types{TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_VARCHAR)};

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_num_of_columns_from_file(2);
    range.__set_path("./be/test/exec/test_data/csv_scanner/csv_file16");
    ranges.push_back(range);

    auto scanner = create_csv_scanner(types, ranges, "\n", "|", 0, true, '"');
    Status st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    scanner->use_v2(_use_v2);

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(2, chunk->num_rows());

    EXPECT_EQ(1, chunk->get(0)[0].get_int32());
    EXPECT_EQ(3, chunk->get(1)[0].get_int32());

    EXPECT_EQ("aa  ", chunk->get(0)[1].get_slice());
    EXPECT_EQ(" bb", chunk->get(1)[1].get_slice());
}

TEST_P(CSVScannerTrimSpaceTest, test_trim_space_with_ENCLOSE) {
    std::vector<TypeDescriptor> types{TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_INT)};

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_num_of_columns_from_file(3);
    range.__set_path("./be/test/exec/test_data/csv_scanner/csv_file19");
    ranges.push_back(range);

    auto scanner = create_csv_scanner(types, ranges, "\n", ",", 0, true, '\'', '\\');
    Status st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(4, chunk->num_rows());

    EXPECT_EQ(1, chunk->get(0)[0].get_int32());
    EXPECT_EQ(2, chunk->get(1)[0].get_int32());
    EXPECT_EQ(3, chunk->get(2)[0].get_int32());
    EXPECT_EQ(4, chunk->get(3)[0].get_int32());

    EXPECT_EQ("  Lily,  asdf\n\n\nsafsdfaasfsdfa23'1111111  ", chunk->get(0)[1].get_slice());
    EXPECT_EQ(" Ro 'se", chunk->get(1)[1].get_slice());
    EXPECT_EQ("Al  i   ce", chunk->get(2)[1].get_slice());
    EXPECT_EQ("Julia", chunk->get(3)[1].get_slice());

    EXPECT_EQ(24, chunk->get(0)[2].get_int32());
    EXPECT_EQ(23, chunk->get(1)[2].get_int32());
    EXPECT_EQ(24, chunk->get(2)[2].get_int32());
    EXPECT_EQ(25, chunk->get(3)[2].get_int32());
}

TEST_P(CSVScannerTest, test_ENCLOSE) {
    std::vector<TypeDescriptor> types{TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_VARCHAR),
                                      TypeDescriptor(TYPE_VARCHAR)};

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_num_of_columns_from_file(3);
    range.__set_path("./be/test/exec/test_data/csv_scanner/csv_file17");
    ranges.push_back(range);

    auto scanner = create_csv_scanner(types, ranges, "\n", "|", 0, true, '"', '\\');
    Status st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(7, chunk->num_rows());

    EXPECT_EQ(1, chunk->get(0)[0].get_int32());
    EXPECT_EQ(3, chunk->get(1)[0].get_int32());
    EXPECT_EQ(5, chunk->get(2)[0].get_int32());
    EXPECT_EQ(7, chunk->get(3)[0].get_int32());
    EXPECT_EQ(9, chunk->get(4)[0].get_int32());
    EXPECT_EQ(11, chunk->get(5)[0].get_int32());
    EXPECT_EQ(13, chunk->get(6)[0].get_int32());

    EXPECT_EQ("aa", chunk->get(0)[1].get_slice());
    EXPECT_EQ("bb|BB", chunk->get(1)[1].get_slice());
    EXPECT_EQ("cc\nadf,1,3455", chunk->get(2)[1].get_slice());
    EXPECT_EQ("dd", chunk->get(3)[1].get_slice());
    EXPECT_EQ("\"ee\"", chunk->get(4)[1].get_slice());
    EXPECT_EQ("", chunk->get(5)[1].get_slice());
    EXPECT_EQ("\"cd\"", chunk->get(6)[1].get_slice());

    EXPECT_EQ("abc", chunk->get(0)[2].get_slice());
    EXPECT_EQ("", chunk->get(1)[2].get_slice());
    EXPECT_EQ("e", chunk->get(2)[2].get_slice());
    EXPECT_EQ("abc|ef\ngh", chunk->get(3)[2].get_slice());
    EXPECT_EQ("", chunk->get(4)[2].get_slice());
    EXPECT_EQ("ab", chunk->get(5)[2].get_slice());
    EXPECT_EQ("ab\"c", chunk->get(6)[2].get_slice());
}

TEST_P(CSVScannerTest, test_ESCAPE) {
    std::vector<TypeDescriptor> types{TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_VARCHAR),
                                      TypeDescriptor(TYPE_VARCHAR)};

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_num_of_columns_from_file(3);
    range.__set_path("./be/test/exec/test_data/csv_scanner/csv_file18");
    ranges.push_back(range);

    auto scanner = create_csv_scanner(types, ranges, "\n", "|", 0, true, '"', '\\');
    Status st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(5, chunk->num_rows());

    EXPECT_EQ(1, chunk->get(0)[0].get_int32());
    EXPECT_EQ(3, chunk->get(1)[0].get_int32());
    EXPECT_EQ(5, chunk->get(2)[0].get_int32());
    EXPECT_EQ(7, chunk->get(3)[0].get_int32());
    EXPECT_EQ(9, chunk->get(4)[0].get_int32());

    EXPECT_EQ("\"aa\"", chunk->get(0)[1].get_slice());
    EXPECT_EQ("bb|BB", chunk->get(1)[1].get_slice());
    EXPECT_EQ("cc\nadf,1,3455", chunk->get(2)[1].get_slice());
    EXPECT_EQ("dd", chunk->get(3)[1].get_slice());
    EXPECT_EQ("\\ee", chunk->get(4)[1].get_slice());

    EXPECT_EQ("abc\"", chunk->get(0)[2].get_slice());
    EXPECT_EQ("", chunk->get(1)[2].get_slice());
    EXPECT_EQ("\"e", chunk->get(2)[2].get_slice());
    EXPECT_EQ("abc|ef\ngh", chunk->get(3)[2].get_slice());
    EXPECT_EQ("", chunk->get(4)[2].get_slice());
}

TEST_P(CSVScannerTest, TEST_Pile_not_ended_with_record_delimiter) {
    std::vector<TypeDescriptor> types{TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_INT)};

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_start_offset(0);
    range.__set_num_of_columns_from_file(types.size());
    range.__set_path("./be/test/exec/test_data/csv_scanner/csv_file10");
    ranges.push_back(range);

    auto scanner = create_csv_scanner(types, ranges);
    Status st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    scanner->use_v2(_use_v2);

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(5, chunk->num_rows());

    EXPECT_EQ(1, chunk->get(0)[0].get_int32());
    EXPECT_EQ(3, chunk->get(1)[0].get_int32());
    EXPECT_EQ(5, chunk->get(2)[0].get_int32());
    EXPECT_EQ(7, chunk->get(3)[0].get_int32());
    EXPECT_EQ(9, chunk->get(4)[0].get_int32());

    EXPECT_EQ(2, chunk->get(0)[1].get_int32());
    EXPECT_EQ(4, chunk->get(1)[1].get_int32());
    EXPECT_EQ(6, chunk->get(2)[1].get_int32());
    EXPECT_EQ(8, chunk->get(3)[1].get_int32());
    EXPECT_EQ(0, chunk->get(4)[1].get_int32());
}

TEST_P(CSVScannerTest, test_large_record_size) {
    constexpr size_t record_length = 65533 * 5;
    constexpr size_t field_length = 65533;
    constexpr size_t field_count = (record_length + field_length - 1) / field_length;

    TypeDescriptor large_varchar_type;
    large_varchar_type.type = TYPE_VARCHAR;
    large_varchar_type.len = field_length;

    std::vector<TypeDescriptor> types(field_count, large_varchar_type);

    constexpr int kNumRecords = 5;

    // Construct kNumRecords records, each record contains |field_count| fields and each field
    // starts with 'x' and ends with two digit suffix.
    std::stringstream ss;
    std::string csv_field(field_length, 'x');
    for (int i = 0; i < kNumRecords; i++) {
        csv_field[csv_field.size() - 2] = '0' + (i % 10);
        for (int j = 0; j < field_count; j++) {
            csv_field[csv_field.size() - 1] = '0' + (j % 10);
            ss << csv_field << (j == field_count - 1 ? '\n' : '|');
        }
    }
    std::string csv_content = ss.str();
    ss.clear();

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_path("./be/test/exec/test_data/csv_scanner/csv_file11");
    range.__set_start_offset(0);
    range.__set_num_of_columns_from_file(types.size());
    ranges.push_back(range);

    auto scanner = create_csv_scanner(types, ranges);
    EXPECT_NE(scanner, nullptr);

    Status st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    scanner->use_v2(_use_v2);

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(kNumRecords, chunk->num_rows());
    EXPECT_EQ(field_count, chunk->num_columns());
    for (int col = 0; col < chunk->num_columns(); col++) {
        const auto& column = chunk->get_column_by_index(col);
        for (int row = 0; row < chunk->num_rows(); row++) {
            auto datum = column->get(row);
            auto s = datum.get_slice();
            ASSERT_EQ(field_length, s.size);
            ASSERT_EQ(row % 10, s[s.size - 2] - '0');
            ASSERT_EQ(col % 10, s[s.size - 1] - '0');
        }
    }
}

TEST_P(CSVScannerTest, test_record_length_exceed_limit) {
    constexpr size_t record_length = TypeDescriptor::MAX_VARCHAR_LENGTH;
    constexpr size_t field_length = TypeDescriptor::MAX_VARCHAR_LENGTH;
    constexpr size_t field_count = (record_length + field_length - 1) / field_length;

    TypeDescriptor large_varchar_type;
    large_varchar_type.type = TYPE_VARCHAR;
    large_varchar_type.len = field_length;

    std::vector<TypeDescriptor> types(field_count, large_varchar_type);

    // Construct 1 record with |field_count| fixed-length fields and the
    // total record length is greater than |TypeDescriptor::MAX_VARCHAR_LENGTH|.
    std::stringstream ss;
    std::string csv_field(field_length, 'x');
    for (int i = 0; i < field_count; i++) {
        ss << csv_field << (i == field_count - 1 ? '\n' : '|');
    }
    std::string csv_content = ss.str();
    ss.clear();

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_path("./be/test/exec/test_data/csv_scanner/csv_file12");
    range.__set_start_offset(0);
    range.__set_num_of_columns_from_file(types.size());
    ranges.push_back(range);

    auto scanner = create_csv_scanner(types, ranges);
    EXPECT_NE(scanner, nullptr);

    Status st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    scanner->use_v2(_use_v2);

    auto res = scanner->get_next();
    EXPECT_TRUE(!res.ok());
}

TEST_P(CSVScannerTest, test_empty) {
    auto run_test = [this](LogicalType lt) {
        std::vector<TypeDescriptor> types{TypeDescriptor(lt)};
        if (lt == TYPE_VARCHAR || lt == TYPE_CHAR) {
            types[0].len = 10;
        }
        std::vector<TBrokerRangeDesc> ranges;
        TBrokerRangeDesc range;
        range.__set_start_offset(0);
        range.__set_path("./be/test/exec/test_data/csv_scanner/csv_file13");
        range.__set_num_of_columns_from_file(types.size());
        ranges.push_back(range);

        auto scanner = create_csv_scanner(types, ranges);
        ASSERT_TRUE(scanner->open().ok());
        scanner->use_v2(_use_v2);
        auto res = scanner->get_next();
        ASSERT_TRUE(res.status().is_end_of_file());
    };
    run_test(TYPE_VARCHAR);
    run_test(TYPE_CHAR);
    run_test(TYPE_INT);
    run_test(TYPE_DATE);
    run_test(TYPE_DATETIME);
}

// 21431,"Rowdy" Roddy Piper, Superstar,,,,1,-999
TEST_P(CSVScannerTest, test_enclose_fanatics) {
    std::vector<TypeDescriptor> types{TypeDescriptor(TYPE_INT),     TypeDescriptor(TYPE_VARCHAR),
                                      TypeDescriptor(TYPE_VARCHAR), TypeDescriptor(TYPE_INT),
                                      TypeDescriptor(TYPE_INT),     TypeDescriptor(TYPE_INT),
                                      TypeDescriptor(TYPE_INT),     TypeDescriptor(TYPE_INT)};

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.__set_num_of_columns_from_file(types.size());
    range.__set_path("./be/test/exec/test_data/csv_scanner/csv_file22");
    ranges.push_back(range);

    auto scanner = create_csv_scanner(types, ranges, "\n", ",", 0, true, '"', '\\');
    Status st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    ChunkPtr chunk = scanner->get_next().value();
    EXPECT_EQ(1, chunk->num_rows());
    EXPECT_EQ(8, chunk->num_columns());
    EXPECT_EQ(21431, chunk->get(0)[0].get_int32());
    EXPECT_EQ("\"Rowdy\" Roddy Piper", chunk->get(0)[1].get_slice());
    EXPECT_EQ("Superstar", chunk->get(0)[2].get_slice());
    EXPECT_TRUE(chunk->get(0)[3].is_null());
    EXPECT_TRUE(chunk->get(0)[4].is_null());
    EXPECT_TRUE(chunk->get(0)[5].is_null());
    EXPECT_EQ(1, chunk->get(0)[6].get_int32());
    EXPECT_EQ(-999, chunk->get(0)[7].get_int32());
}

TEST_P(CSVScannerTest, test_column_count_inconsistent) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_DOUBLE);
    types.emplace_back(TYPE_VARCHAR);
    types.emplace_back(TYPE_DATE);

    types[2].len = 10;

    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range_one;
    range_one.__set_path("./be/test/exec/test_data/csv_scanner/csv_file1");
    range_one.__set_start_offset(0);
    range_one.__set_num_of_columns_from_file(types.size());
    ranges.push_back(range_one);

    auto scanner = create_csv_scanner(types, ranges);
    EXPECT_NE(scanner, nullptr);

    auto st = scanner->open();
    ASSERT_TRUE(st.ok()) << st.to_string();

    scanner->use_v2(_use_v2);

    auto log_file_path = "test_column_count_inconsistent_error_log_file";
    std::ofstream wfile(log_file_path, std::ofstream::out);
    scanner->TEST_runtime_state()->_error_log_file = &wfile;
    auto res = scanner->get_next();
    ASSERT_TRUE(res.status().is_end_of_file()) << res.status().to_string();
    wfile.close();
    scanner->TEST_runtime_state()->_error_log_file = nullptr;

    std::ifstream rfile(log_file_path, std::ifstream::in);
    std::string line;
    line.resize(1024);
    rfile.getline(line.data(), line.size());
    auto found = line.find("Value count does not match column count. Expect 4, but got 5");
    ASSERT_TRUE(found != std::string::npos);
    rfile.close();

    (void)fs::remove(log_file_path);
}

INSTANTIATE_TEST_CASE_P(CSVScannerTestParams, CSVScannerTest, Values(true, false));
INSTANTIATE_TEST_CASE_P(CSVScannerTestParams, CSVScannerTrimSpaceTest, Values(true));

} // namespace starrocks
