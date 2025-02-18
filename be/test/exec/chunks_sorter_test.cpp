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

#include "exec/chunks_sorter.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <memory>
#include <string_view>

#include "column/column_helper.h"
#include "column/datum.h"
#include "column/datum_tuple.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "exec/chunks_sorter_full_sort.h"
#include "exec/chunks_sorter_topn.h"
#include "exec/sorting/sort_helper.h"
#include "exec/sorting/sort_permute.h"
#include "exec/sorting/sorting.h"
#include "exprs/column_ref.h"
#include "fmt/core.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "testutil/assert.h"
#include "testutil/column_test_helper.h"
#include "util/json.h"

namespace starrocks {

class SortRuntimeFilterBuilderTest : public ::testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}

protected:
    ObjectPool _pool;
    detail::SortRuntimeFilterBuilder _builder;
};

TEST_F(SortRuntimeFilterBuilderTest, null_first_all_null_single_column) {
    auto data = std::vector<int32_t>{0, 0, 0, 0, 0};
    auto null_data = std::vector<uint8_t>{1, 1, 1, 1, 1};
    ColumnPtr column = ColumnTestHelper::build_nullable_column(data, null_data);

    int row_id = 4;
    bool asc = true;
    bool null_first = true;
    bool is_close_interval = false;

    auto* rf = _builder.template operator()<TYPE_INT>(&_pool, column, row_id, asc, null_first, is_close_interval);
    ASSERT_TRUE(rf != nullptr);
    auto* bf = reinterpret_cast<MinMaxRuntimeFilter<TYPE_INT>*>(rf);
    ASSERT_EQ(bf->debug_string(),
              "RuntimeMinMax(type = 5 has_null = 0 _min = 2147483647, _max = -2147483648 left_close_interval = 1, "
              "right_close_interval = 1 )");
}

TEST_F(SortRuntimeFilterBuilderTest, null_first_all_null_multi_column) {
    auto data = std::vector<int32_t>{0, 0, 0, 0, 0};
    auto null_data = std::vector<uint8_t>{1, 1, 1, 1, 1};
    ColumnPtr column = ColumnTestHelper::build_nullable_column(data, null_data);

    int row_id = 4;
    bool asc = true;
    bool null_first = true;
    bool is_close_interval = true;

    auto* rf = _builder.template operator()<TYPE_INT>(&_pool, column, row_id, asc, null_first, is_close_interval);
    ASSERT_TRUE(rf != nullptr);
    auto* bf = reinterpret_cast<MinMaxRuntimeFilter<TYPE_INT>*>(rf);
    ASSERT_EQ(bf->debug_string(),
              "RuntimeMinMax(type = 5 has_null = 1 _min = 2147483647, _max = -2147483648 left_close_interval = 1, "
              "right_close_interval = 1 )");
}

TEST_F(SortRuntimeFilterBuilderTest, null_first_partial_null_asc) {
    auto data = std::vector<int32_t>{0, 0, 10, 11, 12};
    auto null_data = std::vector<uint8_t>{1, 1, 0, 0, 0};
    ColumnPtr column = ColumnTestHelper::build_nullable_column(data, null_data);

    int row_id = 4;
    bool asc = true;
    bool null_first = true;
    bool is_close_interval = true;

    auto* rf = _builder.template operator()<TYPE_INT>(&_pool, column, row_id, asc, null_first, is_close_interval);
    ASSERT_TRUE(rf != nullptr);
    auto* bf = reinterpret_cast<MinMaxRuntimeFilter<TYPE_INT>*>(rf);
    ASSERT_EQ(bf->debug_string(),
              "RuntimeMinMax(type = 5 has_null = 1 _min = -2147483648, _max = 12 left_close_interval = 1, "
              "right_close_interval = 1 )");
}

TEST_F(SortRuntimeFilterBuilderTest, null_first_partial_null_desc) {
    auto data = std::vector<int32_t>{0, 0, 12, 11, 10};
    auto null_data = std::vector<uint8_t>{1, 1, 0, 0, 0};
    ColumnPtr column = ColumnTestHelper::build_nullable_column(data, null_data);

    int row_id = 4;
    bool asc = false;
    bool null_first = true;
    bool is_close_interval = true;

    auto* rf = _builder.template operator()<TYPE_INT>(&_pool, column, row_id, asc, null_first, is_close_interval);
    ASSERT_TRUE(rf != nullptr);
    auto* bf = reinterpret_cast<MinMaxRuntimeFilter<TYPE_INT>*>(rf);
    ASSERT_EQ(bf->debug_string(),
              "RuntimeMinMax(type = 5 has_null = 1 _min = 10, _max = 2147483647 left_close_interval = 1, "
              "right_close_interval = 1 )");
}

TEST_F(SortRuntimeFilterBuilderTest, null_last_has_null_single_column) {
    auto data = std::vector<int32_t>{0, 0, 0, 0, 0};
    auto null_data = std::vector<uint8_t>{1, 1, 1, 1, 1};
    ColumnPtr column = ColumnTestHelper::build_nullable_column(data, null_data);

    int row_id = 4;
    bool asc = true;
    bool null_first = false;
    bool is_close_interval = false;

    auto* rf = _builder.template operator()<TYPE_INT>(&_pool, column, row_id, asc, null_first, is_close_interval);
    ASSERT_TRUE(rf != nullptr);
    auto* bf = reinterpret_cast<MinMaxRuntimeFilter<TYPE_INT>*>(rf);
    ASSERT_EQ(bf->debug_string(),
              "RuntimeMinMax(type = 5 has_null = 0 _min = -2147483648, _max = 2147483647 left_close_interval = 1, "
              "right_close_interval = 1 )");
}

TEST_F(SortRuntimeFilterBuilderTest, null_last_has_null_multi_column) {
    auto data = std::vector<int32_t>{0, 0, 0, 0, 0};
    auto null_data = std::vector<uint8_t>{1, 1, 1, 1, 1};
    ColumnPtr column = ColumnTestHelper::build_nullable_column(data, null_data);

    int row_id = 4;
    bool asc = true;
    bool null_first = false;
    bool is_close_interval = true;

    auto* rf = _builder.template operator()<TYPE_INT>(&_pool, column, row_id, asc, null_first, is_close_interval);
    ASSERT_TRUE(rf == nullptr);
}

TEST_F(SortRuntimeFilterBuilderTest, null_first_has_no_null_asc) {
    auto data = std::vector<int32_t>{10, 11, 12, 13, 14};
    auto null_data = std::vector<uint8_t>{0, 0, 0, 0, 0};
    ColumnPtr column = ColumnTestHelper::build_nullable_column(data, null_data);

    int row_id = 4;
    bool asc = true;
    bool null_first = true;
    bool is_close_interval = true;

    auto* rf = _builder.template operator()<TYPE_INT>(&_pool, column, row_id, asc, null_first, is_close_interval);
    ASSERT_TRUE(rf != nullptr);
    auto* bf = reinterpret_cast<MinMaxRuntimeFilter<TYPE_INT>*>(rf);
    ASSERT_EQ(bf->debug_string(),
              "RuntimeMinMax(type = 5 has_null = 1 _min = -2147483648, _max = 14 left_close_interval = 1, "
              "right_close_interval = 1 )");
}

TEST_F(SortRuntimeFilterBuilderTest, null_last_has_no_null_desc) {
    auto data = std::vector<int32_t>{14, 13, 12, 12, 11};
    auto null_data = std::vector<uint8_t>{0, 0, 0, 0, 0};
    ColumnPtr column = ColumnTestHelper::build_nullable_column(data, null_data);

    int row_id = 4;
    bool asc = false;
    bool null_first = false;
    bool is_close_interval = true;

    auto* rf = _builder.template operator()<TYPE_INT>(&_pool, column, row_id, asc, null_first, is_close_interval);
    ASSERT_TRUE(rf != nullptr);
    auto* bf = reinterpret_cast<MinMaxRuntimeFilter<TYPE_INT>*>(rf);
    ASSERT_EQ(bf->debug_string(),
              "RuntimeMinMax(type = 5 has_null = 0 _min = 11, _max = 2147483647 left_close_interval = 1, "
              "right_close_interval = 1 )");
}

class SortRuntimeFilterUpdaterTest : public ::testing::Test {
public:
    void SetUp() override {
        _init_no_null_rf_for_asc_null_first();
        _init_no_null_rf_for_asc_null_last();
        _init_no_null_rf_for_desc();

        _init_all_null_rf_for_null_first();
        _init_all_null_rf_for_null_last();
    }
    void TearDown() override {}

protected:
    void _init_no_null_rf_for_asc_null_first() {
        auto data = std::vector<int32_t>{10, 11, 12, 13, 14};
        auto null_data = std::vector<uint8_t>{0, 0, 0, 0, 0};
        ColumnPtr column = ColumnTestHelper::build_nullable_column(data, null_data);

        int row_id = 4;
        bool asc = true;
        bool null_first = true;
        bool is_close_interval = true;

        auto* rf = _builder.template operator()<TYPE_INT>(&_pool, column, row_id, asc, null_first, is_close_interval);
        ASSERT_TRUE(rf != nullptr);
        _no_null_rf_asc_for_null_first = reinterpret_cast<MinMaxRuntimeFilter<TYPE_INT>*>(rf);
    }

    void _init_no_null_rf_for_asc_null_last() {
        auto data = std::vector<int32_t>{10, 11, 12, 13, 14};
        auto null_data = std::vector<uint8_t>{0, 0, 0, 0, 0};
        ColumnPtr column = ColumnTestHelper::build_nullable_column(data, null_data);

        int row_id = 4;
        bool asc = true;
        bool null_first = false;
        bool is_close_interval = true;

        auto* rf = _builder.template operator()<TYPE_INT>(&_pool, column, row_id, asc, null_first, is_close_interval);
        ASSERT_TRUE(rf != nullptr);
        _no_null_rf_asc_for_null_last = reinterpret_cast<MinMaxRuntimeFilter<TYPE_INT>*>(rf);
    }

    void _init_no_null_rf_for_desc() {
        auto data = std::vector<int32_t>{14, 13, 12, 11, 10};
        auto null_data = std::vector<uint8_t>{0, 0, 0, 0, 0};
        ColumnPtr column = ColumnTestHelper::build_nullable_column(data, null_data);

        int row_id = 4;
        bool asc = false;
        bool null_first = false;
        bool is_close_interval = true;

        auto* rf = _builder.template operator()<TYPE_INT>(&_pool, column, row_id, asc, null_first, is_close_interval);
        ASSERT_TRUE(rf != nullptr);
        _no_null_rf_desc = reinterpret_cast<MinMaxRuntimeFilter<TYPE_INT>*>(rf);
    }

    void _init_all_null_rf_for_null_first() {
        auto data = std::vector<int32_t>{0, 0, 0, 0, 0};
        auto null_data = std::vector<uint8_t>{1, 1, 1, 1, 1};
        ColumnPtr column = ColumnTestHelper::build_nullable_column(data, null_data);

        int row_id = 4;
        bool asc = true;
        bool null_first = true;
        bool is_close_interval = false;

        auto* rf = _builder.template operator()<TYPE_INT>(&_pool, column, row_id, asc, null_first, is_close_interval);
        ASSERT_TRUE(rf != nullptr);
        _all_null_rf_for_null_first = reinterpret_cast<MinMaxRuntimeFilter<TYPE_INT>*>(rf);
    }

    void _init_all_null_rf_for_null_last() {
        auto data = std::vector<int32_t>{0, 0, 0, 0, 0};
        auto null_data = std::vector<uint8_t>{1, 1, 1, 1, 1};
        ColumnPtr column = ColumnTestHelper::build_nullable_column(data, null_data);

        int row_id = 4;
        bool asc = true;
        bool null_first = false;
        bool is_close_interval = false;

        auto* rf = _builder.template operator()<TYPE_INT>(&_pool, column, row_id, asc, null_first, is_close_interval);
        ASSERT_TRUE(rf != nullptr);
        _all_null_rf_for_null_last = reinterpret_cast<MinMaxRuntimeFilter<TYPE_INT>*>(rf);
    }

    ObjectPool _pool;
    detail::SortRuntimeFilterBuilder _builder;
    detail::SortRuntimeFilterUpdater _updater;
    MinMaxRuntimeFilter<TYPE_INT>* _no_null_rf_asc_for_null_first;
    MinMaxRuntimeFilter<TYPE_INT>* _no_null_rf_asc_for_null_last;
    MinMaxRuntimeFilter<TYPE_INT>* _no_null_rf_desc;
    MinMaxRuntimeFilter<TYPE_INT>* _all_null_rf_for_null_first;
    MinMaxRuntimeFilter<TYPE_INT>* _all_null_rf_for_null_last;
};

TEST_F(SortRuntimeFilterUpdaterTest, null_first_single_column_update_to_all_null) {
    auto data = std::vector<int32_t>{0, 0, 0, 0, 0};
    auto null_data = std::vector<uint8_t>{1, 1, 1, 1, 1};
    ColumnPtr column = ColumnTestHelper::build_nullable_column(data, null_data);

    int row_id = 4;
    bool asc = true;
    bool null_first = true;
    bool is_close_interval = false;

    _updater.template operator()<TYPE_INT>(_no_null_rf_asc_for_null_first, column, row_id, asc, null_first,
                                           is_close_interval);
    ASSERT_EQ(_no_null_rf_asc_for_null_first->debug_string(),
              "RuntimeMinMax(type = 5 has_null = 0 _min = 2147483647, _max = -2147483648 left_close_interval = 1, "
              "right_close_interval = 1 )");
}

TEST_F(SortRuntimeFilterUpdaterTest, null_first_multi_column_update_to_all_null) {
    auto data = std::vector<int32_t>{0, 0, 0, 0, 0};
    auto null_data = std::vector<uint8_t>{1, 1, 1, 1, 1};
    ColumnPtr column = ColumnTestHelper::build_nullable_column(data, null_data);

    int row_id = 4;
    bool asc = true;
    bool null_first = true;
    bool is_close_interval = true;

    _updater.template operator()<TYPE_INT>(_no_null_rf_asc_for_null_first, column, row_id, asc, null_first,
                                           is_close_interval);
    ASSERT_EQ(_no_null_rf_asc_for_null_first->debug_string(),
              "RuntimeMinMax(type = 5 has_null = 1 _min = 2147483647, _max = -2147483648 left_close_interval = 1, "
              "right_close_interval = 1 )");
}

TEST_F(SortRuntimeFilterUpdaterTest, null_first_one_column_update) {
    auto data = std::vector<int32_t>{1, 2, 3, 4, 5};
    auto null_data = std::vector<uint8_t>{0, 0, 0, 0, 0};
    ColumnPtr column = ColumnTestHelper::build_nullable_column(data, null_data);

    int row_id = 4;
    bool asc = true;
    bool null_first = true;
    bool is_close_interval = false;

    _updater.template operator()<TYPE_INT>(_no_null_rf_asc_for_null_first, column, row_id, asc, null_first,
                                           is_close_interval);
    ASSERT_EQ(_no_null_rf_asc_for_null_first->debug_string(),
              "RuntimeMinMax(type = 5 has_null = 1 _min = -2147483648, _max = 5 left_close_interval = 1, "
              "right_close_interval = 1 )");
}

TEST_F(SortRuntimeFilterUpdaterTest, null_last_one_column_all_null) {
    auto data = std::vector<int32_t>{0, 0, 0, 0, 0};
    auto null_data = std::vector<uint8_t>{1, 1, 1, 1, 1};
    ColumnPtr column = ColumnTestHelper::build_nullable_column(data, null_data);

    int row_id = 4;
    bool asc = true;
    bool null_first = false;
    bool is_close_interval = false;

    _updater.template operator()<TYPE_INT>(_all_null_rf_for_null_last, column, row_id, asc, null_first,
                                           is_close_interval);
    ASSERT_EQ(_all_null_rf_for_null_last->debug_string(),
              "RuntimeMinMax(type = 5 has_null = 0 _min = -2147483648, _max = 2147483647 left_close_interval = 1, "
              "right_close_interval = 1 )");
}

TEST_F(SortRuntimeFilterUpdaterTest, null_last_one_column_have_null) {
    auto data = std::vector<int32_t>{14, 15, 0, 0, 0};
    auto null_data = std::vector<uint8_t>{0, 0, 0, 1, 1};
    ColumnPtr column = ColumnTestHelper::build_nullable_column(data, null_data);

    int row_id = 4;
    bool asc = true;
    bool null_first = false;
    bool is_close_interval = false;

    _updater.template operator()<TYPE_INT>(_all_null_rf_for_null_last, column, row_id, asc, null_first,
                                           is_close_interval);
    ASSERT_EQ(_all_null_rf_for_null_last->debug_string(),
              "RuntimeMinMax(type = 5 has_null = 0 _min = -2147483648, _max = 2147483647 left_close_interval = 1, "
              "right_close_interval = 1 )");
}

TEST_F(SortRuntimeFilterUpdaterTest, null_first_one_column_have_no_null) {
    auto data = std::vector<int32_t>{1, 2, 3, 4, 5};
    auto null_data = std::vector<uint8_t>{0, 0, 0, 0, 0};
    ColumnPtr column = ColumnTestHelper::build_nullable_column(data, null_data);

    int row_id = 4;
    bool asc = true;
    bool null_first = true;
    bool is_close_interval = false;

    _updater.template operator()<TYPE_INT>(_no_null_rf_asc_for_null_first, column, row_id, asc, null_first,
                                           is_close_interval);
    ASSERT_EQ(_no_null_rf_asc_for_null_first->debug_string(),
              "RuntimeMinMax(type = 5 has_null = 1 _min = -2147483648, _max = 5 left_close_interval = 1, "
              "right_close_interval = 1 )");
}

TEST_F(SortRuntimeFilterUpdaterTest, null_last_one_column_have_no_null_asc) {
    auto data = std::vector<int32_t>{1, 2, 3, 4, 5};
    auto null_data = std::vector<uint8_t>{0, 0, 0, 0, 0};
    ColumnPtr column = ColumnTestHelper::build_nullable_column(data, null_data);

    int row_id = 4;
    bool asc = true;
    bool null_first = false;
    bool is_close_interval = false;

    _updater.template operator()<TYPE_INT>(_no_null_rf_asc_for_null_last, column, row_id, asc, null_first,
                                           is_close_interval);
    ASSERT_EQ(_no_null_rf_asc_for_null_last->debug_string(),
              "RuntimeMinMax(type = 5 has_null = 0 _min = -2147483648, _max = 5 left_close_interval = 1, "
              "right_close_interval = 1 )");
}

TEST_F(SortRuntimeFilterUpdaterTest, null_last_one_column_have_no_null_desc) {
    auto data = std::vector<int32_t>{25, 24, 23, 22, 21};
    auto null_data = std::vector<uint8_t>{0, 0, 0, 0, 0};
    ColumnPtr column = ColumnTestHelper::build_nullable_column(data, null_data);

    int row_id = 4;
    bool asc = false;
    bool null_first = false;
    bool is_close_interval = false;

    _updater.template operator()<TYPE_INT>(_no_null_rf_desc, column, row_id, asc, null_first, is_close_interval);
    ASSERT_EQ(_no_null_rf_desc->debug_string(),
              "RuntimeMinMax(type = 5 has_null = 0 _min = 21, _max = 2147483647 left_close_interval = 1, "
              "right_close_interval = 1 )");
}

class ChunksSorterTest : public ::testing::Test {
public:
    void SetUp() override {
        config::vector_chunk_size = 1024;

        setup_normal();
        setup_ranking();

        _runtime_state = _create_runtime_state();
    }

    void TearDown() override {}

    void setup_normal() {
        auto null_1 = std::vector<uint8_t>{0, 0, 0, 0, 0, 1};
        auto data_cust_key_1 = std::vector<int32_t>{2, 12, 41, 54, 58, 71};
        auto data_nation_1 = std::vector<Slice>{"JORDAN", "JORDAN", "IRAN", "EGYPT", "JORDAN", ""};
        auto data_region_1 =
                std::vector<Slice>{"MIDDLE EAST", "MIDDLE EAST", "MIDDLE EAST", "MIDDLE EAST", "MIDDLE EAST", ""};
        auto data_mkt_sgmt_1 =
                std::vector<Slice>{"AUTOMOBILE", "HOUSEHOLD", "HOUSEHOLD", "AUTOMOBILE", "HOUSEHOLD", ""};

        ColumnPtr col_cust_key_1 = ColumnTestHelper::build_column(data_cust_key_1);
        ColumnPtr col_nation_1 = ColumnTestHelper::build_nullable_column(data_nation_1, null_1);
        ColumnPtr col_region_1 = ColumnTestHelper::build_nullable_column(data_region_1, null_1);
        ColumnPtr col_mkt_sgmt_1 = ColumnTestHelper::build_nullable_column(data_mkt_sgmt_1, null_1);
        Columns columns1 = {col_cust_key_1, col_nation_1, col_region_1, col_mkt_sgmt_1};

        auto null_2 = std::vector<uint8_t>{0, 0, 0, 0, 1};
        auto data_cust_key_2 = std::vector<int32_t>{4, 16, 49, 55, 69};
        auto data_nation_key_2 = std::vector<Slice>{"EGYPT", "IRAN", "IRAN", "IRAN", "null0"};
        auto data_region_2 = std::vector<Slice>{"MIDDLE EAST", "MIDDLE EAST", "MIDDLE EAST", "MIDDLE EAST", "null0"};
        auto data_mkt_sgmt_2 = std::vector<Slice>{"MACHINERY", "FURNITURE", "FURNITURE", "MACHINERY", "null0"};

        ColumnPtr col_cust_key_2 = ColumnTestHelper::build_column(data_cust_key_2);
        ColumnPtr col_nation_2 = ColumnTestHelper::build_nullable_column(data_nation_key_2, null_2);
        ColumnPtr col_region_2 = ColumnTestHelper::build_nullable_column(data_region_2, null_2);
        ColumnPtr col_mkt_sgmt_2 = ColumnTestHelper::build_nullable_column(data_mkt_sgmt_2, null_2);
        Columns columns2 = {col_cust_key_2, col_nation_2, col_region_2, col_mkt_sgmt_2};

        auto null_3 = std::vector<uint8_t>{0, 0, 0, 0, 1};
        auto data_cust_key_3 = std::vector<int32_t>{6, 24, 52, 56, 70};
        auto data_nation_key_3 = std::vector<Slice>{"SAUDI ARABIA", "JORDAN", "IRAQ", "IRAN", "null1"};
        auto data_region_3 = std::vector<Slice>{"MIDDLE EAST", "MIDDLE EAST", "MIDDLE EAST", "MIDDLE EAST", "null1"};
        auto data_mkt_sgmt_3 = std::vector<Slice>{"AUTOMOBILE", "MACHINERY", "HOUSEHOLD", "FURNITURE", "null1"};

        ColumnPtr col_cust_key_3 = ColumnTestHelper::build_column(data_cust_key_3);
        ColumnPtr col_nation_3 = ColumnTestHelper::build_nullable_column(data_nation_key_3, null_3);
        ColumnPtr col_region_3 = ColumnTestHelper::build_nullable_column(data_region_3, null_3);
        ColumnPtr col_mkt_sgmt_3 = ColumnTestHelper::build_nullable_column(data_mkt_sgmt_3, null_3);
        Columns columns3 = {col_cust_key_3, col_nation_3, col_region_3, col_mkt_sgmt_3};

        Chunk::SlotHashMap map;
        map.reserve(columns1.size() * 2);
        for (int i = 0; i < columns1.size(); ++i) {
            map[i] = i;
        }

        _chunk_1 = std::make_shared<Chunk>(columns1, map);
        _chunk_2 = std::make_shared<Chunk>(columns2, map);
        _chunk_3 = std::make_shared<Chunk>(columns3, map);

        _expr_cust_key = std::make_unique<ColumnRef>(TypeDescriptor(TYPE_INT), 0);     // refer to cust_key
        _expr_nation = std::make_unique<ColumnRef>(TypeDescriptor(TYPE_VARCHAR), 1);   // refer to nation
        _expr_region = std::make_unique<ColumnRef>(TypeDescriptor(TYPE_VARCHAR), 2);   // refer to region
        _expr_mkt_sgmt = std::make_unique<ColumnRef>(TypeDescriptor(TYPE_VARCHAR), 3); // refer to mkt_sgmt
        _expr_constant = std::make_unique<ColumnRef>(TypeDescriptor(TYPE_SMALLINT),
                                                     4); // refer to constant value
    }

    void setup_ranking() {
        auto data_ranking1 = std::vector<int32_t>{7, 7, 7, 6, 6, 6, 5, 4, 3, 2, 1};
        auto data_ranking2 = std::vector<int32_t>{11, 12, 13, 14, 15, 16, 16, 16, 17, 17, 17};

        ColumnPtr col_ranking1 = ColumnTestHelper::build_column(data_ranking1);
        ColumnPtr col_ranking2 = ColumnTestHelper::build_column(data_ranking2);

        Columns columns1 = {col_ranking1};
        Columns columns2 = {col_ranking2};

        Chunk::SlotHashMap map;
        map.reserve(columns1.size() * 2);
        for (int i = 0; i < columns1.size(); ++i) {
            map[i] = i;
        }

        _chunk_ranking_1 = std::make_shared<Chunk>(columns1, map);
        _chunk_ranking_2 = std::make_shared<Chunk>(columns2, map);

        _expr_ranking_key = std::make_unique<ColumnRef>(TYPE_INT_DESC, 0);
    }

protected:
    std::shared_ptr<RuntimeState> _create_runtime_state() {
        TUniqueId fragment_id;
        TQueryOptions query_options;
        query_options.batch_size = config::vector_chunk_size;
        TQueryGlobals query_globals;
        auto runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, nullptr);
        runtime_state->init_instance_mem_tracker();
        return runtime_state;
    }

    std::shared_ptr<RuntimeState> _runtime_state;
    ChunkPtr _chunk_1, _chunk_2, _chunk_3;
    std::unique_ptr<ColumnRef> _expr_cust_key, _expr_nation, _expr_region, _expr_mkt_sgmt, _expr_constant;

    ChunkPtr _chunk_ranking_1, _chunk_ranking_2;
    std::unique_ptr<ColumnRef> _expr_ranking_key;
};

static void clear_sort_exprs(std::vector<ExprContext*>& exprs) {
    for (ExprContext* ctx : exprs) {
        delete ctx;
    }
    exprs.clear();
}

static Permutation make_permutation(int len) {
    Permutation perm(len);
    for (int i = 0; i < perm.size(); i++) {
        perm[i].index_in_chunk = i;
        perm[i].chunk_index = 0;
    }
    return perm;
}

[[maybe_unused]] static std::vector<ChunkPtr> consume_pages_from_sorter(ChunksSorter& sorter) {
    std::vector<ChunkPtr> result;

    bool eos = false;
    while (!eos) {
        ChunkPtr chunk;
        (void)sorter.get_next(&chunk, &eos);
        if (chunk) {
            result.push_back(chunk);
        }
    }
    return result;
}

static ChunkPtr consume_page_from_sorter(ChunksSorter& sorter) {
    ChunkPtr res;
    bool eos = false;
    while (!eos) {
        ChunkPtr chunk;
        (void)sorter.get_next(&chunk, &eos);
        if (chunk) {
            if (!res) {
                res.reset(chunk->clone_unique().release());
            } else {
                res->append(*chunk, 0, chunk->num_rows());
            }
        }
    }

    return res;
}

TEST_F(ChunksSorterTest, full_sort_incremental) {
    std::vector<bool> is_asc, is_null_first;
    is_asc.push_back(false); // cust_key
    is_asc.push_back(true);  // cust_key
    is_null_first.push_back(true);
    is_null_first.push_back(true);
    std::vector<ExprContext*> sort_exprs;
    sort_exprs.push_back(new ExprContext(_expr_region.get()));
    sort_exprs.push_back(new ExprContext(_expr_cust_key.get()));
    ASSERT_OK(Expr::prepare(sort_exprs, _runtime_state.get()));
    ASSERT_OK(Expr::open(sort_exprs, _runtime_state.get()));
    auto pool = std::make_unique<ObjectPool>();
    std::vector<SlotId> slots{_expr_region->slot_id(), _expr_cust_key->slot_id()};
    ChunksSorterFullSort sorter(_runtime_state.get(), &sort_exprs, &is_asc, &is_null_first, "", 1024000, 16777216,
                                slots);
    sorter.setup_runtime(_runtime_state.get(), pool->add(new RuntimeProfile("", false)),
                         pool->add(new MemTracker(1L << 62, "", nullptr)));
    size_t total_rows = _chunk_1->num_rows() + _chunk_2->num_rows() + _chunk_3->num_rows();
    ASSERT_OK(sorter.update(_runtime_state.get(), _chunk_1));
    ASSERT_OK(sorter.update(_runtime_state.get(), _chunk_2));
    ASSERT_OK(sorter.update(_runtime_state.get(), _chunk_3));
    ASSERT_OK(sorter.done(_runtime_state.get()));

    ChunkPtr page_1 = consume_page_from_sorter(sorter);

    ASSERT_EQ(16, total_rows);
    ASSERT_EQ(16, page_1->num_rows());
    const size_t Size = 16;
    std::vector<int32_t> permutation{69, 70, 71, 2, 4, 6, 12, 16, 24, 41, 49, 52, 54, 55, 56, 58};
    std::vector<int> result;
    for (size_t i = 0; i < Size; ++i) {
        result.push_back(page_1->get(i).get(0).get_int32());
    }
    EXPECT_EQ(permutation, result);

    clear_sort_exprs(sort_exprs);
}

// NOTE: this test case runs too slow
// TEST_F(ChunksSorterTest, full_sort_chunk_overflow) {
//     std::vector<bool> is_asc{true};
//     std::vector<bool> is_null_first{true};
//     auto expr_varchar = std::make_unique<ColumnRef>(TypeDescriptor(TYPE_VARCHAR), 0);
//     std::vector<ExprContext*> sort_exprs{new ExprContext(expr_varchar.get())};
//     DeferOp defer([&]() { clear_sort_exprs(sort_exprs); });

//     std::string big_string(1024, 'a');
//     ColumnPtr big_column = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), false);
//     for (int i = 0; i < 1024; i++) {
//         big_column->append_datum(Datum(Slice(big_string)));
//     }
//     Columns columns{big_column};
//     Chunk::SlotHashMap slots;
//     slots[0] = 0;
//     ChunkPtr big_chunk = std::make_shared<Chunk>(columns, slots);
//     ChunksSorterFullSort sorter(_runtime_state.get(), &sort_exprs, &is_asc, &is_null_first, "");

//     // Update until overflow
//     size_t total_bytes = 0;
//     while (total_bytes < Column::MAX_CAPACITY_LIMIT) {
//         total_bytes += big_column->byte_size();
//         std::cerr << "total bytes: " << total_bytes << std::endl;
//         sorter.update(_runtime_state.get(), big_chunk);
//     }
//     ASSERT_OK(sorter.done(_runtime_state.get()));
//     std::vector<ChunkPtr> output = consume_pages_from_sorter(sorter);
//     size_t output_bytes = 0;
//     for (auto& output_chunk : output) {
//         ASSERT_TRUE(output_chunk->has_large_column());
//         output_bytes += output_chunk->get_column_by_index(0)->byte_size();
//     }
//     ASSERT_EQ(total_bytes, output_bytes);
// }

TEST_F(ChunksSorterTest, topn_sort_limit_prune) {
    {
        // notnull
        auto column = ColumnTestHelper::build_column<int32_t>({1, 1, 1, 2, 2, 3, 4, 5, 6});
        auto cmp = [&](PermutationItem lhs, PermutationItem rhs) {
            return column->compare_at(lhs.index_in_chunk, rhs.index_in_chunk, *column, 1);
        };
        std::pair<int, int> range{0, column->size()};

        std::vector<int> expected{0, 3, 3, 3, 5, 5, 6, 7, 8, 9};
        for (size_t limit = 1; limit < column->size(); limit++) {
            size_t limited = column->size();
            Tie tie(column->size(), 1);
            Permutation perm = make_permutation(column->size());
            sort_and_tie_helper(false, column.get(), true, perm, tie, cmp, range, true, limit, &limited);
            EXPECT_EQ(expected[limit], limited);
        }
    }

    {
        // nullable column
        auto data = std::vector<int32_t>{0, 0, 0, 2, 2, 2, 3, 3, 4, 5, 6};
        auto null_data = std::vector<uint8_t>{1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0};
        auto column = ColumnTestHelper::build_nullable_column(data, null_data);
        std::vector<ColumnPtr> columns{column};
        auto null_pred = [&](PermutationItem item) { return column->is_null(item.index_in_chunk); };
        std::pair<int, int> range{0, column->size()};

        std::vector<int> expected{0, 3, 3, 3, 6, 6, 6, 8, 8, 9, 10, 11};
        for (size_t limit = 1; limit < column->size(); limit++) {
            size_t limited = column->size();
            Permutation perm = make_permutation(column->size());
            Tie tie(column->size(), 1);

            sort_and_tie_helper_nullable_vertical(false, columns, null_pred, SortDesc(true, true), perm, tie, range,
                                                  true, limit, &limited);
            EXPECT_EQ(expected[limit], limited) << " at index " << limit;
        }
    }
}

// NOLINTNEXTLINE
TEST_F(ChunksSorterTest, topn_sort_with_limit) {
    std::vector<std::tuple<std::string, ColumnRef*, std::vector<int32_t>>> test_cases = {
            {"cust_key", _expr_cust_key.get(),
             std::vector<int32_t>{2, 4, 6, 12, 16, 24, 41, 49, 52, 54, 55, 56, 58, 69, 70, 71}},
            {"nation", _expr_nation.get(),
             std::vector<int32_t>{69, 70, 71, 4, 54, 16, 41, 49, 55, 56, 52, 2, 12, 24, 58}},
            {"region", _expr_region.get(),
             std::vector<int32_t>{69, 70, 71, 2, 4, 6, 12, 16, 24, 41, 49, 52, 54, 55, 56}},
    };

    for (auto [name, column, expected] : test_cases) {
        std::vector<bool> is_asc{true, true};
        std::vector<bool> is_null_first{true, true};
        std::vector<ExprContext*> sort_exprs;
        sort_exprs.push_back(new ExprContext(column));
        sort_exprs.push_back(new ExprContext(_expr_cust_key.get()));
        ASSERT_OK(Expr::prepare(sort_exprs, _runtime_state.get()));
        ASSERT_OK(Expr::open(sort_exprs, _runtime_state.get()));

        constexpr int kTotalRows = 16;
        for (int limit = 1; limit < kTotalRows; limit++) {
            std::cerr << fmt::format("order by column {} limit {}", name, limit) << std::endl;
            ChunksSorterTopn sorter(_runtime_state.get(), &sort_exprs, &is_asc, &is_null_first, "", 0, limit);
            size_t total_rows = _chunk_1->num_rows() + _chunk_2->num_rows() + _chunk_3->num_rows();
            ASSERT_OK(sorter.update(_runtime_state.get(), ChunkPtr(_chunk_1->clone_unique().release())));
            ASSERT_OK(sorter.update(_runtime_state.get(), ChunkPtr(_chunk_2->clone_unique().release())));
            ASSERT_OK(sorter.update(_runtime_state.get(), ChunkPtr(_chunk_3->clone_unique().release())));
            ASSERT_OK(sorter.done(_runtime_state.get()));

            ChunkPtr page_1 = consume_page_from_sorter(sorter);

            ASSERT_EQ(kTotalRows, total_rows);
            ASSERT_EQ(limit, page_1->num_rows());
            std::vector<int32_t> permutation = expected;
            std::vector<int> result;
            for (size_t i = 0; i < page_1->num_rows(); ++i) {
                result.push_back(page_1->get(i).get(0).get_int32());
            }
            permutation.resize(limit);
            EXPECT_EQ(permutation, result);
        }

        clear_sort_exprs(sort_exprs);
    }
}

// NOLINTNEXTLINE
TEST_F(ChunksSorterTest, rank_topn) {
    std::vector<bool> is_asc{true};
    std::vector<bool> is_null_first{true};
    std::vector<ExprContext*> sort_exprs;
    sort_exprs.push_back(new ExprContext(_expr_ranking_key.get()));
    ASSERT_OK(Expr::prepare(sort_exprs, _runtime_state.get()));
    ASSERT_OK(Expr::open(sort_exprs, _runtime_state.get()));

    std::vector<int32_t> expected_perm = {1, 2, 3, 4, 5, 6, 6, 6, 7, 7, 7, 11, 12, 13, 14, 15, 16, 16, 16, 17, 17, 17};
    std::vector<int32_t> res_num_rows_by_limit = {-1, 1,  2,  3,  4,  5,  8,  8,  8,  11, 11, 11,
                                                  12, 13, 14, 15, 16, 19, 19, 19, 22, 22, 22};

    for (int i = 0; i < 2; ++i) {
        bool ranking1_first = true;
        if (i == 1) {
            ranking1_first = false;
        }
        for (int limit = 1; limit <= 22; limit++) {
            std::cerr << fmt::format("order by column {} limit {}", "ranking_key", limit) << std::endl;
            ChunksSorterTopn sorter(_runtime_state.get(), &sort_exprs, &is_asc, &is_null_first, "", 0, limit,
                                    TTopNType::RANK, 1);
            if (ranking1_first) {
                ASSERT_OK(sorter.update(_runtime_state.get(), ChunkPtr(_chunk_ranking_1->clone_unique().release())));
                ASSERT_OK(sorter.update(_runtime_state.get(), ChunkPtr(_chunk_ranking_2->clone_unique().release())));
            } else {
                ASSERT_OK(sorter.update(_runtime_state.get(), ChunkPtr(_chunk_ranking_2->clone_unique().release())));
                ASSERT_OK(sorter.update(_runtime_state.get(), ChunkPtr(_chunk_ranking_1->clone_unique().release())));
            }
            ASSERT_OK(sorter.done(_runtime_state.get()));

            ChunkPtr page = consume_page_from_sorter(sorter);
            int res_num_rows = res_num_rows_by_limit[limit];

            ASSERT_EQ(res_num_rows, page->num_rows());
            std::vector<int32_t> permutation = expected_perm;
            std::vector<int32_t> result;
            for (size_t i = 0; i < page->num_rows(); ++i) {
                result.push_back(page->get(i).get(0).get_int32());
            }
            permutation.resize(res_num_rows);
            EXPECT_EQ(permutation, result);
        }
    }

    clear_sort_exprs(sort_exprs);
}

// NOLINTNEXTLINE
TEST_F(ChunksSorterTest, full_sort_by_2_columns_null_first) {
    std::vector<bool> is_asc, is_null_first;
    is_asc.push_back(false); // region
    is_asc.push_back(true);  // cust_key
    is_null_first.push_back(true);
    is_null_first.push_back(true);
    std::vector<ExprContext*> sort_exprs;
    sort_exprs.push_back(new ExprContext(_expr_region.get()));
    sort_exprs.push_back(new ExprContext(_expr_cust_key.get()));
    ASSERT_OK(Expr::prepare(sort_exprs, _runtime_state.get()));
    ASSERT_OK(Expr::open(sort_exprs, _runtime_state.get()));

    auto pool = std::make_unique<ObjectPool>();
    std::vector<SlotId> slots{_expr_region->slot_id(), _expr_cust_key->slot_id()};
    ChunksSorterFullSort sorter(_runtime_state.get(), &sort_exprs, &is_asc, &is_null_first, "", 1024000, 16777216,
                                slots);
    sorter.setup_runtime(_runtime_state.get(), pool->add(new RuntimeProfile("", false)),
                         pool->add(new MemTracker(1L << 62, "", nullptr)));
    size_t total_rows = _chunk_1->num_rows() + _chunk_2->num_rows() + _chunk_3->num_rows();
    ASSERT_OK(sorter.update(_runtime_state.get(), _chunk_1));
    ASSERT_OK(sorter.update(_runtime_state.get(), _chunk_2));
    ASSERT_OK(sorter.update(_runtime_state.get(), _chunk_3));
    ASSERT_OK(sorter.done(_runtime_state.get()));

    ChunkPtr page_1 = consume_page_from_sorter(sorter);

    ASSERT_EQ(16, total_rows);
    ASSERT_EQ(16, page_1->num_rows());
    const size_t Size = 16;
    std::vector<int32_t> permutation{69, 70, 71, 2, 4, 6, 12, 16, 24, 41, 49, 52, 54, 55, 56, 58};
    std::vector<int32_t> result;
    for (size_t i = 0; i < Size; ++i) {
        result.push_back(page_1->get(i).get(0).get_int32());
    }
    EXPECT_EQ(permutation, result);

    clear_sort_exprs(sort_exprs);
}

// NOLINTNEXTLINE
TEST_F(ChunksSorterTest, full_sort_by_2_columns_null_last) {
    std::vector<bool> is_asc, is_null_first;
    is_asc.push_back(true);  // region
    is_asc.push_back(false); // cust_key
    is_null_first.push_back(false);
    is_null_first.push_back(false);
    std::vector<ExprContext*> sort_exprs;
    sort_exprs.push_back(new ExprContext(_expr_region.get()));
    sort_exprs.push_back(new ExprContext(_expr_cust_key.get()));
    ASSERT_OK(Expr::prepare(sort_exprs, _runtime_state.get()));
    ASSERT_OK(Expr::open(sort_exprs, _runtime_state.get()));

    auto pool = std::make_unique<ObjectPool>();
    std::vector<SlotId> slots{_expr_region->slot_id(), _expr_cust_key->slot_id()};
    ChunksSorterFullSort sorter(_runtime_state.get(), &sort_exprs, &is_asc, &is_null_first, "", 1024000, 16777216,
                                slots);
    sorter.setup_runtime(_runtime_state.get(), pool->add(new RuntimeProfile("", false)),
                         pool->add(new MemTracker(1L << 62, "", nullptr)));
    size_t total_rows = _chunk_1->num_rows() + _chunk_2->num_rows() + _chunk_3->num_rows();
    ASSERT_OK(sorter.update(_runtime_state.get(), _chunk_1));
    ASSERT_OK(sorter.update(_runtime_state.get(), _chunk_2));
    ASSERT_OK(sorter.update(_runtime_state.get(), _chunk_3));
    ASSERT_OK(sorter.done(_runtime_state.get()));

    ChunkPtr page_1 = consume_page_from_sorter(sorter);

    ASSERT_EQ(16, total_rows);
    ASSERT_EQ(16, page_1->num_rows());
    const size_t Size = 16;
    std::vector<int32_t> permutation{58, 56, 55, 54, 52, 49, 41, 24, 16, 12, 6, 4, 2, 71, 70, 69};
    std::vector<int32_t> result;
    for (size_t i = 0; i < Size; ++i) {
        result.push_back(page_1->get(i).get(0).get_int32());
    }
    EXPECT_EQ(permutation, result);

    clear_sort_exprs(sort_exprs);
}

// NOLINTNEXTLINE
TEST_F(ChunksSorterTest, full_sort_by_3_columns) {
    std::vector<bool> is_asc, is_null_first;
    is_asc.push_back(false); // region
    is_asc.push_back(true);  // nation
    is_asc.push_back(false); // cust_key
    is_null_first.push_back(true);
    is_null_first.push_back(true);
    is_null_first.push_back(true);
    std::vector<ExprContext*> sort_exprs;
    sort_exprs.push_back(new ExprContext(_expr_region.get()));
    sort_exprs.push_back(new ExprContext(_expr_nation.get()));
    sort_exprs.push_back(new ExprContext(_expr_cust_key.get()));
    ASSERT_OK(Expr::prepare(sort_exprs, _runtime_state.get()));
    ASSERT_OK(Expr::open(sort_exprs, _runtime_state.get()));

    auto pool = std::make_unique<ObjectPool>();
    std::vector<SlotId> slots{_expr_region->slot_id(), _expr_cust_key->slot_id()};
    ChunksSorterFullSort sorter(_runtime_state.get(), &sort_exprs, &is_asc, &is_null_first, "", 1024000, 16777216,
                                slots);
    sorter.setup_runtime(_runtime_state.get(), pool->add(new RuntimeProfile("", false)),
                         pool->add(new MemTracker(1L << 62, "", nullptr)));
    size_t total_rows = _chunk_1->num_rows() + _chunk_2->num_rows() + _chunk_3->num_rows();
    ASSERT_OK(sorter.update(_runtime_state.get(), _chunk_1));
    ASSERT_OK(sorter.update(_runtime_state.get(), _chunk_2));
    ASSERT_OK(sorter.update(_runtime_state.get(), _chunk_3));
    ASSERT_OK(sorter.done(_runtime_state.get()));

    ChunkPtr page_1 = consume_page_from_sorter(sorter);

    ASSERT_EQ(16, total_rows);
    ASSERT_EQ(16, page_1->num_rows());
    const size_t Size = 16;
    std::vector<int32_t> permutation{71, 70, 69, 54, 4, 56, 55, 49, 41, 16, 52, 58, 24, 12, 2, 6};
    std::vector<int32_t> result;
    for (size_t i = 0; i < Size; ++i) {
        result.push_back(page_1->get(i).get(0).get_int32());
    }
    ASSERT_EQ(permutation, result);

    clear_sort_exprs(sort_exprs);
}

// NOLINTNEXTLINE
TEST_F(ChunksSorterTest, full_sort_by_4_columns) {
    std::vector<bool> is_asc, is_null_first;
    is_asc.push_back(false); // mtk_sgmt
    is_asc.push_back(true);  // region
    is_asc.push_back(false); // nation
    is_asc.push_back(false); // cust_key
    is_null_first.push_back(false);
    is_null_first.push_back(true);
    is_null_first.push_back(true);
    is_null_first.push_back(false);
    std::vector<ExprContext*> sort_exprs;
    sort_exprs.push_back(new ExprContext(_expr_mkt_sgmt.get()));
    sort_exprs.push_back(new ExprContext(_expr_region.get()));
    sort_exprs.push_back(new ExprContext(_expr_nation.get()));
    sort_exprs.push_back(new ExprContext(_expr_cust_key.get()));
    ASSERT_OK(Expr::prepare(sort_exprs, _runtime_state.get()));
    ASSERT_OK(Expr::open(sort_exprs, _runtime_state.get()));

    auto pool = std::make_unique<ObjectPool>();
    std::vector<SlotId> slots{_expr_region->slot_id(), _expr_cust_key->slot_id()};
    ChunksSorterFullSort sorter(_runtime_state.get(), &sort_exprs, &is_asc, &is_null_first, "", 1024000, 16777216,
                                slots);
    sorter.setup_runtime(_runtime_state.get(), pool->add(new RuntimeProfile("", false)),
                         pool->add(new MemTracker(1L << 62, "", nullptr)));
    size_t total_rows = _chunk_1->num_rows() + _chunk_2->num_rows() + _chunk_3->num_rows();
    ASSERT_OK(sorter.update(_runtime_state.get(), _chunk_1));
    ASSERT_OK(sorter.update(_runtime_state.get(), _chunk_2));
    ASSERT_OK(sorter.update(_runtime_state.get(), _chunk_3));
    ASSERT_OK(sorter.done(_runtime_state.get()));

    ChunkPtr page_1 = consume_page_from_sorter(sorter);
    // print_chunk(page_1);

    ASSERT_EQ(16, total_rows);
    ASSERT_EQ(16, page_1->num_rows());
    const size_t Size = 16;
    std::vector<int32_t> permutation{24, 55, 4, 58, 12, 52, 41, 56, 49, 16, 6, 2, 54, 71, 70, 69};
    std::vector<int32_t> result;
    for (size_t i = 0; i < Size; ++i) {
        result.push_back(page_1->get(i).get(0).get_int32());
    }
    EXPECT_EQ(permutation, result);

    clear_sort_exprs(sort_exprs);
}

// NOLINTNEXTLINE
TEST_F(ChunksSorterTest, part_sort_by_3_columns_null_fisrt) {
    std::vector<bool> is_asc, is_null_first;
    is_asc.push_back(false); // region
    is_asc.push_back(true);  // nation
    is_asc.push_back(true);  // cust_key
    is_null_first.push_back(true);
    is_null_first.push_back(true);
    is_null_first.push_back(true);
    std::vector<ExprContext*> sort_exprs;
    sort_exprs.push_back(new ExprContext(_expr_region.get()));
    sort_exprs.push_back(new ExprContext(_expr_nation.get()));
    sort_exprs.push_back(new ExprContext(_expr_cust_key.get()));
    ASSERT_OK(Expr::prepare(sort_exprs, _runtime_state.get()));
    ASSERT_OK(Expr::open(sort_exprs, _runtime_state.get()));

    ChunksSorterTopn sorter(_runtime_state.get(), &sort_exprs, &is_asc, &is_null_first, "", 2, 7, TTopNType::ROW_NUMBER,
                            2);

    size_t total_rows = _chunk_1->num_rows() + _chunk_2->num_rows() + _chunk_3->num_rows();
    ASSERT_OK(sorter.update(_runtime_state.get(), ChunkPtr(_chunk_1->clone_unique().release())));
    ASSERT_OK(sorter.update(_runtime_state.get(), ChunkPtr(_chunk_2->clone_unique().release())));
    ASSERT_OK(sorter.update(_runtime_state.get(), ChunkPtr(_chunk_3->clone_unique().release())));
    ASSERT_OK(sorter.done(_runtime_state.get()));

    ChunkPtr page_1 = consume_page_from_sorter(sorter);

    ASSERT_EQ(16, total_rows);
    ASSERT_EQ(7, page_1->num_rows());
    // full sort: {69, 70, 71, 4, 54, 16, 41, 49, 55, 56, 52, 2, 12, 24, 58, 6};
    const size_t Size = 7;
    int32_t permutation[Size] = {71, 4, 54, 16, 41, 49, 55};
    for (size_t i = 0; i < Size; ++i) {
        ASSERT_EQ(permutation[i], page_1->get(i).get(0).get_int32());
    }

    clear_sort_exprs(sort_exprs);
}

// NOLINTNEXTLINE
TEST_F(ChunksSorterTest, part_sort_by_3_columns_null_last) {
    std::vector<bool> is_asc, is_null_first;
    is_asc.push_back(false); // region
    is_asc.push_back(true);  // nation
    is_asc.push_back(true);  // cust_key
    is_null_first.push_back(false);
    is_null_first.push_back(false);
    is_null_first.push_back(false);
    std::vector<ExprContext*> sort_exprs;
    sort_exprs.push_back(new ExprContext(_expr_region.get()));
    sort_exprs.push_back(new ExprContext(_expr_nation.get()));
    sort_exprs.push_back(new ExprContext(_expr_cust_key.get()));
    ASSERT_OK(Expr::prepare(sort_exprs, _runtime_state.get()));
    ASSERT_OK(Expr::open(sort_exprs, _runtime_state.get()));

    int offset = 7;
    for (int limit = 8; limit + offset <= 16; limit++) {
        ChunksSorterTopn sorter(_runtime_state.get(), &sort_exprs, &is_asc, &is_null_first, "", offset, limit,
                                TTopNType::ROW_NUMBER, 2);
        size_t total_rows = _chunk_1->num_rows() + _chunk_2->num_rows() + _chunk_3->num_rows();
        ASSERT_OK(sorter.update(_runtime_state.get(), ChunkPtr(_chunk_1->clone_unique().release())));
        ASSERT_OK(sorter.update(_runtime_state.get(), ChunkPtr(_chunk_2->clone_unique().release())));
        ASSERT_OK(sorter.update(_runtime_state.get(), ChunkPtr(_chunk_3->clone_unique().release())));
        ASSERT_OK(sorter.done(_runtime_state.get()));

        ChunkPtr page_1 = consume_page_from_sorter(sorter);

        ASSERT_EQ(16, total_rows);
        ASSERT_EQ(limit, page_1->num_rows());
        // full sort: {4, 54, 16, 41, 49, 55, 56, 52, 2, 12, 24, 58, 6, 69, 70, 71};
        std::vector<int32_t> permutation{52, 2, 12, 24, 58, 6, 69, 70, 71};
        std::vector<int32_t> result;
        for (size_t i = 0; i < page_1->num_rows(); ++i) {
            result.push_back(page_1->get(i).get(0).get_int32());
        }
        permutation.resize(limit);
        EXPECT_EQ(permutation, result);

        // part sort with large offset
        ChunksSorterTopn sorter2(_runtime_state.get(), &sort_exprs, &is_asc, &is_null_first, "", 100, limit,
                                 TTopNType::ROW_NUMBER, 2);
        ASSERT_OK(sorter.update(_runtime_state.get(), ChunkPtr(_chunk_1->clone_unique().release())));
        ASSERT_OK(sorter.update(_runtime_state.get(), ChunkPtr(_chunk_2->clone_unique().release())));
        ASSERT_OK(sorter.update(_runtime_state.get(), ChunkPtr(_chunk_3->clone_unique().release())));
        ASSERT_OK(sorter2.done(_runtime_state.get()));
        page_1 = consume_page_from_sorter(sorter2);
        ASSERT_TRUE(page_1 == nullptr);
    }

    clear_sort_exprs(sort_exprs);
}

// NOLINTNEXTLINE
TEST_F(ChunksSorterTest, order_by_with_unequal_sized_chunks) {
    std::vector<bool> is_asc, is_null_first;
    is_asc.push_back(false); // nation
    is_asc.push_back(false); // cust_key
    is_null_first.push_back(false);
    is_null_first.push_back(false);
    std::vector<ExprContext*> sort_exprs;
    sort_exprs.push_back(new ExprContext(_expr_nation.get()));
    sort_exprs.push_back(new ExprContext(_expr_cust_key.get()));
    ASSERT_OK(Expr::prepare(sort_exprs, _runtime_state.get()));
    ASSERT_OK(Expr::open(sort_exprs, _runtime_state.get()));

    // partial sort
    ChunksSorterTopn full_sorter(_runtime_state.get(), &sort_exprs, &is_asc, &is_null_first, "", 1, 6,
                                 TTopNType::ROW_NUMBER, 2);
    ChunkPtr chunk_1 = _chunk_1->clone_empty();
    ChunkPtr chunk_2 = _chunk_2->clone_empty();
    for (size_t i = 0; i < _chunk_1->num_columns(); ++i) {
        chunk_1->get_column_by_index(i)->append(*(_chunk_1->get_column_by_index(i)), 0, 1);
        chunk_2->get_column_by_index(i)->append(*(_chunk_2->get_column_by_index(i)), 0, 1);
    }
    ASSERT_OK(full_sorter.update(_runtime_state.get(), chunk_1));
    ASSERT_OK(full_sorter.update(_runtime_state.get(), chunk_2));
    ASSERT_OK(full_sorter.update(_runtime_state.get(), _chunk_3));
    ASSERT_OK(full_sorter.done(_runtime_state.get()));

    ChunkPtr page_1 = consume_page_from_sorter(full_sorter);

    ASSERT_EQ(6, page_1->num_rows());
    const size_t Size = 6;
    int32_t permutation[Size] = {24, 2, 52, 56, 4, 70};
    for (size_t i = 0; i < Size; ++i) {
        ASSERT_EQ(permutation[i], page_1->get(i).get(0).get_int32());
    }

    clear_sort_exprs(sort_exprs);
}

static void reset_permutation(SmallPermutation& permutation, int n) {
    permutation.resize(n);
    for (int i = 0; i < permutation.size(); i++) {
        permutation[i].index_in_chunk = i;
    }
}

TEST_F(ChunksSorterTest, stable_sort) {
    constexpr int N = 7;
    TypeDescriptor type_desc = TypeDescriptor(TYPE_INT);
    ColumnPtr col1 = ColumnHelper::create_column(type_desc, false);
    ColumnPtr col2 = ColumnHelper::create_column(type_desc, false);
    Columns columns{col1, col2};
    std::vector<int32_t> elements_col1{3, 1, 1, 2, 1, 2, 3};
    std::vector<int32_t> elements_col2{3, 2, 1, 3, 1, 2, 3};
    for (int i = 0; i < elements_col1.size(); i++) {
        col1->append_datum(Datum(elements_col1[i]));
        col2->append_datum(Datum(elements_col2[i]));
    }

    SmallPermutation perm = create_small_permutation(N);
    SortDescs sort_desc(std::vector<int>{1, 1}, std::vector<int>{1, 1});
    stable_sort_and_tie_columns(false, columns, sort_desc, &perm);

    bool sorted = std::is_sorted(perm.begin(), perm.end(), [&](SmallPermuteItem lhs, SmallPermuteItem rhs) {
        int x = col1->compare_at(lhs.index_in_chunk, rhs.index_in_chunk, *col1, 1);
        if (x != 0) {
            return x < 0;
        }
        x = col2->compare_at(lhs.index_in_chunk, rhs.index_in_chunk, *col2, 1);
        if (x != 0) {
            return x < 0;
        }
        return lhs.index_in_chunk < rhs.index_in_chunk;
    });
    ASSERT_TRUE(sorted);
    std::vector<uint32_t> result;
    permutate_to_selective(perm, &result);
    std::vector<uint32_t> expect{2, 4, 1, 5, 3, 0, 6};
    ASSERT_EQ(expect, result);
}

void pack_nullable(const ChunkPtr& chunk) {
    for (auto& col : chunk->columns()) {
        col = std::make_shared<NullableColumn>(col, std::make_shared<NullColumn>(col->size()));
    }
}

TEST_F(ChunksSorterTest, get_filter_test) {
    auto c0 = std::make_unique<ColumnRef>(TypeDescriptor(TYPE_INT), 0);
    auto c1 = std::make_unique<ColumnRef>(TypeDescriptor(TYPE_INT), 1);
    ObjectPool pool;

    std::vector<ExprContext*> sort_exprs;
    sort_exprs.push_back(pool.add(new ExprContext(c0.get())));
    sort_exprs.push_back(pool.add(new ExprContext(c1.get())));
    ASSERT_OK(Expr::prepare(sort_exprs, _runtime_state.get()));
    ASSERT_OK(Expr::open(sort_exprs, _runtime_state.get()));

    ChunkPtr merged_chunk = std::make_shared<Chunk>();
    {
        auto c0_merged = Int32Column::create();
        c0_merged->append(3);
        c0_merged->append(10);
        auto c1_merged = Int32Column::create();
        c1_merged->append(5);
        c1_merged->append(0);
        merged_chunk->append_column(c0_merged, 0);
        merged_chunk->append_column(c1_merged, 1);
    }
    pack_nullable(merged_chunk);

    DataSegment merged_segment;
    merged_segment.init(&sort_exprs, merged_chunk);

    ChunkPtr unmerged_chunk = std::make_shared<Chunk>();
    {
        auto c0_unmerged = Int32Column::create();
        int c0_datas[] = {0, 3, 10, 10};
        c0_unmerged->append_numbers(c0_datas, sizeof(c0_datas));
        auto c1_unmerged = Int32Column::create();
        int c1_datas[] = {1, 5, 30, 30};
        c1_unmerged->append_numbers(c1_datas, sizeof(c1_datas));

        unmerged_chunk->append_column(c0_unmerged, 0);
        unmerged_chunk->append_column(c1_unmerged, 1);
    }
    pack_nullable(unmerged_chunk);

    DataSegment unmerged_segment;
    unmerged_segment.init(&sort_exprs, unmerged_chunk);

    std::vector<DataSegment> segments;
    segments.push_back(std::move(unmerged_segment));

    std::vector<std::vector<uint8_t>> filter_array;
    filter_array.resize(1);
    filter_array.resize(unmerged_chunk->num_rows());

    std::vector<int> sort_order_flags = {1, 1};

    std::vector<int> null_first_flags = {1, 1};

    SortDescs desc(sort_order_flags, null_first_flags);

    size_t rows_to_sort = 2;
    uint32_t smaller_num, include_num;
    auto st = merged_segment.get_filter_array(segments, rows_to_sort, filter_array, desc, smaller_num, include_num);
    ASSERT_OK(st);

    size_t inc = 0;
    for (unsigned char i : filter_array[0]) {
        inc += i == DataSegment::INCLUDE_IN_SEGMENT;
    }

    ASSERT_EQ(include_num, inc);
}

TEST_F(ChunksSorterTest, column_incremental_sort) {
    TypeDescriptor type_desc = TypeDescriptor(TYPE_INT);
    ColumnPtr nullable_column = ColumnHelper::create_column(type_desc, true);

    // sort empty column
    SmallPermutation permutation;
    Tie tie;
    std::pair<int, int> range{0, 0};
    sort_and_tie_column(false, nullable_column, SortDesc(true, true), permutation, tie, range, false);
    sort_and_tie_column(false, nullable_column, SortDesc(true, false), permutation, tie, range, false);
    sort_and_tie_column(false, nullable_column, SortDesc(false, false), permutation, tie, range, false);
    sort_and_tie_column(false, nullable_column, SortDesc(false, true), permutation, tie, range, false);

    // sort all null column
    const int kNullCount = 5;
    nullable_column->append_nulls(kNullCount);
    permutation.resize(kNullCount);
    for (int i = 0; i < permutation.size(); i++) {
        permutation[i].index_in_chunk = i;
    }
    tie.resize(kNullCount);
    range = {0, kNullCount};
    sort_and_tie_column(false, nullable_column, SortDesc(true, true), permutation, tie, range, false);
    sort_and_tie_column(false, nullable_column, SortDesc(true, false), permutation, tie, range, false);
    sort_and_tie_column(false, nullable_column, SortDesc(false, false), permutation, tie, range, false);
    sort_and_tie_column(false, nullable_column, SortDesc(false, true), permutation, tie, range, false);

    // sort 1 element with 5 nulls
    SmallPermutation expect_perm;
    nullable_column->append_datum(Datum(1));
    reset_permutation(permutation, kNullCount + 1);
    tie = Tie(kNullCount + 1, 0);

    sort_and_tie_column(false, nullable_column, SortDesc(true, true), permutation, tie, range, false);
    reset_permutation(expect_perm, kNullCount + 1);
    EXPECT_EQ(expect_perm, permutation);
    EXPECT_EQ(Tie({0, 0, 0, 0, 0, 0}), tie);

    reset_permutation(permutation, kNullCount + 1);
    tie = Tie(kNullCount + 1, 0);
    sort_and_tie_column(false, nullable_column, SortDesc(true, false), permutation, tie, range, false);
    reset_permutation(expect_perm, kNullCount + 1);
    EXPECT_EQ(expect_perm, permutation);
    EXPECT_EQ(Tie({0, 0, 0, 0, 0, 0}), tie);

    reset_permutation(permutation, kNullCount + 1);
    tie = Tie(kNullCount + 1, 0);
    sort_and_tie_column(false, nullable_column, SortDesc(false, false), permutation, tie, range, false);
    reset_permutation(expect_perm, kNullCount + 1);
    EXPECT_EQ(expect_perm, permutation);
    EXPECT_EQ(Tie({0, 0, 0, 0, 0, 0}), tie);

    reset_permutation(permutation, kNullCount + 1);
    tie = Tie(kNullCount + 1, 0);
    sort_and_tie_column(false, nullable_column, SortDesc(false, true), permutation, tie, range, false);
    reset_permutation(expect_perm, kNullCount + 1);
    EXPECT_EQ(expect_perm, permutation);
    EXPECT_EQ(Tie({0, 0, 0, 0, 0, 0}), tie);

    // sort not-null elements
    nullable_column = nullable_column->clone_empty();
    nullable_column->append_datum(Datum(1));
    reset_permutation(expect_perm, 1);
    reset_permutation(permutation, 1);
    tie = Tie(1, 1);

    sort_and_tie_column(false, nullable_column, SortDesc(true, true), permutation, tie, range, false);
    EXPECT_EQ(expect_perm, permutation);
    EXPECT_EQ(Tie({1}), tie);

    sort_and_tie_column(false, nullable_column, SortDesc(true, false), permutation, tie, range, false);
    EXPECT_EQ(expect_perm, permutation);
    EXPECT_EQ(Tie({1}), tie);

    sort_and_tie_column(false, nullable_column, SortDesc(false, false), permutation, tie, range, false);
    EXPECT_EQ(expect_perm, permutation);
    EXPECT_EQ(Tie({1}), tie);

    sort_and_tie_column(false, nullable_column, SortDesc(false, true), permutation, tie, range, false);
    EXPECT_EQ(expect_perm, permutation);
    EXPECT_EQ(Tie({1}), tie);
}

TEST_F(ChunksSorterTest, find_zero) {
    std::vector<uint8_t> bytes;
    for (int len : std::vector<int>{1, 3, 7, 8, 12, 15, 16, 17, 127, 128}) {
        for (int zero_pos = 0; zero_pos < len; zero_pos++) {
            bytes = std::vector<uint8_t>(len, 1);
            bytes[zero_pos] = 0;

            size_t result = SIMD::find_zero(bytes, 0);
            EXPECT_EQ(zero_pos, result);

            // test non-zero
            std::fill(bytes.begin(), bytes.end(), 0);
            bytes[zero_pos] = 1;
            result = SIMD::find_nonzero(bytes, 0);
            EXPECT_EQ(zero_pos, result);
        }

        bytes = std::vector<uint8_t>(len, 1);
        EXPECT_EQ(len, SIMD::find_zero(bytes, 0));
        // test nonzero
        std::fill(bytes.begin(), bytes.end(), 0);
        EXPECT_EQ(len, SIMD::find_nonzero(bytes, 0));
    }
}

TEST_F(ChunksSorterTest, test_compare_column) {
    CompareVector cmp_vector;
    Buffer<Datum> rhs_values;

    rhs_values.emplace_back(int32_t(1));

    // get filter array x < 1
    TypeDescriptor type_desc = TypeDescriptor(TYPE_INT);
    ColumnPtr nullable_column = ColumnHelper::create_column(type_desc, true);

    nullable_column->append_datum(Datum(1));
    nullable_column->append_datum(Datum(2));
    nullable_column->append_nulls(2);

    cmp_vector.resize(nullable_column->size());

    auto desc_null_last = SortDescs();
    desc_null_last.descs.emplace_back(false, false);
    compare_columns(Columns{nullable_column}, cmp_vector, rhs_values, desc_null_last);

    CompareVector expected = {0, -1, 1, 1};
    EXPECT_EQ(cmp_vector, expected);

    // test asc null last
    // get filter array x > 1
    auto asc_null_last = SortDescs();
    asc_null_last.descs.emplace_back(true, true);

    cmp_vector.assign(4, 0);
    compare_columns(Columns{nullable_column}, cmp_vector, rhs_values, asc_null_last);

    expected = {0, 1, -1, -1};
    EXPECT_EQ(cmp_vector, expected);
}

TEST_F(ChunksSorterTest, test_tie) {
    using Ranges = std::vector<std::pair<int, int>>;
    Tie tie{0, 1, 1, 1, 0, 1, 1, 0, 1, 0, 1};
    TieIterator iterator(tie);
    Ranges ranges;

    while (iterator.next()) {
        ranges.emplace_back(iterator.range_first, iterator.range_last);
    }
    Ranges expected = {
            {0, 4},
            {4, 7},
            {7, 9},
            {9, 11},
    };
    ASSERT_EQ(expected, ranges);

    {
        // Empty tie
        Tie tie{0, 0};
        TieIterator iterator(tie);
        ASSERT_FALSE(iterator.next());
    }
    {
        // Empty tie
        Tie tie{0, 0, 0, 0};
        TieIterator iterator(tie);
        ASSERT_FALSE(iterator.next());
    }
    {
        Tie tie{0, 1};
        TieIterator iterator(tie);
        ASSERT_TRUE(iterator.next());
        ASSERT_EQ(iterator.range_first, 0);
        ASSERT_EQ(iterator.range_last, 2);
        ASSERT_FALSE(iterator.next());
    }
    {
        // Partial tie all 1
        Tie tie{1, 1, 1, 1, 1, 1};
        TieIterator iterator(tie, 0, 5);
        ASSERT_TRUE(iterator.next());
        ASSERT_EQ(iterator.range_first, 0);
        ASSERT_EQ(iterator.range_last, 5);
        ASSERT_FALSE(iterator.next());
    }
    {
        // Partial tie with prefix 0
        Tie tie{0, 1, 1, 1, 1, 1};
        TieIterator iterator(tie, 0, 5);
        ASSERT_TRUE(iterator.next());
        ASSERT_EQ(iterator.range_first, 0);
        ASSERT_EQ(iterator.range_last, 5);
        ASSERT_FALSE(iterator.next());
    }
    {
        // Partial tie with suffix 0
        Tie tie{0, 1, 1, 1, 0, 1};
        TieIterator iterator(tie, 0, 5);
        ASSERT_TRUE(iterator.next());
        ASSERT_EQ(iterator.range_first, 0);
        ASSERT_EQ(iterator.range_last, 4);
        ASSERT_FALSE(iterator.next());
    }
    {
        // Partial tie with prefix 0, start from 1
        Tie tie{0, 1, 1, 1, 1, 1};
        TieIterator iterator(tie, 1, 5);
        ASSERT_TRUE(iterator.next());
        ASSERT_EQ(iterator.range_first, 1);
        ASSERT_EQ(iterator.range_last, 5);
        ASSERT_FALSE(iterator.next());
    }
}

// https://github.com/StarRocks/starrocks/issues/30758
TEST_F(ChunksSorterTest, test_nan) {
    std::ifstream ifs("./be/test/exec/test_data/nan_column");
    std::string context((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    DoubleColumn double_column;
    double_column.resize(86016);
    auto& data = double_column.get_data();
    memcpy(data.data(), context.data(), context.size());
    auto permutation = create_small_permutation(double_column.size());
    auto inlined = create_inline_permutation<double>(permutation, data);
    auto begin = inlined.begin() + 26012;
    auto end = inlined.begin() + 26047;

    auto cmp = [](auto& lhs, auto& rhs) {
        return SorterComparator<double>::compare(lhs.inline_value, rhs.inline_value);
    };
    auto greater = [&](auto lhs, auto rhs) { return cmp(lhs, rhs) > 0; };
    ::pdqsort(begin, end, greater);
}

} // namespace starrocks
