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

#include "exprs/utility_functions.h"

#include <cctz/civil_time.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "common/statusor.h"
#include "exprs/function_context.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"
#include "types/logical_type.h"
#include "util/random.h"
#include "util/time.h"

namespace starrocks {

class UtilityFunctionsTest : public ::testing::Test {
public:
    void SetUp() override {}
};

TEST_F(UtilityFunctionsTest, versionTest) {
    FunctionContext* ctx = FunctionContext::create_test_context();
    auto ptr = std::unique_ptr<FunctionContext>(ctx);

    // test version
    {
        auto var1_col = ColumnHelper::create_const_column<TYPE_INT>(2, 1);

        Columns columns;
        columns.emplace_back(std::move(var1_col));

        ColumnPtr result = UtilityFunctions::version(ctx, columns).value();

        ASSERT_TRUE(result->is_constant());

        auto v = ColumnHelper::get_const_value<TYPE_VARCHAR>(result);

        ASSERT_EQ("5.1.0", v);
    }
}

TEST_F(UtilityFunctionsTest, sleepTest) {
    FunctionContext* ctx = FunctionContext::create_test_context();
    auto ptr = std::unique_ptr<FunctionContext>(ctx);
    RuntimeState state;
    ptr->set_runtime_state(&state);

    // test sleep
    {
        auto var1_col = ColumnHelper::create_const_column<TYPE_INT>(1, 1);

        Columns columns;
        columns.emplace_back(std::move(var1_col));

        ColumnPtr result = UtilityFunctions::sleep(ctx, columns).value();

        ASSERT_EQ(1, result->size());

        auto v = ColumnHelper::get_const_value<TYPE_BOOLEAN>(result);
        ASSERT_TRUE(v);
    }
}

TEST_F(UtilityFunctionsTest, uuidTest) {
    FunctionContext* ctx = FunctionContext::create_test_context();
    auto ptr = std::unique_ptr<FunctionContext>(ctx);

    // test uuid
    {
        int column_size = static_cast<int>(Random(GetCurrentTimeNanos()).Uniform(10) + 1);
        auto var1_col = ColumnHelper::create_const_column<TYPE_INT>(column_size, column_size);

        Columns columns;
        columns.emplace_back(std::move(var1_col));

        ColumnPtr result = UtilityFunctions::uuid(ctx, columns).value();

        ASSERT_FALSE(result->is_constant());

        std::set<int> hyphens_position = {8, 13, 18, 23};
        std::set<std::string> deduplication;
        ColumnViewer<TYPE_VARCHAR> column_viewer(result);

        ASSERT_EQ(column_viewer.size(), column_size);

        for (int column_idx = 0; column_idx < column_viewer.size(); column_idx++) {
            auto& uuid = column_viewer.value(column_idx);
            ASSERT_EQ(36, uuid.get_size());
            deduplication.insert(uuid.to_string());
        }

        ASSERT_EQ(deduplication.size(), column_size);
    }

    {
        int32_t chunk_size = 4096;
        auto var1_col = ColumnHelper::create_const_column<TYPE_INT>(chunk_size, 1);
        Columns columns;
        columns.emplace_back(std::move(var1_col));
        ColumnPtr result = UtilityFunctions::uuid_numeric(ctx, columns).value();
        Int128Column* col = ColumnHelper::cast_to_raw<TYPE_LARGEINT>(result);
        std::set<int128_t> vals;
        vals.insert(col->get_data().begin(), col->get_data().end());
        ASSERT_EQ(vals.size(), chunk_size);
    }

    {
        Columns columns;
        ColumnPtr result = UtilityFunctions::host_name(ctx, columns).value();
        ColumnViewer<TYPE_VARCHAR> column_viewer(result);
        ASSERT_EQ(column_viewer.size(), 1);
    }
}

TEST_F(UtilityFunctionsTest, makeSortKeyBasicOrdering) {
    FunctionContext* ctx = FunctionContext::create_test_context();
    auto ptr = std::unique_ptr<FunctionContext>(ctx);

    // Prepare columns of different types
    auto c_int = Int32Column::create();
    c_int->append(1);
    c_int->append(2);
    c_int->append(10);

    auto c_str = BinaryColumn::create();
    c_str->append(Slice("a"));
    c_str->append(Slice("b"));
    c_str->append(Slice("b"));

    auto c_date = Int32Column::create();
    // Assume DATE stored as int32 days; smaller -> earlier
    c_date->append(1000);
    c_date->append(1000);
    c_date->append(999);

    Columns cols;
    cols.emplace_back(c_int);
    cols.emplace_back(c_str);
    cols.emplace_back(c_date);

    ColumnPtr out = UtilityFunctions::make_sort_key(ctx, cols).value();
    auto* bin = ColumnHelper::cast_to_raw<TYPE_VARBINARY>(out);
    ASSERT_EQ(3, bin->size());

    // Verify lexicographic order aligns with tuple order
    std::vector<Slice> keys = {bin->get_slice(0), bin->get_slice(1), bin->get_slice(2)};
    // Expect row0 (1,"a",1000) < row1 (2,"b",1000) < row2 (10,"b",999)
    ASSERT_LT(keys[0].compare(keys[1]), 0);
    ASSERT_LT(keys[1].compare(keys[2]), 0);
}

TEST_F(UtilityFunctionsTest, makeSortKeyNullHandling) {
    FunctionContext* ctx = FunctionContext::create_test_context();
    auto ptr = std::unique_ptr<FunctionContext>(ctx);

    auto c_int = NullableColumn::create(Int32Column::create(), NullColumn::create());
    c_int->append_datum(Datum(int32_t(1)));
    c_int->append_nulls(1);

    auto c_str = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    c_str->append_datum(Datum(Slice("z")));
    c_str->append_datum(Datum(Slice("a")));

    Columns cols;
    cols.emplace_back(c_int);
    cols.emplace_back(c_str);

    ASSIGN_OR_ASSERT_FAIL(ColumnPtr out, UtilityFunctions::make_sort_key(ctx, cols));
    auto* bin = ColumnHelper::cast_to_raw<TYPE_VARBINARY>(out);
    ASSERT_EQ(2, bin->size());

    // NULL in first column should sort before non-NULL
    Slice k0 = bin->get_slice(0); // (1,"z")
    Slice k1 = bin->get_slice(1); // (NULL,"a")
    ASSERT_LT(k1.compare(k0), 0);
}

TEST_F(UtilityFunctionsTest, makeSortKeyStringEscaping) {
    FunctionContext* ctx = FunctionContext::create_test_context();
    auto ptr = std::unique_ptr<FunctionContext>(ctx);

    auto c1 = BinaryColumn::create();
    auto c2 = BinaryColumn::create();
    c1->append(Slice("a\0b", 3));
    c1->append(Slice("a", 1));
    c2->append(Slice("x", 1));
    c2->append(Slice("\0", 1));

    Columns cols;
    cols.emplace_back(c1);
    cols.emplace_back(c2);

    ASSIGN_OR_ASSERT_FAIL(ColumnPtr out, UtilityFunctions::make_sort_key(ctx, cols));
    auto* bin = ColumnHelper::cast_to_raw<TYPE_VARBINARY>(out);
    ASSERT_EQ(2, bin->size());
    // Ensure keys are comparable and not identical
    Slice k0 = bin->get_slice(0);
    Slice k1 = bin->get_slice(1);
    ASSERT_NE(k0.to_string(), k1.to_string());
}

} // namespace starrocks
