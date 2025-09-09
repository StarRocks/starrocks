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

TEST_F(UtilityFunctionsTest, encodeSortKeyBasicOrdering) {
    FunctionContext* ctx = FunctionContext::create_test_context();
    auto ptr = std::unique_ptr<FunctionContext>(ctx);

    // Prepare columns of different types
    // int32
    auto c_int = Int32Column::create();
    c_int->append(1);
    c_int->append(2);
    c_int->append(10);

    // binary
    auto c_str = BinaryColumn::create();
    c_str->append(Slice("a"));
    c_str->append(Slice("b"));
    c_str->append(Slice("c"));

    // date
    auto c_date = DateColumn::create();
    c_date->append(DateValue::create(2021, 1, 1));
    c_date->append(DateValue::create(2021, 1, 2));
    c_date->append(DateValue::create(2021, 1, 10));

    // timestamp
    auto c_timestamp = TimestampColumn::create();
    c_timestamp->append(TimestampValue::create(2021, 1, 1, 0, 0, 0, 0));
    c_timestamp->append(TimestampValue::create(2021, 1, 2, 0, 0, 0, 0));
    c_timestamp->append(TimestampValue::create(2021, 1, 10, 0, 0, 0, 0));

    // float32
    auto c_float32 = FloatColumn::create();
    c_float32->append(1.0f);
    c_float32->append(2.0f);
    c_float32->append(10.0f);

    // float64
    auto c_float64 = DoubleColumn::create();
    c_float64->append(1.0);
    c_float64->append(2.0);
    c_float64->append(10.0);

    Columns cols;
    cols.emplace_back(c_int);
    cols.emplace_back(c_str);
    cols.emplace_back(c_date);
    cols.emplace_back(c_timestamp);
    cols.emplace_back(c_float32);
    cols.emplace_back(c_float64);

    // verify each column
    for (auto& col : cols) {
        ASSIGN_OR_ASSERT_FAIL(ColumnPtr out, UtilityFunctions::encode_sort_key(ctx, {col}));
        auto* bin = ColumnHelper::cast_to_raw<TYPE_VARBINARY>(out);
        ASSERT_EQ(3, bin->size());

        std::vector<Slice> keys = {bin->get_slice(0), bin->get_slice(1), bin->get_slice(2)};
        ASSERT_LT(keys[0].compare(keys[1]), 0) << col->get_name();
        ASSERT_LT(keys[1].compare(keys[2]), 0) << col->get_name();
    }

    // verify all columns
    ASSIGN_OR_ASSERT_FAIL(ColumnPtr out, UtilityFunctions::encode_sort_key(ctx, cols));
    auto* bin = ColumnHelper::cast_to_raw<TYPE_VARBINARY>(out);
    ASSERT_EQ(3, bin->size());

    // Verify lexicographic order aligns with tuple order
    std::vector<Slice> keys = {bin->get_slice(0), bin->get_slice(1), bin->get_slice(2)};
    ASSERT_LT(keys[0].compare(keys[1]), 0);
    ASSERT_LT(keys[1].compare(keys[2]), 0);
}

TEST_F(UtilityFunctionsTest, encodeSortKeyNullHandling) {
    FunctionContext* ctx = FunctionContext::create_test_context();
    auto ptr = std::unique_ptr<FunctionContext>(ctx);

    // Create nullable columns with mixed null and non-null values
    auto c_int = NullableColumn::create(Int32Column::create(), NullColumn::create());
    c_int->append_datum(Datum(int32_t(1))); // non-null
    c_int->append_nulls(1);                 // null
    c_int->append_datum(Datum(int32_t(2))); // non-null
    c_int->append_nulls(1);                 // null

    auto c_str = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    c_str->append_nulls(1);                 // null
    c_str->append_datum(Datum(Slice("z"))); // non-null
    c_str->append_nulls(1);                 // null
    c_str->append_datum(Datum(Slice("a"))); // non-null

    Columns cols;
    cols.emplace_back(c_int);
    cols.emplace_back(c_str);

    ASSIGN_OR_ASSERT_FAIL(ColumnPtr out, UtilityFunctions::encode_sort_key(ctx, cols));
    auto* bin = ColumnHelper::cast_to_raw<TYPE_VARBINARY>(out);
    ASSERT_EQ(4, bin->size());

    // Get the sort keys for each row
    Slice k0 = bin->get_slice(0); // (1,NULL) - first non-null, second null
    Slice k1 = bin->get_slice(1); // (NULL,"z") - first null, second non-null
    Slice k2 = bin->get_slice(2); // (2,NULL) - first non-null, second null
    Slice k3 = bin->get_slice(3); // (NULL,"a") - first null, second non-null

    // Verify that null values sort before non-null values
    // Row 1 (NULL,"z") should sort before Row 0 (1,NULL) - null in first column takes precedence
    ASSERT_LT(k1.compare(k0), 0) << "NULL should sort before non-NULL in first column";

    // Row 3 (NULL,"a") should sort before Row 2 (2,NULL) - null in first column takes precedence
    ASSERT_LT(k3.compare(k2), 0) << "NULL should sort before non-NULL in first column";

    // Row 3 (NULL,"a") should sort before Row 0 (1,NULL) - null in first column takes precedence
    ASSERT_LT(k3.compare(k0), 0) << "NULL in first column should sort before any non-NULL";

    // Verify that null values only contain null markers and no underlying data encoding
    // This tests the fix where null rows don't process underlying data
    // The first byte should be the null marker (\0 for null, \1 for non-null)
    ASSERT_EQ('\0', k1.data[0]) << "First column null marker should be \\0";
    ASSERT_EQ('\1', k0.data[0]) << "First column non-null marker should be \\1";
    ASSERT_EQ('\0', k3.data[0]) << "First column null marker should be \\0";
    ASSERT_EQ('\1', k2.data[0]) << "First column non-null marker should be \\1";

    // Verify that rows with null in second column also have correct null markers
    // The second column null marker should be at position after first column encoding
    // For row 0: first column is non-null (1), so second column null marker should be after int32 encoding
    // For row 2: first column is non-null (2), so second column null marker should be after int32 encoding
    ASSERT_EQ('\0', k0.data[5])
            << "Second column null marker should be \\0 for row 0"; // After int32 encoding (4 bytes) + null marker (1 byte)
    ASSERT_EQ('\0', k2.data[5]) << "Second column null marker should be \\0 for row 2";

    // Verify that the sort keys are deterministic and consistent
    // Running the same operation should produce identical results
    ASSIGN_OR_ASSERT_FAIL(ColumnPtr out2, UtilityFunctions::encode_sort_key(ctx, cols));
    auto* bin2 = ColumnHelper::cast_to_raw<TYPE_VARBINARY>(out2);
    ASSERT_EQ(4, bin2->size());

    for (size_t i = 0; i < 4; i++) {
        ASSERT_EQ(bin->get_slice(i).to_string(), bin2->get_slice(i).to_string())
                << "Sort keys should be deterministic for row " << i;
    }
}

TEST_F(UtilityFunctionsTest, encodeSortKeyStringEscaping) {
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

    ASSIGN_OR_ASSERT_FAIL(ColumnPtr out, UtilityFunctions::encode_sort_key(ctx, cols));
    auto* bin = ColumnHelper::cast_to_raw<TYPE_VARBINARY>(out);
    ASSERT_EQ(2, bin->size());
    // Ensure keys are comparable and not identical
    Slice k0 = bin->get_slice(0);
    Slice k1 = bin->get_slice(1);
    ASSERT_NE(k0.to_string(), k1.to_string());
}

} // namespace starrocks
