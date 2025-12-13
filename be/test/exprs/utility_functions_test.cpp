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

    // Helper function to sort and print zorder encode results
    std::vector<std::pair<int, std::string>> sortAndPrintZOrderResults(
            const BinaryColumn* bin, const std::string& test_name, const std::vector<std::string>& row_labels = {}) {
        // Create pairs of (row_index, encoded_value) for sorting
        std::vector<std::pair<int, std::string>> row_encoded_pairs;
        for (int i = 0; i < bin->size(); ++i) {
            row_encoded_pairs.emplace_back(i, bin->get_slice(i).to_string());
        }

        // Sort by encoded value (lexicographic order)
        std::sort(row_encoded_pairs.begin(), row_encoded_pairs.end(),
                  [](const std::pair<int, std::string>& a, const std::pair<int, std::string>& b) {
                      return a.second < b.second;
                  });

        // Print sorted results for debugging
        std::cout << "\n=== " << test_name << " sorted results ===" << std::endl;
        for (const auto& pair : row_encoded_pairs) {
            std::cout << "Row " << pair.first;
            if (!row_labels.empty() && pair.first < static_cast<int>(row_labels.size())) {
                std::cout << " (" << row_labels[pair.first] << ")";
            }
            std::cout << " -> " << pair.second << std::endl;
        }

        return row_encoded_pairs;
    }
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
        const Int128Column* col = ColumnHelper::cast_to_raw<TYPE_LARGEINT>(result);
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

TEST_F(UtilityFunctionsTest, zorderEncodeSingleDimOrdering) {
    FunctionContext* ctx = FunctionContext::create_test_context();
    auto ptr = std::unique_ptr<FunctionContext>(ctx);

    // Test data table: [row_index] -> value
    std::vector<int32_t> test_data = {-2, -1, 0, 1, 2};

    auto c_int = Int32Column::create();
    for (int32_t value : test_data) {
        c_int->append(value);
    }

    Columns cols;
    cols.emplace_back(c_int);

    ASSIGN_OR_ASSERT_FAIL(ColumnPtr out, UtilityFunctions::encode_zorder_key(ctx, cols));
    auto* bin = ColumnHelper::cast_to_raw<TYPE_VARBINARY>(out);
    ASSERT_EQ(5, bin->size());

    // Use helper function to sort and print results
    std::vector<std::string> row_labels = {"-2", "-1", "0", "1", "2"};
    auto sorted_results = sortAndPrintZOrderResults(bin, "zorderEncodeSingleDimOrdering", row_labels);

    // Extract row indices in sorted order
    std::vector<int> sorted_row_indices;
    for (const auto& pair : sorted_results) {
        sorted_row_indices.push_back(pair.first);
    }

    std::vector<int> expected_order = {0, 1, 2, 3, 4};
    ASSERT_EQ(expected_order, sorted_row_indices);
}

TEST_F(UtilityFunctionsTest, zorderEncodeTwoDimsBasicInterleaving) {
    FunctionContext* ctx = FunctionContext::create_test_context();
    auto ptr = std::unique_ptr<FunctionContext>(ctx);

    // Test data table: [row_index] -> (a, b)
    // clang-format off
    std::vector<std::pair<int32_t, int32_t>> test_data = {
            {100, 10000},
            {123, 10001},
            {145, 10010},
            {167, 10019}
    };
    // clang-format on

    auto a = Int32Column::create();
    auto b = Int32Column::create();
    for (const auto& pair : test_data) {
        a->append(pair.first);
        b->append(pair.second);
    }

    Columns cols;
    cols.emplace_back(a);
    cols.emplace_back(b);

    ASSIGN_OR_ASSERT_FAIL(ColumnPtr out, UtilityFunctions::encode_zorder_key(ctx, cols));
    auto* bin = ColumnHelper::cast_to_raw<TYPE_VARBINARY>(out);
    ASSERT_EQ(test_data.size(), bin->size());

    // Use helper function to sort and print results
    auto sorted_results = sortAndPrintZOrderResults(bin, "zorderEncodeTwoDimsBasicInterleaving");

    // Extract row indices in sorted order
    std::vector<int> sorted_row_indices;
    for (const auto& pair : sorted_results) {
        sorted_row_indices.push_back(pair.first);
    }

    std::vector<int> expected_order = {0, 1, 2, 3};
    ASSERT_EQ(expected_order, sorted_row_indices);
}

TEST_F(UtilityFunctionsTest, zorderEncodeNullHandling) {
    FunctionContext* ctx = FunctionContext::create_test_context();
    auto ptr = std::unique_ptr<FunctionContext>(ctx);

    // Test data table: [row_index] -> (c1_value, c2_value, c1_is_null)
    std::vector<std::tuple<int32_t, int32_t, bool>> test_data = {
            {0, 0, true},  // (NULL,0)
            {1, 0, false}, // (1,0)
            {0, 1, true},  // (NULL,1)
            {1, 1, false}  // (1,1)
    };

    auto c1 = NullableColumn::create(Int32Column::create(), NullColumn::create());
    auto c2 = Int32Column::create();

    for (const auto& tuple : test_data) {
        if (std::get<2>(tuple)) { // is_null
            c1->append_nulls(1);
        } else {
            c1->append_datum(Datum(int32_t(std::get<0>(tuple))));
        }
        c2->append(std::get<1>(tuple));
    }

    Columns cols;
    cols.emplace_back(c1);
    cols.emplace_back(c2);

    ASSIGN_OR_ASSERT_FAIL(ColumnPtr out, UtilityFunctions::encode_zorder_key(ctx, cols));
    auto* bin = ColumnHelper::cast_to_raw<TYPE_VARBINARY>(out);
    ASSERT_EQ(4, bin->size());

    // Use helper function to sort and print results
    std::vector<std::string> row_labels = {"(NULL,0)", "(1,0)", "(NULL,1)", "(1,1)"};
    auto sorted_results = sortAndPrintZOrderResults(bin, "zorderEncodeNullHandling", row_labels);

    // Extract row indices in sorted order
    std::vector<int> sorted_row_indices;
    for (const auto& pair : sorted_results) {
        sorted_row_indices.push_back(pair.first);
    }

    // Verify expected ordering: NULL values should sort before non-NULL values
    // Expected order: Row 0(NULL,0) < Row 2(NULL,1) < Row 1(1,0) < Row 3(1,1)
    std::vector<int> expected_order = {0, 2, 1, 3};

    ASSERT_EQ(expected_order, sorted_row_indices);
}

TEST_F(UtilityFunctionsTest, zorderEncodeMixedDataTypes) {
    FunctionContext* ctx = FunctionContext::create_test_context();
    auto ptr = std::unique_ptr<FunctionContext>(ctx);

    // Test data table: [row_index] -> (int_val, bigint_val, float_val, double_val, date_val, timestamp_val)
    std::vector<std::tuple<int32_t, int64_t, float, double, DateValue, TimestampValue>> test_data = {
            {10, 20L, 30.5f, 40.5, DateValue::create(2023, 1, 10),
             TimestampValue::create(2023, 1, 10, 12, 0, 0, 0)}, // small positive values
            {50, 60L, 70.5f, 80.5, DateValue::create(2023, 2, 20),
             TimestampValue::create(2023, 2, 20, 12, 0, 0, 0)}, // medium positive values
            {0, 0L, 0.0f, 0.0, DateValue::create(1970, 1, 1), TimestampValue::create(1970, 1, 1, 0, 0, 0, 0)}, // zeros
            {90, 95L, 98.5f, 99.5, DateValue::create(2023, 12, 31),
             TimestampValue::create(2023, 12, 31, 23, 59, 59, 0)}, // large positive values
            {5, 15L, 25.5f, 35.5, DateValue::create(2023, 1, 5),
             TimestampValue::create(2023, 1, 5, 12, 0, 0, 0)} // very small positive values
    };

    // Create columns with different data types
    auto int_col = Int32Column::create();
    auto bigint_col = Int64Column::create();
    auto float_col = FloatColumn::create();
    auto double_col = DoubleColumn::create();
    auto date_col = DateColumn::create();
    auto timestamp_col = TimestampColumn::create();

    // Fill columns from test data
    for (const auto& tuple : test_data) {
        int_col->append(std::get<0>(tuple));
        bigint_col->append(std::get<1>(tuple));
        float_col->append(std::get<2>(tuple));
        double_col->append(std::get<3>(tuple));
        date_col->append(std::get<4>(tuple));
        timestamp_col->append(std::get<5>(tuple));
    }

    Columns cols;
    cols.emplace_back(int_col);
    cols.emplace_back(bigint_col);
    cols.emplace_back(float_col);
    cols.emplace_back(double_col);
    cols.emplace_back(date_col);
    cols.emplace_back(timestamp_col);

    ASSIGN_OR_ASSERT_FAIL(ColumnPtr out, UtilityFunctions::encode_zorder_key(ctx, cols));
    auto* bin = ColumnHelper::cast_to_raw<TYPE_VARBINARY>(out);
    ASSERT_EQ(5, bin->size());

    // Helper function to convert slice to vector for easier inspection
    auto slice_to_vec = [](const Slice& s) { return std::vector<uint8_t>(s.data, s.data + s.size); };

    // Get all encoded keys
    std::vector<std::vector<uint8_t>> keys;
    for (int i = 0; i < 5; ++i) {
        keys.push_back(slice_to_vec(bin->get_slice(i)));
    }

    // Verify that all keys have the expected structure:
    // [6 null markers] + [interleaved bits from 6 columns]
    // Z-order encoding interleaves bits from each dimension up to the maximum bit width
    // Max bit width is 64 bits, so total interleaved bits = 64 * 6 = 384 bits
    // Total bytes = 6 (null markers) + ceil(384 / 8) = 6 + 48 = 54 bytes
    const size_t expected_size = 6 + (64 * 6 + 7) / 8;

    for (const auto& key : keys) {
        ASSERT_EQ(expected_size, key.size()) << "Key size mismatch";

        // Verify null markers (first 6 bytes should all be 0x01 for non-null values)
        for (int i = 0; i < 6; ++i) {
            ASSERT_EQ(0x01, key[i]) << "Null marker at position " << i << " should be 0x01";
        }
    }

    // Use helper function to sort and print results
    std::vector<std::string> row_labels = {"small", "medium", "zeros", "large", "very_small"};
    auto sorted_results = sortAndPrintZOrderResults(bin, "zorderEncodeMixedDataTypes", row_labels);

    // Extract row indices in sorted order
    std::vector<int> sorted_row_indices;
    for (const auto& pair : sorted_results) {
        sorted_row_indices.push_back(pair.first);
    }

    // Verify expected ordering based on actual Z-order encoding behavior
    // Since all values are positive, Z-order should sort by magnitude
    // Expected order: Row 2 (zeros) < Row 4 (very_small) < Row 0 (small) < Row 1 (medium) < Row 3 (large)
    std::vector<int> expected_order = {2, 4, 0, 1, 3};
    ASSERT_EQ(expected_order, sorted_row_indices);
}

} // namespace starrocks
