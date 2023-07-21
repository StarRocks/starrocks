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

#include "exec/pipeline/sink/iceberg_table_sink_operator.h"

#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "testutil/assert.h"

namespace starrocks {

class IcebergTableSinkTest : public testing::Test {
public:
    IcebergTableSinkTest() = default;
    ~IcebergTableSinkTest() override = default;
};

TEST_F(IcebergTableSinkTest, TestDateValueToString) {
    auto type_date = TypeDescriptor::from_logical_type(TYPE_DATE);
    std::vector<TypeDescriptor> type_descs{type_date};

    auto data_column = DateColumn::create();
    {
        Datum datum;
        datum.set_date(DateValue::create(2023, 7, 9));
        data_column->append_datum(datum);
    }
    std::string date_partition_value;
    auto st = pipeline::IcebergTableSinkOperator::partition_value_to_string(data_column.get(), date_partition_value);
    ASSERT_OK(st);
    ASSERT_EQ("2023-07-09", date_partition_value);
}

TEST_F(IcebergTableSinkTest, TestIntValueToString) {
    auto int32_col = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_INT), true);
    std::vector<int32_t> int32_nums{33};
    auto count = int32_col->append_numbers(int32_nums.data(), size(int32_nums) * sizeof(int32_t));
    ASSERT_EQ(1, count);

    std::string int32_partition_value;
    auto st = pipeline::IcebergTableSinkOperator::partition_value_to_string(int32_col.get(), int32_partition_value);
    ASSERT_OK(st);
    ASSERT_EQ("33", int32_partition_value);
}

TEST_F(IcebergTableSinkTest, TestStringValueToString) {
    auto string_col = BinaryColumn::create();
    string_col->append("hello");

    std::string string_partition_value;
    auto st = pipeline::IcebergTableSinkOperator::partition_value_to_string(string_col.get(), string_partition_value);
    ASSERT_OK(st);
    ASSERT_EQ("hello", string_partition_value);
}

TEST_F(IcebergTableSinkTest, TestFloatValueToString) {
    auto float_column = FloatColumn::create();
    std::vector<float> values = {0.1};
    float_column->append_numbers(values.data(), values.size() * sizeof(float));

    std::string float_partition_value;
    auto st = pipeline::IcebergTableSinkOperator::partition_value_to_string(float_column.get(), float_partition_value);
    ASSERT_TRUE(st.is_not_supported());
    ASSERT_EQ("Partition value can't be float-4", st.message());
}

TEST_F(IcebergTableSinkTest, TestBooleanValueToString) {
    auto boolean_column = BooleanColumn::create();
    std::vector<uint8_t> values = {0};
    boolean_column->append_numbers(values.data(), values.size() * sizeof(uint8_t));

    std::string boolean_partition_value;
    auto st = pipeline::IcebergTableSinkOperator::partition_value_to_string(boolean_column.get(),
                                                                            boolean_partition_value);
    ASSERT_OK(st);
    ASSERT_EQ("false", boolean_partition_value);
}

} // namespace starrocks