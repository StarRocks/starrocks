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

#include <memory>
#include <vector>

#include "column/array_column.h"
#include "column/column_helper.h"
#include "storage/aggregate_type.h"
#include "storage/column_aggregate_func.h"
#include "types/array_type_info.h"

namespace starrocks {

TEST(ColumnAggregator, testIntSum) {
    FieldPtr field = std::make_shared<Field>(1, "test", LogicalType::TYPE_INT, false);
    field->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_SUM);

    auto aggregator = ColumnAggregatorFactory::create_value_column_aggregator(field);

    auto src1 = Int32Column::create();
    auto src2 = Int32Column::create();
    auto src3 = Int32Column::create();

    for (int i = 0; i < 1024; i++) {
        src1->append(1);
        src2->append(1);
        src3->append(1);
    }

    auto agg1 = Int32Column::create();

    aggregator->update_aggregate(agg1.get());
    aggregator->update_source(src1);

    std::vector<uint32_t> loops;
    loops.emplace_back(2);
    loops.emplace_back(1022);

    aggregator->aggregate_values(0, 2, loops.data(), false);

    ASSERT_EQ(1, agg1->size());
    ASSERT_EQ(2, agg1->get_data()[0]);

    aggregator->update_source(src2);

    loops.clear();
    loops.emplace_back(3);
    loops.emplace_back(100);
    loops.emplace_back(921);

    aggregator->aggregate_values(0, 3, loops.data(), false);

    ASSERT_EQ(3, agg1->size());
    ASSERT_EQ(2, agg1->get_data()[0]);
    ASSERT_EQ(1025, agg1->get_data()[1]);
    ASSERT_EQ(100, agg1->get_data()[2]);

    aggregator->update_source(src3);

    loops.clear();
    loops.emplace_back(1);
    loops.emplace_back(1023);

    aggregator->aggregate_values(0, 2, loops.data(), true);

    aggregator->finalize();

    ASSERT_EQ(6, agg1->size());
    ASSERT_EQ(2, agg1->get_data()[0]);
    ASSERT_EQ(1025, agg1->get_data()[1]);
    ASSERT_EQ(100, agg1->get_data()[2]);
    ASSERT_EQ(921, agg1->get_data()[3]);
    ASSERT_EQ(1, agg1->get_data()[4]);
    ASSERT_EQ(1023, agg1->get_data()[5]);
}

TEST(ColumnAggregator, testNullIntSum) {
    FieldPtr field = std::make_shared<Field>(1, "test", LogicalType::TYPE_INT, true);
    field->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_SUM);

    auto aggregator = ColumnAggregatorFactory::create_value_column_aggregator(field);

    auto src1 = Int32Column::create();
    auto null1 = NullColumn ::create();

    auto src2 = Int32Column::create();
    auto null2 = NullColumn::create();

    auto src3 = Int32Column::create();
    auto null3 = NullColumn::create();

    for (int i = 0; i < 1024; i++) {
        src1->append(1);
        null1->append(0);
    }

    for (int i = 0; i < 1024; i++) {
        src2->append(1);
        null2->append(1);
    }

    for (int i = 0; i < 1024; i++) {
        src3->append(1);
        null3->append(i % 2 == 0);
    }

    auto nsrc1 = NullableColumn::create(src1, null1);
    auto nsrc2 = NullableColumn::create(src2, null2);
    auto nsrc3 = NullableColumn::create(src3, null3);

    auto agg1 = NullableColumn::create(Int32Column::create(), NullColumn::create());

    auto dst = down_cast<Int32Column*>(agg1->data_column().get());
    auto ndst = down_cast<NullColumn*>(agg1->null_column().get());

    aggregator->update_aggregate(agg1.get());
    aggregator->update_source(nsrc1);

    std::vector<uint32_t> loops;
    loops.emplace_back(2);
    loops.emplace_back(1022);

    aggregator->aggregate_values(0, 2, loops.data(), false);

    ASSERT_EQ(1, agg1->size());
    ASSERT_EQ(2, dst->get_data()[0]);
    ASSERT_EQ(0, ndst->get_data()[0]);
    ASSERT_EQ(false, agg1->is_null(0));

    aggregator->update_source(nsrc2);

    loops.clear();
    loops.emplace_back(3);
    loops.emplace_back(100);
    loops.emplace_back(921);

    aggregator->aggregate_values(0, 3, loops.data(), false);

    ASSERT_EQ(3, agg1->size());
    ASSERT_EQ(2, dst->get_data()[0]);
    ASSERT_EQ(0, ndst->get_data()[0]);

    ASSERT_EQ(1022, dst->get_data()[1]);
    ASSERT_EQ(0, ndst->get_data()[1]);

    ASSERT_EQ(0, dst->get_data()[2]);
    ASSERT_EQ(1, ndst->get_data()[2]);

    aggregator->update_source(nsrc3);

    loops.clear();
    loops.emplace_back(1);
    loops.emplace_back(1023);

    aggregator->aggregate_values(0, 2, loops.data(), true);

    aggregator->finalize();

    ASSERT_EQ(6, agg1->size());

    ASSERT_EQ(2, dst->get_data()[0]);
    ASSERT_EQ(0, ndst->get_data()[0]);

    ASSERT_EQ(1022, dst->get_data()[1]);
    ASSERT_EQ(0, ndst->get_data()[1]);

    ASSERT_EQ(0, dst->get_data()[2]);
    ASSERT_EQ(1, ndst->get_data()[2]);

    ASSERT_EQ(0, dst->get_data()[3]);
    ASSERT_EQ(1, ndst->get_data()[3]);

    ASSERT_EQ(0, dst->get_data()[4]);
    ASSERT_EQ(1, ndst->get_data()[4]);

    ASSERT_EQ(512, dst->get_data()[5]);
    ASSERT_EQ(0, ndst->get_data()[5]);

    ASSERT_EQ(false, agg1->is_null(0));
    ASSERT_EQ(false, agg1->is_null(1));
    ASSERT_EQ(true, agg1->is_null(2));
    ASSERT_EQ(true, agg1->is_null(3));
    ASSERT_EQ(true, agg1->is_null(4));
    ASSERT_EQ(false, agg1->is_null(5));
}

TEST(ColumnAggregator, testIntMax) {
    FieldPtr field = std::make_shared<Field>(1, "test", LogicalType::TYPE_INT, false);
    field->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_MAX);

    auto aggregator = ColumnAggregatorFactory::create_value_column_aggregator(field);

    auto src1 = Int32Column::create();
    auto src2 = Int32Column::create();
    auto src3 = Int32Column::create();

    for (int i = 0; i < 1024; i++) {
        src1->append(i);
        src2->append(i * 3);
        src3->append(i * 2);
    }

    auto agg1 = Int32Column::create();

    aggregator->update_aggregate(agg1.get());
    aggregator->update_source(src1);

    std::vector<uint32_t> loops;
    loops.emplace_back(2);
    loops.emplace_back(1022);

    aggregator->aggregate_values(0, 2, loops.data(), false);

    ASSERT_EQ(1, agg1->size());
    ASSERT_EQ(1, agg1->get_data()[0]);

    aggregator->update_source(src2);

    loops.clear();
    loops.emplace_back(3);
    loops.emplace_back(100);
    loops.emplace_back(921);

    aggregator->aggregate_values(0, 3, loops.data(), false);

    ASSERT_EQ(3, agg1->size());
    ASSERT_EQ(1, agg1->get_data()[0]);
    ASSERT_EQ(1023, agg1->get_data()[1]);
    ASSERT_EQ(306, agg1->get_data()[2]);

    aggregator->update_source(src3);

    loops.clear();
    loops.emplace_back(1);
    loops.emplace_back(1023);

    aggregator->aggregate_values(0, 2, loops.data(), true);

    aggregator->finalize();

    ASSERT_EQ(6, agg1->size());
    ASSERT_EQ(1, agg1->get_data()[0]);
    ASSERT_EQ(1023, agg1->get_data()[1]);
    ASSERT_EQ(306, agg1->get_data()[2]);
    ASSERT_EQ(3069, agg1->get_data()[3]);
    ASSERT_EQ(0, agg1->get_data()[4]);
    ASSERT_EQ(2046, agg1->get_data()[5]);
}

TEST(ColumnAggregator, testStringMin) {
    FieldPtr field = std::make_shared<Field>(1, "test", LogicalType::TYPE_VARCHAR, false);
    field->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_MIN);

    auto aggregator = ColumnAggregatorFactory::create_value_column_aggregator(field);

    auto src1 = BinaryColumn::create();
    auto src2 = BinaryColumn::create();
    auto src3 = BinaryColumn::create();

    for (int i = 0; i < 1024; i++) {
        src1->append(Slice(std::to_string(i + 1000)));
        src2->append(Slice(std::to_string(i + 3000)));
        src3->append(Slice(std::to_string(i + 2000)));
    }

    auto agg1 = BinaryColumn::create();

    aggregator->update_aggregate(agg1.get());
    aggregator->update_source(src1);

    std::vector<uint32_t> loops;
    loops.emplace_back(2);
    loops.emplace_back(1022);

    aggregator->aggregate_values(0, 2, loops.data(), false);

    ASSERT_EQ(1, agg1->size());
    ASSERT_EQ("1000", agg1->get_data()[0].to_string());

    aggregator->update_source(src2);

    loops.clear();
    loops.emplace_back(3);
    loops.emplace_back(100);
    loops.emplace_back(921);

    aggregator->aggregate_values(0, 3, loops.data(), false);

    EXPECT_EQ(3, agg1->size());
    EXPECT_EQ("1000", agg1->get_data()[0].to_string());
    EXPECT_EQ("1002", agg1->get_data()[1].to_string());
    EXPECT_EQ("3003", agg1->get_data()[2].to_string());

    aggregator->update_source(src3);

    loops.clear();
    loops.emplace_back(1);
    loops.emplace_back(1023);

    aggregator->aggregate_values(0, 2, loops.data(), true);

    aggregator->finalize();

    EXPECT_EQ(6, agg1->size());
    EXPECT_EQ("1000", agg1->get_data()[0].to_string());
    EXPECT_EQ("1002", agg1->get_data()[1].to_string());
    EXPECT_EQ("3003", agg1->get_data()[2].to_string());
    EXPECT_EQ("3103", agg1->get_data()[3].to_string());
    EXPECT_EQ("2000", agg1->get_data()[4].to_string());
    EXPECT_EQ("2001", agg1->get_data()[5].to_string());
}

TEST(ColumnAggregator, testNullBooleanMin) {
    FieldPtr field = std::make_shared<Field>(1, "test_boolean", LogicalType::TYPE_BOOLEAN, true);
    field->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_MIN);

    auto agg = NullableColumn::create(BooleanColumn::create(), NullColumn::create());
    auto aggregator = ColumnAggregatorFactory::create_value_column_aggregator(field);
    aggregator->update_aggregate(agg.get());
    std::vector<uint32_t> loops;

    // first chunk column
    auto src = NullableColumn::create(BooleanColumn::create(), NullColumn::create());
    src->append_nulls(1);

    aggregator->update_source(src);

    loops.clear();
    loops.emplace_back(1);

    aggregator->aggregate_values(0, 1, loops.data(), false);

    ASSERT_EQ(0, agg->size());

    // second chunk column
    src->reset_column();
    uint8_t val = 1;
    src->append_numbers(&val, 1);
    src->append_nulls(1);

    aggregator->update_source(src);

    loops.clear();
    loops.emplace_back(1);
    loops.emplace_back(1);

    aggregator->aggregate_values(0, 2, loops.data(), true);

    ASSERT_EQ(2, agg->size());
    ASSERT_EQ("NULL", agg->debug_item(0));
    ASSERT_EQ("1", agg->debug_item(1));

    // third chunk column
    src->reset_column();
    val = 0;
    src->append_numbers(&val, 1);

    aggregator->update_source(src);

    loops.clear();
    loops.emplace_back(1);

    aggregator->aggregate_values(0, 1, loops.data(), false);

    aggregator->finalize();

    ASSERT_EQ(3, agg->size());
    ASSERT_EQ("0", agg->debug_item(2));

    // check agg data and null column
    ASSERT_EQ("[1, 0, 0]", agg->null_column()->debug_string());
    ASSERT_TRUE(agg->data_column()->get(1).get_uint8());
    ASSERT_FALSE(agg->data_column()->get(2).get_uint8());
}

TEST(ColumnAggregator, testNullIntReplaceIfNotNull) {
    FieldPtr field = std::make_shared<Field>(1, "test", LogicalType::TYPE_INT, true);
    field->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_REPLACE_IF_NOT_NULL);

    auto aggregator = ColumnAggregatorFactory::create_value_column_aggregator(field);

    auto src1 = Int32Column::create();
    auto null1 = NullColumn ::create();

    auto src2 = Int32Column::create();
    auto null2 = NullColumn::create();

    auto src3 = Int32Column::create();
    auto null3 = NullColumn::create();

    for (int i = 0; i < 1024; i++) {
        src1->append(i);
        null1->append(0);
    }

    for (int i = 0; i < 1024; i++) {
        src2->append(i);
        null2->append(1);
    }

    for (int i = 0; i < 1024; i++) {
        src3->append(i);
        null3->append(i > 512);
    }

    auto nsrc1 = NullableColumn::create(src1, null1);
    auto nsrc2 = NullableColumn::create(src2, null2);
    auto nsrc3 = NullableColumn::create(src3, null3);

    auto agg1 = NullableColumn::create(Int32Column::create(), NullColumn::create());

    auto dst = down_cast<Int32Column*>(agg1->data_column().get());
    auto ndst = down_cast<NullColumn*>(agg1->null_column().get());

    aggregator->update_aggregate(agg1.get());
    aggregator->update_source(nsrc1);

    std::vector<uint32_t> loops;
    loops.emplace_back(2);
    loops.emplace_back(1022);

    aggregator->aggregate_values(0, 2, loops.data(), false);

    EXPECT_EQ(1, agg1->size());
    EXPECT_EQ(1, dst->get_data()[0]);
    EXPECT_EQ(0, ndst->get_data()[0]);
    EXPECT_EQ(false, agg1->is_null(0));

    aggregator->update_source(nsrc2);

    loops.clear();
    loops.emplace_back(3);
    loops.emplace_back(100);
    loops.emplace_back(921);

    aggregator->aggregate_values(0, 3, loops.data(), false);

    EXPECT_EQ(3, agg1->size());
    EXPECT_EQ(1, dst->get_data()[0]);
    EXPECT_EQ(0, ndst->get_data()[0]);

    EXPECT_EQ(1023, dst->get_data()[1]);
    EXPECT_EQ(0, ndst->get_data()[1]);

    EXPECT_EQ(0, dst->get_data()[2]);
    EXPECT_EQ(1, ndst->get_data()[2]);

    aggregator->update_source(nsrc3);

    loops.clear();
    loops.emplace_back(1);
    loops.emplace_back(1023);

    aggregator->aggregate_values(0, 2, loops.data(), true);

    aggregator->finalize();

    EXPECT_EQ(6, agg1->size());

    EXPECT_EQ(1, dst->get_data()[0]);
    EXPECT_EQ(0, ndst->get_data()[0]);

    EXPECT_EQ(1023, dst->get_data()[1]);
    EXPECT_EQ(0, ndst->get_data()[1]);

    EXPECT_EQ(0, dst->get_data()[2]);
    EXPECT_EQ(1, ndst->get_data()[2]);

    EXPECT_EQ(0, dst->get_data()[3]);
    EXPECT_EQ(1, ndst->get_data()[3]);

    EXPECT_EQ(0, dst->get_data()[4]);
    EXPECT_EQ(0, ndst->get_data()[4]);

    EXPECT_EQ(512, dst->get_data()[5]);
    EXPECT_EQ(0, ndst->get_data()[5]);

    EXPECT_EQ(false, agg1->is_null(0));
    EXPECT_EQ(false, agg1->is_null(1));
    EXPECT_EQ(true, agg1->is_null(2));
    EXPECT_EQ(true, agg1->is_null(3));
    EXPECT_EQ(false, agg1->is_null(4));
    EXPECT_EQ(false, agg1->is_null(5));
}

TEST(ColumnAggregator, testNullIntReplace) {
    FieldPtr field = std::make_shared<Field>(1, "test", LogicalType::TYPE_INT, true);
    field->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_REPLACE);

    auto aggregator = ColumnAggregatorFactory::create_value_column_aggregator(field);

    auto src1 = Int32Column::create();
    auto null1 = NullColumn ::create();

    auto src2 = Int32Column::create();
    auto null2 = NullColumn::create();

    auto src3 = Int32Column::create();
    auto null3 = NullColumn::create();

    for (int i = 0; i < 1024; i++) {
        src1->append(i);
        null1->append(0);
    }

    for (int i = 0; i < 1024; i++) {
        src2->append(i);
        null2->append(1);
    }

    for (int i = 0; i < 1024; i++) {
        src3->append(i);
        null3->append(i > 512);
    }

    auto nsrc1 = NullableColumn::create(src1, null1);
    auto nsrc2 = NullableColumn::create(src2, null2);
    auto nsrc3 = NullableColumn::create(src3, null3);

    auto agg1 = NullableColumn::create(Int32Column::create(), NullColumn::create());

    auto dst = down_cast<Int32Column*>(agg1->data_column().get());
    auto ndst = down_cast<NullColumn*>(agg1->null_column().get());

    aggregator->update_aggregate(agg1.get());
    aggregator->update_source(nsrc1);

    std::vector<uint32_t> loops;
    loops.emplace_back(2);
    loops.emplace_back(1022);

    aggregator->aggregate_values(0, 2, loops.data(), false);

    EXPECT_EQ(1, agg1->size());
    EXPECT_EQ(1, dst->get_data()[0]);
    EXPECT_EQ(0, ndst->get_data()[0]);
    EXPECT_EQ(false, agg1->is_null(0));

    aggregator->update_source(nsrc2);

    loops.clear();
    loops.emplace_back(3);
    loops.emplace_back(100);
    loops.emplace_back(921);

    aggregator->aggregate_values(0, 3, loops.data(), false);

    EXPECT_EQ(3, agg1->size());
    EXPECT_EQ(1, dst->get_data()[0]);
    EXPECT_EQ(0, ndst->get_data()[0]);

    EXPECT_EQ(2, dst->get_data()[1]);
    EXPECT_EQ(1, ndst->get_data()[1]);

    EXPECT_EQ(102, dst->get_data()[2]);
    EXPECT_EQ(1, ndst->get_data()[2]);

    aggregator->update_source(nsrc3);

    loops.clear();
    loops.emplace_back(1);
    loops.emplace_back(1023);

    aggregator->aggregate_values(0, 2, loops.data(), true);

    aggregator->finalize();

    EXPECT_EQ(6, agg1->size());

    EXPECT_EQ(1, dst->get_data()[0]);
    EXPECT_EQ(0, ndst->get_data()[0]);

    EXPECT_EQ(2, dst->get_data()[1]);
    EXPECT_EQ(1, ndst->get_data()[1]);

    EXPECT_EQ(102, dst->get_data()[2]);
    EXPECT_EQ(1, ndst->get_data()[2]);

    EXPECT_EQ(1023, dst->get_data()[3]);
    EXPECT_EQ(1, ndst->get_data()[3]);

    EXPECT_EQ(0, dst->get_data()[4]);
    EXPECT_EQ(0, ndst->get_data()[4]);

    EXPECT_EQ(1023, dst->get_data()[5]);
    EXPECT_EQ(1, ndst->get_data()[5]);

    EXPECT_EQ(false, agg1->is_null(0));
    EXPECT_EQ(true, agg1->is_null(1));
    EXPECT_EQ(true, agg1->is_null(2));
    EXPECT_EQ(true, agg1->is_null(3));
    EXPECT_EQ(false, agg1->is_null(4));
    EXPECT_EQ(true, agg1->is_null(5));
}

TEST(ColumnAggregator, testArrayReplace) {
    auto array_type_info = get_array_type_info(get_type_info(LogicalType::TYPE_VARCHAR));
    FieldPtr field = std::make_shared<Field>(1, "test_array", array_type_info,
                                             StorageAggregateType::STORAGE_AGGREGATE_REPLACE, 1, false, false);

    auto agg_elements = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    auto agg_offsets = UInt32Column::create();
    auto agg = ArrayColumn::create(agg_elements, agg_offsets);

    auto aggregator = ColumnAggregatorFactory::create_value_column_aggregator(field);
    aggregator->update_aggregate(agg.get());
    std::vector<uint32_t> loops;

    // first chunk column
    auto elements = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    auto offsets = UInt32Column::create();
    auto src = ArrayColumn::create(elements, offsets);
    for (int i = 0; i < 10; ++i) {
        elements->append_datum(Slice(std::to_string(i)));
    }
    offsets->append(2);
    offsets->append(5);
    offsets->append(10);

    aggregator->update_source(src);

    loops.clear();
    loops.emplace_back(2);
    loops.emplace_back(1);

    aggregator->aggregate_values(0, 2, loops.data(), false);

    ASSERT_EQ(1, agg->size());
    EXPECT_EQ("['2','3','4']", agg->debug_item(0));

    // second chunk column
    src->reset_column();
    for (int i = 10; i < 20; ++i) {
        elements->append_datum(Slice(std::to_string(i)));
    }
    offsets->append(2);
    offsets->append(7);
    offsets->append(9);
    offsets->append(10);

    aggregator->update_source(src);

    loops.clear();
    loops.emplace_back(1);
    loops.emplace_back(2);
    loops.emplace_back(1);

    aggregator->aggregate_values(0, 3, loops.data(), false);

    EXPECT_EQ(3, agg->size());
    EXPECT_EQ("['10','11']", agg->debug_item(1));
    EXPECT_EQ("['17','18']", agg->debug_item(2));

    // third chunk column
    src->reset_column();
    for (int i = 20; i < 30; ++i) {
        elements->append_datum(Slice(std::to_string(i)));
    }
    offsets->append(10);

    aggregator->update_source(src);

    loops.clear();
    loops.emplace_back(1);

    aggregator->aggregate_values(0, 1, loops.data(), true);

    aggregator->finalize();

    EXPECT_EQ(5, agg->size());
    EXPECT_EQ("['19']", agg->debug_item(3));
    EXPECT_EQ("['20','21','22','23','24','25','26','27','28','29']", agg->debug_item(4));
}

// NOLINTNEXTLINE
TEST(ColumnAggregator, testNullArrayReplaceIfNotNull2) {
    auto array_type_info = get_array_type_info(get_type_info(LogicalType::TYPE_INT));
    FieldPtr field =
            std::make_shared<Field>(1, "test_array", array_type_info,
                                    StorageAggregateType::STORAGE_AGGREGATE_REPLACE_IF_NOT_NULL, 1, false, true);
    auto agg = NullableColumn::create(
            ArrayColumn::create(NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                UInt32Column::create()),
            NullColumn::create());
    auto aggregator = ColumnAggregatorFactory::create_value_column_aggregator(field);
    aggregator->update_aggregate(agg.get());

    // first chunk column
    auto src = NullableColumn::create(
            ArrayColumn::create(NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                UInt32Column::create()),
            NullColumn::create());
    DatumArray array_3{Datum((int32_t)(3))};
    DatumArray array_4{Datum((int32_t)(4))};
    DatumArray array_8{Datum((int32_t)(8))};
    DatumArray array_11{Datum((int32_t)(11))};
    DatumArray array_13{Datum((int32_t)(13))};
    DatumArray array_14{Datum((int32_t)(14))};
    DatumArray array_15{Datum((int32_t)(15))};

    src->append_nulls(1);
    src->append_datum(Datum(array_3));
    src->append_datum(Datum(array_4));
    src->append_datum(Datum(array_8));
    src->append_nulls(1);

    aggregator->update_source(src);

    std::vector<uint32_t> loops{1, 1, 1, 2};

    aggregator->aggregate_values(0, 4, loops.data(), false);

    src->reset_column();

    src->append_nulls(1);
    src->append_datum(Datum(array_11));
    src->append_datum(Datum(array_13));
    src->append_datum(Datum(array_14));
    src->append_datum(Datum(array_15));

    aggregator->update_source(src);

    loops.clear();
    loops.emplace_back(1);
    loops.emplace_back(1);
    loops.emplace_back(1);
    loops.emplace_back(1);
    loops.emplace_back(1);

    aggregator->aggregate_values(0, 1, loops.data(), false);
    aggregator->finalize();

    ASSERT_EQ(agg->size(), 4);
    ASSERT_TRUE(agg->get(0).is_null());
    ASSERT_EQ(agg->get(1).get_array()[0].get_int32(), 3);
    ASSERT_EQ(agg->get(2).get_array()[0].get_int32(), 4);
    ASSERT_EQ(agg->get(3).get_array()[0].get_int32(), 8);

    agg->reset_column();
    aggregator->update_aggregate(agg.get());

    aggregator->aggregate_values(1, 4, loops.data(), false);
    aggregator->finalize();

    ASSERT_EQ(agg->size(), 4);
    ASSERT_EQ(agg->get(0).get_array()[0].get_int32(), 11);
    ASSERT_EQ(agg->get(1).get_array()[0].get_int32(), 13);
    ASSERT_EQ(agg->get(2).get_array()[0].get_int32(), 14);
    ASSERT_EQ(agg->get(3).get_array()[0].get_int32(), 15);
}

// insert into tbl values (key, null);
TEST(ColumnAggregator, testNullArrayReplaceIfNotNull) {
    auto array_type_info = get_array_type_info(get_type_info(LogicalType::TYPE_VARCHAR));
    FieldPtr field =
            std::make_shared<Field>(1, "test_array", array_type_info,
                                    StorageAggregateType::STORAGE_AGGREGATE_REPLACE_IF_NOT_NULL, 1, false, true);

    auto agg = NullableColumn::create(
            ArrayColumn::create(NullableColumn::create(BinaryColumn::create(), NullColumn::create()),
                                UInt32Column::create()),
            NullColumn::create());
    auto aggregator = ColumnAggregatorFactory::create_value_column_aggregator(field);
    aggregator->update_aggregate(agg.get());
    std::vector<uint32_t> loops;

    // first chunk column
    auto src = NullableColumn::create(
            ArrayColumn::create(NullableColumn::create(BinaryColumn::create(), NullColumn::create()),
                                UInt32Column::create()),
            NullColumn::create());
    src->append_nulls(1);

    aggregator->update_source(src);

    loops.clear();
    loops.emplace_back(1);

    aggregator->aggregate_values(0, 1, loops.data(), false);

    ASSERT_EQ(0, agg->size());

    aggregator->finalize();

    ASSERT_EQ(1, agg->size());
    ASSERT_EQ("NULL", agg->debug_item(0));
}

} // namespace starrocks
