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
#include "types/map_type_info.h"
#include "types/struct_type_info.h"

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

    ASSERT_EQ(1, ndst->get_data()[2]);

    ASSERT_EQ(1, ndst->get_data()[3]);

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

    EXPECT_EQ(1, ndst->get_data()[2]);

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

// NOLINTNEXTLINE
TEST(ColumnAggregator, testNullArrayFirstIfNotNull2) {
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

// test first
TEST(ColumnAggregator, testFirstTpeDispatch) {
    FieldPtr field1 = std::make_shared<Field>(1, "test3", LogicalType::TYPE_TINYINT, false);
    field1->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_FIRST);
    auto aggregator1 = ColumnAggregatorFactory::create_value_column_aggregator(field1);
    ASSERT_TRUE(aggregator1);

    FieldPtr field2 = std::make_shared<Field>(2, "test3", LogicalType::TYPE_SMALLINT, false);
    field2->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_FIRST);
    auto aggregator2 = ColumnAggregatorFactory::create_value_column_aggregator(field2);
    ASSERT_TRUE(aggregator2);

    FieldPtr field3 = std::make_shared<Field>(3, "test3", LogicalType::TYPE_INT, false);
    field3->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_FIRST);
    auto aggregator3 = ColumnAggregatorFactory::create_value_column_aggregator(field3);
    ASSERT_TRUE(aggregator3);

    FieldPtr field4 = std::make_shared<Field>(3, "test3", LogicalType::TYPE_BIGINT, false);
    field4->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_FIRST);
    auto aggregator4 = ColumnAggregatorFactory::create_value_column_aggregator(field4);
    ASSERT_TRUE(aggregator4);

    FieldPtr field5 = std::make_shared<Field>(3, "test3", LogicalType::TYPE_LARGEINT, false);
    field5->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_FIRST);
    auto aggregator5 = ColumnAggregatorFactory::create_value_column_aggregator(field5);
    ASSERT_TRUE(aggregator5);

    FieldPtr field6 = std::make_shared<Field>(3, "test3", LogicalType::TYPE_FLOAT, false);
    field6->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_FIRST);
    auto aggregator6 = ColumnAggregatorFactory::create_value_column_aggregator(field6);
    ASSERT_TRUE(aggregator6);

    FieldPtr field7 = std::make_shared<Field>(3, "test3", LogicalType::TYPE_DOUBLE, false);
    field7->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_FIRST);
    auto aggregator7 = ColumnAggregatorFactory::create_value_column_aggregator(field7);
    ASSERT_TRUE(aggregator7);

    FieldPtr field8 = std::make_shared<Field>(3, "test3", LogicalType::TYPE_DECIMAL, false);
    field8->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_FIRST);
    auto aggregator8 = ColumnAggregatorFactory::create_value_column_aggregator(field8);
    ASSERT_TRUE(aggregator8);

    FieldPtr field9 = std::make_shared<Field>(3, "test3", LogicalType::TYPE_DECIMALV2, false);
    field9->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_FIRST);
    auto aggregator9 = ColumnAggregatorFactory::create_value_column_aggregator(field9);
    ASSERT_TRUE(aggregator9);

    auto decimal32_type_info = get_type_info(LogicalType::TYPE_DECIMAL32);
    FieldPtr field10 = std::make_shared<Field>(1, "test3", decimal32_type_info,
                                               StorageAggregateType::STORAGE_AGGREGATE_FIRST, 1, false, false);
    auto aggregator10 = ColumnAggregatorFactory::create_value_column_aggregator(field10);
    ASSERT_TRUE(aggregator10);

    auto decimal64_type_info = get_type_info(LogicalType::TYPE_DECIMAL64);
    FieldPtr field11 = std::make_shared<Field>(1, "test3", decimal64_type_info,
                                               StorageAggregateType::STORAGE_AGGREGATE_FIRST, 1, false, false);
    auto aggregator11 = ColumnAggregatorFactory::create_value_column_aggregator(field11);
    ASSERT_TRUE(aggregator11);

    auto decimal128_type_info = get_type_info(LogicalType::TYPE_DECIMAL128);
    FieldPtr field12 = std::make_shared<Field>(1, "test3", decimal128_type_info,
                                               StorageAggregateType::STORAGE_AGGREGATE_FIRST, 1, false, false);
    auto aggregator12 = ColumnAggregatorFactory::create_value_column_aggregator(field12);
    ASSERT_TRUE(aggregator12);

    FieldPtr field13 = std::make_shared<Field>(3, "test3", LogicalType::TYPE_DATE_V1, false);
    field13->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_FIRST);
    auto aggregator13 = ColumnAggregatorFactory::create_value_column_aggregator(field13);
    ASSERT_TRUE(aggregator13);

    FieldPtr field14 = std::make_shared<Field>(3, "test3", LogicalType::TYPE_DATE, false);
    field14->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_FIRST);
    auto aggregator14 = ColumnAggregatorFactory::create_value_column_aggregator(field14);
    ASSERT_TRUE(aggregator14);

    FieldPtr field15 = std::make_shared<Field>(3, "test3", LogicalType::TYPE_DATETIME_V1, false);
    field15->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_FIRST);
    auto aggregator15 = ColumnAggregatorFactory::create_value_column_aggregator(field15);
    ASSERT_TRUE(aggregator15);

    FieldPtr field16 = std::make_shared<Field>(3, "test3", LogicalType::TYPE_DATETIME, false);
    field16->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_FIRST);
    auto aggregator16 = ColumnAggregatorFactory::create_value_column_aggregator(field16);
    ASSERT_TRUE(aggregator16);

    FieldPtr field17 = std::make_shared<Field>(3, "test3", LogicalType::TYPE_CHAR, false);
    field17->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_FIRST);
    auto aggregator17 = ColumnAggregatorFactory::create_value_column_aggregator(field17);
    ASSERT_TRUE(aggregator17);

    FieldPtr field18 = std::make_shared<Field>(3, "test3", LogicalType::TYPE_VARCHAR, false);
    field18->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_FIRST);
    auto aggregator18 = ColumnAggregatorFactory::create_value_column_aggregator(field18);
    ASSERT_TRUE(aggregator18);

    FieldPtr field19 = std::make_shared<Field>(3, "test3", LogicalType::TYPE_VARBINARY, false);
    field19->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_FIRST);
    auto aggregator19 = ColumnAggregatorFactory::create_value_column_aggregator(field19);
    ASSERT_TRUE(aggregator19);

    FieldPtr field20 = std::make_shared<Field>(3, "test3", LogicalType::TYPE_BOOLEAN, false);
    field20->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_FIRST);
    auto aggregator20 = ColumnAggregatorFactory::create_value_column_aggregator(field20);
    ASSERT_TRUE(aggregator20);

    auto array_type_info = get_array_type_info(get_type_info(LogicalType::TYPE_INT));
    FieldPtr field21 = std::make_shared<Field>(1, "test3", array_type_info,
                                               StorageAggregateType::STORAGE_AGGREGATE_FIRST, 1, false, false);
    auto aggregator21 = ColumnAggregatorFactory::create_value_column_aggregator(field21);
    ASSERT_TRUE(aggregator21);

    auto map_type_info = get_map_type_info(get_type_info(LogicalType::TYPE_INT), get_type_info(LogicalType::TYPE_INT));
    FieldPtr field22 = std::make_shared<Field>(1, "test3", map_type_info, StorageAggregateType::STORAGE_AGGREGATE_FIRST,
                                               1, false, false);
    auto aggregator22 = ColumnAggregatorFactory::create_value_column_aggregator(field22);
    ASSERT_TRUE(aggregator22);

    std::vector<TypeInfoPtr> field_types;
    field_types.emplace_back(get_type_info(LogicalType::TYPE_INT));
    field_types.emplace_back(get_type_info(LogicalType::TYPE_VARCHAR));
    auto struct_type_info = get_struct_type_info(std::move(field_types));
    FieldPtr field23 = std::make_shared<Field>(1, "test3", struct_type_info,
                                               StorageAggregateType::STORAGE_AGGREGATE_FIRST, 1, false, false);
    auto aggregator23 = ColumnAggregatorFactory::create_value_column_aggregator(field23);
    ASSERT_TRUE(aggregator23);

    FieldPtr field24 = std::make_shared<Field>(3, "test3", LogicalType::TYPE_HLL, false);
    field24->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_FIRST);
    auto aggregator24 = ColumnAggregatorFactory::create_value_column_aggregator(field24);
    ASSERT_TRUE(aggregator24);

    FieldPtr field25 = std::make_shared<Field>(3, "test3", LogicalType::TYPE_OBJECT, false);
    field25->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_FIRST);
    auto aggregator25 = ColumnAggregatorFactory::create_value_column_aggregator(field25);
    ASSERT_TRUE(aggregator25);

    FieldPtr field26 = std::make_shared<Field>(3, "test3", LogicalType::TYPE_PERCENTILE, false);
    field26->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_FIRST);
    auto aggregator26 = ColumnAggregatorFactory::create_value_column_aggregator(field26);
    ASSERT_TRUE(aggregator26);

    FieldPtr field27 = std::make_shared<Field>(3, "test3", LogicalType::TYPE_JSON, false);
    field27->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_FIRST);
    auto aggregator27 = ColumnAggregatorFactory::create_value_column_aggregator(field27);
    ASSERT_TRUE(aggregator27);
}

TEST(ColumnAggregator, testStringFirst) {
    FieldPtr field = std::make_shared<Field>(1, "test", LogicalType::TYPE_VARCHAR, false);
    field->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_FIRST);

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

TEST(ColumnAggregator, testBitmapFirst) {
    FieldPtr field = std::make_shared<Field>(1, "test", LogicalType::TYPE_OBJECT, false);
    field->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_FIRST);

    auto aggregator = ColumnAggregatorFactory::create_value_column_aggregator(field);

    auto src1 = BitmapColumn::create();
    auto src2 = BitmapColumn::create();
    auto src3 = BitmapColumn::create();

    for (int i = 0; i < 1024; i++) {
        BitmapValue b1;
        b1.add(i + 1000);
        b1.add(i + 10000);
        src1->append(&b1);
        BitmapValue b2;
        b2.add(i + 3000);
        b2.add(i + 30000);
        src2->append(&b2);
        BitmapValue b3;
        b3.add(i + 2000);
        b3.add(i + 20000);
        src3->append(&b3);
    }

    auto agg1 = BitmapColumn::create();

    aggregator->update_aggregate(agg1.get());
    aggregator->update_source(src1);

    std::vector<uint32_t> loops;
    loops.emplace_back(2);
    loops.emplace_back(1022);

    aggregator->aggregate_values(0, 2, loops.data(), false);

    ASSERT_EQ(1, agg1->size());
    ASSERT_EQ("1000,10000", agg1->debug_item(0));

    aggregator->update_source(src2);

    loops.clear();
    loops.emplace_back(3);
    loops.emplace_back(100);
    loops.emplace_back(921);

    aggregator->aggregate_values(0, 3, loops.data(), false);

    EXPECT_EQ(3, agg1->size());
    EXPECT_EQ("1000,10000", agg1->debug_item(0));
    EXPECT_EQ("1002,10002", agg1->debug_item(1));
    EXPECT_EQ("3003,30003", agg1->debug_item(2));

    aggregator->update_source(src3);

    loops.clear();
    loops.emplace_back(1);
    loops.emplace_back(1023);

    aggregator->aggregate_values(0, 2, loops.data(), true);

    aggregator->finalize();

    EXPECT_EQ(6, agg1->size());
    EXPECT_EQ("1000,10000", agg1->debug_item(0));
    EXPECT_EQ("1002,10002", agg1->debug_item(1));
    EXPECT_EQ("3003,30003", agg1->debug_item(2));
    EXPECT_EQ("3103,30103", agg1->debug_item(3));
    EXPECT_EQ("2000,20000", agg1->debug_item(4));
    EXPECT_EQ("2001,20001", agg1->debug_item(5));
}

TEST(ColumnAggregator, testJsonFirst) {
    FieldPtr field = std::make_shared<Field>(1, "test", LogicalType::TYPE_JSON, false);
    field->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_FIRST);

    auto aggregator = ColumnAggregatorFactory::create_value_column_aggregator(field);

    auto src1 = JsonColumn::create();
    auto src2 = JsonColumn::create();
    auto src3 = JsonColumn::create();

    for (int i = 0; i < 1024; i++) {
        JsonValue j1 = JsonValue::from_string(R"({"k1":)" + std::to_string(i + 1000) + R"("k2": )" +
                                              std::to_string(10000 + i) + "}");
        src1->append(&j1);
        JsonValue j2 = JsonValue::from_string(R"({"k1":)" + std::to_string(i + 3000) + R"("k2": )" +
                                              std::to_string(30000 + i) + "}");
        src2->append(&j2);
        JsonValue j3 = JsonValue::from_string(R"({"k1":)" + std::to_string(i + 2000) + R"("k2": )" +
                                              std::to_string(20000 + i) + "}");
        src3->append(&j3);
    }

    auto agg1 = JsonColumn::create();

    aggregator->update_aggregate(agg1.get());
    aggregator->update_source(src1);

    std::vector<uint32_t> loops;
    loops.emplace_back(2);
    loops.emplace_back(1022);

    aggregator->aggregate_values(0, 2, loops.data(), false);

    ASSERT_EQ(1, agg1->size());
    ASSERT_EQ("\"{\\\"k1\\\":1000\\\"k2\\\": 10000}\"", agg1->debug_item(0));

    aggregator->update_source(src2);

    loops.clear();
    loops.emplace_back(3);
    loops.emplace_back(100);
    loops.emplace_back(921);

    aggregator->aggregate_values(0, 3, loops.data(), false);

    EXPECT_EQ(3, agg1->size());
    EXPECT_EQ("\"{\\\"k1\\\":1000\\\"k2\\\": 10000}\"", agg1->debug_item(0));
    EXPECT_EQ("\"{\\\"k1\\\":1002\\\"k2\\\": 10002}\"", agg1->debug_item(1));
    EXPECT_EQ("\"{\\\"k1\\\":3003\\\"k2\\\": 30003}\"", agg1->debug_item(2));

    aggregator->update_source(src3);

    loops.clear();
    loops.emplace_back(1);
    loops.emplace_back(1023);

    aggregator->aggregate_values(0, 2, loops.data(), true);

    aggregator->finalize();

    EXPECT_EQ(6, agg1->size());
    EXPECT_EQ("\"{\\\"k1\\\":1000\\\"k2\\\": 10000}\"", agg1->debug_item(0));
    EXPECT_EQ("\"{\\\"k1\\\":1002\\\"k2\\\": 10002}\"", agg1->debug_item(1));
    EXPECT_EQ("\"{\\\"k1\\\":3003\\\"k2\\\": 30003}\"", agg1->debug_item(2));
    EXPECT_EQ("\"{\\\"k1\\\":3103\\\"k2\\\": 30103}\"", agg1->debug_item(3));
    EXPECT_EQ("\"{\\\"k1\\\":2000\\\"k2\\\": 20000}\"", agg1->debug_item(4));
    EXPECT_EQ("\"{\\\"k1\\\":2001\\\"k2\\\": 20001}\"", agg1->debug_item(5));
}

TEST(ColumnAggregator, testHLLFirst) {
    FieldPtr field = std::make_shared<Field>(1, "test", LogicalType::TYPE_HLL, false);
    field->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_FIRST);

    auto aggregator = ColumnAggregatorFactory::create_value_column_aggregator(field);

    auto src1 = HyperLogLogColumn::create();
    auto src2 = HyperLogLogColumn::create();
    auto src3 = HyperLogLogColumn::create();

    for (int i = 0; i < 1024; i++) {
        HyperLogLog h1;
        h1.update(i + 1000);
        src1->append(&h1);
        HyperLogLog h2;
        h2.update(i + 3000);
        src2->append(&h2);
        HyperLogLog h3;
        h3.update(i + 2000);
        src3->append(&h3);
    }

    auto agg1 = HyperLogLogColumn::create();

    aggregator->update_aggregate(agg1.get());
    aggregator->update_source(src1);

    std::vector<uint32_t> loops;
    loops.emplace_back(2);
    loops.emplace_back(1022);

    aggregator->aggregate_values(0, 2, loops.data(), false);

    ASSERT_EQ(1, agg1->size());

    aggregator->update_source(src2);

    loops.clear();
    loops.emplace_back(3);
    loops.emplace_back(100);
    loops.emplace_back(921);

    aggregator->aggregate_values(0, 3, loops.data(), false);

    EXPECT_EQ(3, agg1->size());

    aggregator->update_source(src3);

    loops.clear();
    loops.emplace_back(1);
    loops.emplace_back(1023);

    aggregator->aggregate_values(0, 2, loops.data(), true);

    aggregator->finalize();

    EXPECT_EQ(6, agg1->size());
}

TEST(ColumnAggregator, testPercentileFirst) {
    FieldPtr field = std::make_shared<Field>(1, "test", LogicalType::TYPE_PERCENTILE, false);
    field->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_FIRST);

    auto aggregator = ColumnAggregatorFactory::create_value_column_aggregator(field);

    auto src1 = PercentileColumn::create();
    auto src2 = PercentileColumn::create();
    auto src3 = PercentileColumn::create();

    for (int i = 0; i < 1024; i++) {
        src1->append_default();
        src2->append_default();
        src3->append_default();
    }

    auto agg1 = PercentileColumn::create();

    aggregator->update_aggregate(agg1.get());
    aggregator->update_source(src1);

    std::vector<uint32_t> loops;
    loops.emplace_back(2);
    loops.emplace_back(1022);

    aggregator->aggregate_values(0, 2, loops.data(), false);

    ASSERT_EQ(1, agg1->size());

    aggregator->update_source(src2);

    loops.clear();
    loops.emplace_back(3);
    loops.emplace_back(100);
    loops.emplace_back(921);

    aggregator->aggregate_values(0, 3, loops.data(), false);

    EXPECT_EQ(3, agg1->size());

    aggregator->update_source(src3);

    loops.clear();
    loops.emplace_back(1);
    loops.emplace_back(1023);

    aggregator->aggregate_values(0, 2, loops.data(), true);

    aggregator->finalize();

    EXPECT_EQ(6, agg1->size());
}

TEST(ColumnAggregator, testNullIntFirst) {
    FieldPtr field = std::make_shared<Field>(1, "test", LogicalType::TYPE_INT, true);
    field->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_FIRST);

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
    EXPECT_EQ(0, dst->get_data()[0]);
    EXPECT_EQ(0, ndst->get_data()[0]);
    EXPECT_EQ(false, agg1->is_null(0));

    aggregator->update_source(nsrc2);

    loops.clear();
    loops.emplace_back(3);
    loops.emplace_back(100);
    loops.emplace_back(921);

    aggregator->aggregate_values(0, 3, loops.data(), false);

    EXPECT_EQ(3, agg1->size());
    EXPECT_EQ(0, dst->get_data()[0]);
    EXPECT_EQ(0, ndst->get_data()[0]);

    EXPECT_EQ(2, dst->get_data()[1]);
    EXPECT_EQ(0, ndst->get_data()[1]);

    EXPECT_EQ(3, dst->get_data()[2]);
    EXPECT_EQ(1, ndst->get_data()[2]);

    aggregator->update_source(nsrc3);

    loops.clear();
    loops.emplace_back(1);
    loops.emplace_back(1023);

    aggregator->aggregate_values(0, 2, loops.data(), true);

    aggregator->finalize();

    EXPECT_EQ(6, agg1->size());

    EXPECT_EQ(0, dst->get_data()[0]);
    EXPECT_EQ(0, ndst->get_data()[0]);

    EXPECT_EQ(2, dst->get_data()[1]);
    EXPECT_EQ(0, ndst->get_data()[1]);

    EXPECT_EQ(3, dst->get_data()[2]);
    EXPECT_EQ(1, ndst->get_data()[2]);

    EXPECT_EQ(103, dst->get_data()[3]);
    EXPECT_EQ(1, ndst->get_data()[3]);

    EXPECT_EQ(0, dst->get_data()[4]);
    EXPECT_EQ(0, ndst->get_data()[4]);

    EXPECT_EQ(1, dst->get_data()[5]);
    EXPECT_EQ(0, ndst->get_data()[5]);

    EXPECT_EQ(false, agg1->is_null(0));
    EXPECT_EQ(false, agg1->is_null(1));
    EXPECT_EQ(true, agg1->is_null(2));
    EXPECT_EQ(true, agg1->is_null(3));
    EXPECT_EQ(false, agg1->is_null(4));
    EXPECT_EQ(false, agg1->is_null(5));
}

TEST(ColumnAggregator, testArrayFirst) {
    auto array_type_info = get_array_type_info(get_type_info(LogicalType::TYPE_VARCHAR));
    FieldPtr field = std::make_shared<Field>(1, "test_array", array_type_info,
                                             StorageAggregateType::STORAGE_AGGREGATE_FIRST, 1, false, false);

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
    EXPECT_EQ("['0','1']", agg->debug_item(0));

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
    EXPECT_EQ("['5','6','7','8','9']", agg->debug_item(1));
    EXPECT_EQ("['12','13','14','15','16']", agg->debug_item(2));

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
} // namespace starrocks
