// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include <memory>
#include <vector>

#include "column/column_helper.h"
#include "gtest/gtest.h"
#include "storage/vectorized/column_aggregate_func.h"

namespace starrocks::vectorized {

TEST(ColumnAggregator, testIntSum) {
    FieldPtr field = std::make_shared<Field>(1, "test", FieldType::OLAP_FIELD_TYPE_INT, false);
    field->set_aggregate_method(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_SUM);

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
    FieldPtr field = std::make_shared<Field>(1, "test", FieldType::OLAP_FIELD_TYPE_INT, true);
    field->set_aggregate_method(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_SUM);

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
    FieldPtr field = std::make_shared<Field>(1, "test", FieldType::OLAP_FIELD_TYPE_INT, false);
    field->set_aggregate_method(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_MAX);

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
    FieldPtr field = std::make_shared<Field>(1, "test", FieldType::OLAP_FIELD_TYPE_VARCHAR, false);
    field->set_aggregate_method(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_MIN);

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

TEST(ColumnAggregator, testNullIntReplaceIfNotNull) {
    FieldPtr field = std::make_shared<Field>(1, "test", FieldType::OLAP_FIELD_TYPE_INT, true);
    field->set_aggregate_method(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL);

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
    FieldPtr field = std::make_shared<Field>(1, "test", FieldType::OLAP_FIELD_TYPE_INT, true);
    field->set_aggregate_method(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE);

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

} // namespace starrocks::vectorized
