// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/vectorized/chunk_aggregator.h"

#include <memory>
#include <vector>

#include "column/column_helper.h"
#include "gtest/gtest.h"
#include "storage/vectorized/column_aggregate_func.h"

namespace starrocks::vectorized {

TEST(ColumnAggregator, testNoneAggregator) {
    FieldPtr key = std::make_shared<Field>(1, "key", FieldType::OLAP_FIELD_TYPE_INT, false);
    key->set_aggregate_method(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE);
    key->set_is_key(true);

    FieldPtr value = std::make_shared<Field>(1, "value", FieldType::OLAP_FIELD_TYPE_INT, false);
    value->set_aggregate_method(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_SUM);
    value->set_is_key(false);

    Fields fields;
    fields.emplace_back(key);
    fields.emplace_back(value);

    SchemaPtr schema = std::make_shared<Schema>(fields);

    ChunkAggregator aggregator(schema.get(), 1024, 0.9);

    ASSERT_EQ(true, aggregator.source_exhausted());

    auto k_col = Int32Column::create();
    auto v_col = Int32Column::create();

    for (int i = 0; i < 1024; ++i) {
        k_col->append(1);
        v_col->append(1);
    }

    Columns cols{k_col, v_col};

    ChunkPtr chunk = std::make_shared<Chunk>(cols, schema);

    aggregator.update_source(chunk);
    ASSERT_EQ(true, aggregator.is_do_aggregate());

    ASSERT_EQ(false, aggregator.source_exhausted());

    aggregator.aggregate();

    ASSERT_EQ(true, aggregator.source_exhausted());

    ASSERT_EQ(true, aggregator.has_aggregate_data());
    ASSERT_EQ(false, aggregator.is_finish());

    auto ck = aggregator.aggregate_result();

    ASSERT_EQ(1, ck->num_rows());

    auto r_col = down_cast<Int32Column*>(ck->get_column_by_index(1).get());

    ASSERT_EQ(1024, r_col->get_data()[0]);

    ASSERT_EQ(false, aggregator.is_finish());
}

} // namespace starrocks::vectorized
