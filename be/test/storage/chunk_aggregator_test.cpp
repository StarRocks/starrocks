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

#include "storage/chunk_aggregator.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "column/column_helper.h"
#include "storage/column_aggregate_func.h"

namespace starrocks {

TEST(ChunkAggregatorTest, testNoneAggregator) {
    FieldPtr key = std::make_shared<Field>(1, "key", LogicalType::TYPE_INT, false);
    key->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_NONE);
    key->set_is_key(true);

    FieldPtr value = std::make_shared<Field>(1, "value", LogicalType::TYPE_INT, false);
    value->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_SUM);
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

    Columns cols({std::move(k_col), std::move(v_col)});

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

TEST(ChunkAggregatorTest, testNonKeyColumnsByMask) {
    FieldPtr value = std::make_shared<Field>(1, "value", LogicalType::TYPE_INT, false);
    value->set_aggregate_method(StorageAggregateType::STORAGE_AGGREGATE_SUM);
    value->set_is_key(false);

    Fields fields;
    fields.emplace_back(value);

    SchemaPtr schema = std::make_shared<Schema>(fields);

    ChunkAggregator aggregator(schema.get(), 1024, 0, true, false);

    ASSERT_EQ(true, aggregator.source_exhausted());

    auto v_col = Int32Column::create();
    std::vector<RowSourceMask> source_masks;
    for (int i = 0; i < 1024; ++i) {
        v_col->append(1);
        if (i % 2 == 0) {
            source_masks.emplace_back(RowSourceMask{0, false});
        } else {
            source_masks.emplace_back(RowSourceMask{0, true});
        }
    }
    Columns cols{std::move(v_col)};
    ChunkPtr chunk = std::make_shared<Chunk>(cols, schema);

    aggregator.update_source(chunk, &source_masks);
    ASSERT_EQ(true, aggregator.is_do_aggregate());
    ASSERT_EQ(false, aggregator.source_exhausted());

    aggregator.aggregate();

    ASSERT_EQ(true, aggregator.source_exhausted());
    ASSERT_EQ(true, aggregator.has_aggregate_data());
    ASSERT_EQ(false, aggregator.is_finish());
    ASSERT_EQ(512, aggregator._aggregate_rows);

    auto ck = aggregator.aggregate_result();
    ASSERT_EQ(512, ck->num_rows());
    auto r_col = down_cast<Int32Column*>(ck->get_column_by_index(0).get());
    for (size_t i = 0; i < ck->num_rows(); ++i) {
        ASSERT_EQ(2, r_col->get_data()[i]);
    }

    ASSERT_EQ(false, aggregator.is_finish());
}

} // namespace starrocks
