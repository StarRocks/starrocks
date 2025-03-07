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

#include "storage/aggregate_iterator.h"

#include "common/config.h"
#include "gtest/gtest.h"
#include "storage/aggregate_type.h"
#include "storage/vector_chunk_iterator.h"

namespace starrocks {

static std::vector<Datum> row(const Chunk& chunk, size_t row_id) {
    std::vector<Datum> result;
    for (size_t i = 0; i < chunk.num_columns(); i++) {
        if (chunk.get_column_by_index(i)->is_null(row_id)) {
            result.emplace_back(Datum());
        } else {
            result.emplace_back(chunk.get_column_by_index(i)->get(row_id));
        }
    }
    return result;
}

class AggregateIteratorTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

// NOLINTNEXTLINE
TEST_F(AggregateIteratorTest, agg_max) {
    config::vector_chunk_size = 1024;

    auto k1 = std::make_shared<Field>(0, "k1", TYPE_BIGINT, false);
    auto c1 = std::make_shared<Field>(1, "c1", TYPE_SMALLINT, false);
    auto c2 = std::make_shared<Field>(2, "c2", TYPE_LARGEINT, false);
    auto c3 = std::make_shared<Field>(3, "c3", TYPE_FLOAT, false);
    auto c4 = std::make_shared<Field>(4, "c4", TYPE_DOUBLE, false);
    auto c5 = std::make_shared<Field>(5, "c5", TYPE_DECIMALV2, false);
    auto c6 = std::make_shared<Field>(6, "c6", TYPE_DATE, false);
    auto c7 = std::make_shared<Field>(7, "c7", TYPE_DATETIME, false);
    auto c8 = std::make_shared<Field>(8, "c8", TYPE_VARCHAR, false);

    k1->set_is_key(true);
    k1->set_aggregate_method(STORAGE_AGGREGATE_NONE);
    c1->set_aggregate_method(STORAGE_AGGREGATE_MAX);
    c2->set_aggregate_method(STORAGE_AGGREGATE_MAX);
    c3->set_aggregate_method(STORAGE_AGGREGATE_MAX);
    c4->set_aggregate_method(STORAGE_AGGREGATE_MAX);
    c5->set_aggregate_method(STORAGE_AGGREGATE_MAX);
    c6->set_aggregate_method(STORAGE_AGGREGATE_MAX);
    c7->set_aggregate_method(STORAGE_AGGREGATE_MAX);
    c8->set_aggregate_method(STORAGE_AGGREGATE_MAX);

    Schema schema({k1, c1, c2, c3, c4, c5, c6, c7, c8});

    // clang-format off
    auto pk = std::vector<int64_t>{1, 1, 1, 2, 2};
    auto v1 = std::vector<int64_t>{ 1, 2, 3, 1, -1};  // numeric columns
    auto v2 = std::vector<const char*>{"1", "2", "3", "1", "-1"};  // decimal
    auto v3 = std::vector<const char*>{"2020-01-02", "2020-05-01", "2020-06-01", "1990-11-25", "1998-09-01"};
    auto v4 = std::vector<const char*>{"2020-01-01 01:01:01", "2020-01-01 01:02:01", "2020-01-01 01:03:01",
                                       "1990-01-01 01:01:01", "1991-01-01 01:01:01"};
    auto v5 = std::vector<const char*>{"a", "b", "c", "x", "y"};  // varchar

    auto child_iter = std::make_shared<VectorChunkIterator>(schema,
                                                            COL_BIGINT(pk),
                                                            COL_SMALLINT(v1),
                                                            COL_LARGEINT(v1),
                                                            COL_FLOAT(v1),
                                                            COL_DOUBLE(v1),
                                                            COL_DECIMAL(v2),
                                                            COL_DATE(v3),
                                                            COL_DATETIME(v4),
                                                            COL_VARCHAR(v5));
    // clang-format on
    auto agg_iter = new_aggregate_iterator(child_iter);
    ASSERT_TRUE(agg_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
    ChunkPtr chunk = ChunkHelper::new_chunk(agg_iter->schema(), config::vector_chunk_size);
    Status st = agg_iter->get_next(chunk.get());
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2u, chunk->num_rows());
    ASSERT_EQ(9u, chunk->num_columns());

    // first row
    ASSERT_EQ(1, row(*chunk, 0)[0].get_int64());                                               // k1
    ASSERT_EQ(3, row(*chunk, 0)[1].get_int16());                                               // c1
    ASSERT_EQ(3, row(*chunk, 0)[2].get_int128());                                              // c2
    ASSERT_EQ(3.0, row(*chunk, 0)[3].get_float());                                             // c3
    ASSERT_EQ(3.0, row(*chunk, 0)[4].get_double());                                            // c4
    ASSERT_EQ(DecimalV2Value("3"), row(*chunk, 0)[5].get_decimal());                           // c5
    ASSERT_EQ(DateValue::create(2020, 6, 1), row(*chunk, 0)[6].get_date());                    // c6
    ASSERT_EQ(TimestampValue::create(2020, 1, 1, 1, 3, 1), row(*chunk, 0)[7].get_timestamp()); // c7
    ASSERT_EQ("c", row(*chunk, 0)[8].get_slice());                                             // c8

    // second row
    ASSERT_EQ(2, row(*chunk, 1)[0].get_int64());                                               // k1
    ASSERT_EQ(1, row(*chunk, 1)[1].get_int16());                                               // c1
    ASSERT_EQ(1, row(*chunk, 1)[2].get_int128());                                              // c2
    ASSERT_EQ(1.0, row(*chunk, 1)[3].get_float());                                             // c3
    ASSERT_EQ(1.0, row(*chunk, 1)[4].get_double());                                            // c4
    ASSERT_EQ(DecimalV2Value("1"), row(*chunk, 1)[5].get_decimal());                           // c5
    ASSERT_EQ(DateValue::create(1998, 9, 1), row(*chunk, 1)[6].get_date());                    // c6
    ASSERT_EQ(TimestampValue::create(1991, 1, 1, 1, 1, 1), row(*chunk, 1)[7].get_timestamp()); // c7
    ASSERT_EQ("y", row(*chunk, 1)[8].get_slice());                                             // c8
    agg_iter->close();
}

// NOLINTNEXTLINE
TEST_F(AggregateIteratorTest, agg_min) {
    config::vector_chunk_size = 1024;

    auto k1 = std::make_shared<Field>(0, "k1", TYPE_BIGINT, false);
    auto c1 = std::make_shared<Field>(1, "c1", TYPE_SMALLINT, false);
    auto c2 = std::make_shared<Field>(2, "c2", TYPE_LARGEINT, false);
    auto c3 = std::make_shared<Field>(3, "c3", TYPE_FLOAT, false);
    auto c4 = std::make_shared<Field>(4, "c4", TYPE_DOUBLE, false);
    auto c5 = std::make_shared<Field>(5, "c5", TYPE_DECIMALV2, false);
    auto c6 = std::make_shared<Field>(6, "c6", TYPE_DATE, false);
    auto c7 = std::make_shared<Field>(7, "c7", TYPE_DATETIME, false);
    auto c8 = std::make_shared<Field>(8, "c8", TYPE_VARCHAR, false);

    k1->set_is_key(true);
    k1->set_aggregate_method(STORAGE_AGGREGATE_NONE);
    c1->set_aggregate_method(STORAGE_AGGREGATE_MIN);
    c2->set_aggregate_method(STORAGE_AGGREGATE_MIN);
    c3->set_aggregate_method(STORAGE_AGGREGATE_MIN);
    c4->set_aggregate_method(STORAGE_AGGREGATE_MIN);
    c5->set_aggregate_method(STORAGE_AGGREGATE_MIN);
    c6->set_aggregate_method(STORAGE_AGGREGATE_MIN);
    c7->set_aggregate_method(STORAGE_AGGREGATE_MIN);
    c8->set_aggregate_method(STORAGE_AGGREGATE_MIN);

    Schema schema({k1, c1, c2, c3, c4, c5, c6, c7, c8});

    // clang-format off
    auto pk = std::vector<int64_t>{1, 1, 1, 2, 2};
    auto v1 = std::vector<int64_t>{ 1, 2, 3, 1, -1};  // numeric columns
    auto v2 = std::vector<const char*>{"1", "2", "3", "1", "-1"};  // decimal
    auto v3 = std::vector<const char*>{"2020-01-02", "2020-05-01", "2020-06-01", "1990-11-25", "1998-09-01"};
    auto v4 = std::vector<const char*>{"2020-01-01 01:01:01", "2020-01-01 01:02:01", "2020-01-01 01:03:01",
                                       "1990-01-01 01:01:01", "1991-01-01 01:01:01"};
    auto v5 = std::vector<const char*>{"a", "b", "c", "x", "y"};  // varchar

    auto child_iter = std::make_shared<VectorChunkIterator>(schema,
                                                            COL_BIGINT(pk),
                                                            COL_SMALLINT(v1),
                                                            COL_LARGEINT(v1),
                                                            COL_FLOAT(v1),
                                                            COL_DOUBLE(v1),
                                                            COL_DECIMAL(v2),
                                                            COL_DATE(v3),
                                                            COL_DATETIME(v4),
                                                            COL_VARCHAR(v5));
    // clang-format on
    auto agg_iter = new_aggregate_iterator(child_iter);
    ASSERT_TRUE(agg_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
    ChunkPtr chunk = ChunkHelper::new_chunk(agg_iter->schema(), config::vector_chunk_size);
    Status st = agg_iter->get_next(chunk.get());
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2u, chunk->num_rows());
    ASSERT_EQ(9u, chunk->num_columns());

    // first row
    ASSERT_EQ(1, row(*chunk, 0)[0].get_int64());                                               // k1
    ASSERT_EQ(1, row(*chunk, 0)[1].get_int16());                                               // c1
    ASSERT_EQ(1, row(*chunk, 0)[2].get_int128());                                              // c2
    ASSERT_EQ(1.0, row(*chunk, 0)[3].get_float());                                             // c3
    ASSERT_EQ(1.0, row(*chunk, 0)[4].get_double());                                            // c4
    ASSERT_EQ(DecimalV2Value("1"), row(*chunk, 0)[5].get_decimal());                           // c5
    ASSERT_EQ(DateValue::create(2020, 1, 2), row(*chunk, 0)[6].get_date());                    // c6
    ASSERT_EQ(TimestampValue::create(2020, 1, 1, 1, 1, 1), row(*chunk, 0)[7].get_timestamp()); // c7
    ASSERT_EQ("a", row(*chunk, 0)[8].get_slice());                                             // c8

    // second row
    ASSERT_EQ(2, row(*chunk, 1)[0].get_int64());                                               // k1
    ASSERT_EQ(-1, row(*chunk, 1)[1].get_int16());                                              // c1
    ASSERT_EQ(-1, row(*chunk, 1)[2].get_int128());                                             // c2
    ASSERT_EQ(-1.0, row(*chunk, 1)[3].get_float());                                            // c3
    ASSERT_EQ(-1.0, row(*chunk, 1)[4].get_double());                                           // c4
    ASSERT_EQ(DecimalV2Value("-1"), row(*chunk, 1)[5].get_decimal());                          // c5
    ASSERT_EQ(DateValue::create(1990, 11, 25), row(*chunk, 1)[6].get_date());                  // c6
    ASSERT_EQ(TimestampValue::create(1990, 1, 1, 1, 1, 1), row(*chunk, 1)[7].get_timestamp()); // c7
    ASSERT_EQ("x", row(*chunk, 1)[8].get_slice());                                             // c8
    agg_iter->close();
}

// NOLINTNEXTLINE
TEST_F(AggregateIteratorTest, agg_sum) {
    config::vector_chunk_size = 1024;

    auto k1 = std::make_shared<Field>(0, "k1", TYPE_BIGINT, false);
    auto c1 = std::make_shared<Field>(1, "c1", TYPE_TINYINT, false);
    auto c2 = std::make_shared<Field>(2, "c2", TYPE_SMALLINT, false);
    auto c3 = std::make_shared<Field>(3, "c3", TYPE_INT, false);
    auto c4 = std::make_shared<Field>(4, "c4", TYPE_BIGINT, false);
    auto c5 = std::make_shared<Field>(5, "c5", TYPE_LARGEINT, false);
    auto c6 = std::make_shared<Field>(6, "c6", TYPE_FLOAT, false);
    auto c7 = std::make_shared<Field>(7, "c7", TYPE_DOUBLE, false);
    auto c8 = std::make_shared<Field>(8, "c8", TYPE_DECIMALV2, false);

    k1->set_is_key(true);
    k1->set_aggregate_method(STORAGE_AGGREGATE_NONE);
    c1->set_aggregate_method(STORAGE_AGGREGATE_SUM);
    c2->set_aggregate_method(STORAGE_AGGREGATE_SUM);
    c3->set_aggregate_method(STORAGE_AGGREGATE_SUM);
    c4->set_aggregate_method(STORAGE_AGGREGATE_SUM);
    c5->set_aggregate_method(STORAGE_AGGREGATE_SUM);
    c6->set_aggregate_method(STORAGE_AGGREGATE_SUM);
    c7->set_aggregate_method(STORAGE_AGGREGATE_SUM);
    c8->set_aggregate_method(STORAGE_AGGREGATE_SUM);

    Schema schema({k1, c1, c2, c3, c4, c5, c6, c7, c8});
    auto pk = std::vector<int64_t>{1, 1, 1, 2, 2};
    auto v1 = std::vector<int64_t>{1, 2, 3, 1, -1};
    auto v2 = std::vector<const char*>{"1", "2", "3", "1", "-1"};

    // clang-format off
    auto child_iter = std::make_shared<VectorChunkIterator>(schema,
                                                            COL_BIGINT(pk),
                                                            COL_TINYINT(v1),
                                                            COL_SMALLINT(v1),
                                                            COL_INT(v1),
                                                            COL_BIGINT(v1),
                                                            COL_LARGEINT(v1),
                                                            COL_FLOAT(v1),
                                                            COL_DOUBLE(v1),
                                                            COL_DECIMAL(v2));
    // clang-format on
    auto agg_iter = new_aggregate_iterator(child_iter);
    ASSERT_TRUE(agg_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
    ChunkPtr chunk = ChunkHelper::new_chunk(agg_iter->schema(), config::vector_chunk_size);
    Status st = agg_iter->get_next(chunk.get());
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2u, chunk->num_rows());
    ASSERT_EQ(9u, chunk->num_columns());

    // first row
    ASSERT_EQ(1, row(*chunk, 0)[0].get_int64());
    ASSERT_EQ(6, row(*chunk, 0)[1].get_int8());
    ASSERT_EQ(6, row(*chunk, 0)[2].get_int16());
    ASSERT_EQ(6, row(*chunk, 0)[3].get_int32());
    ASSERT_EQ(6, row(*chunk, 0)[4].get_int64());
    ASSERT_EQ(6, row(*chunk, 0)[5].get_int128());
    ASSERT_EQ(6.0, row(*chunk, 0)[6].get_float());
    ASSERT_EQ(6.0, row(*chunk, 0)[7].get_double());
    ASSERT_EQ(DecimalV2Value("6"), row(*chunk, 0)[8].get_decimal());

    // second row
    ASSERT_EQ(2, row(*chunk, 1)[0].get_int64());
    ASSERT_EQ(0, row(*chunk, 1)[1].get_int8());
    ASSERT_EQ(0, row(*chunk, 1)[2].get_int16());
    ASSERT_EQ(0, row(*chunk, 1)[3].get_int32());
    ASSERT_EQ(0, row(*chunk, 1)[4].get_int64());
    ASSERT_EQ(0, row(*chunk, 1)[5].get_int128());
    ASSERT_EQ(0.0, row(*chunk, 1)[6].get_float());
    ASSERT_EQ(0.0, row(*chunk, 1)[7].get_double());
    ASSERT_EQ(DecimalV2Value("0"), row(*chunk, 1)[8].get_decimal());
    agg_iter->close();
}

// NOLINTNEXTLINE
TEST_F(AggregateIteratorTest, agg_replace) {
    config::vector_chunk_size = 1024;

    auto k1 = std::make_shared<Field>(0, "k1", TYPE_BIGINT, false);
    auto c1 = std::make_shared<Field>(1, "c1", TYPE_SMALLINT, false);
    auto c2 = std::make_shared<Field>(2, "c2", TYPE_LARGEINT, false);
    auto c3 = std::make_shared<Field>(3, "c3", TYPE_FLOAT, false);
    auto c4 = std::make_shared<Field>(4, "c4", TYPE_DOUBLE, false);
    auto c5 = std::make_shared<Field>(5, "c5", TYPE_DECIMALV2, false);
    auto c6 = std::make_shared<Field>(6, "c6", TYPE_DATE, false);
    auto c7 = std::make_shared<Field>(7, "c7", TYPE_DATETIME, false);
    auto c8 = std::make_shared<Field>(8, "c8", TYPE_VARCHAR, false);

    k1->set_is_key(true);
    k1->set_aggregate_method(STORAGE_AGGREGATE_NONE);
    c1->set_aggregate_method(STORAGE_AGGREGATE_REPLACE);
    c2->set_aggregate_method(STORAGE_AGGREGATE_REPLACE);
    c3->set_aggregate_method(STORAGE_AGGREGATE_REPLACE);
    c4->set_aggregate_method(STORAGE_AGGREGATE_REPLACE);
    c5->set_aggregate_method(STORAGE_AGGREGATE_REPLACE);
    c6->set_aggregate_method(STORAGE_AGGREGATE_REPLACE);
    c7->set_aggregate_method(STORAGE_AGGREGATE_REPLACE);
    c8->set_aggregate_method(STORAGE_AGGREGATE_REPLACE);

    Schema schema({k1, c1, c2, c3, c4, c5, c6, c7, c8});

    // clang-format off
    auto pk = std::vector<int64_t>{1, 1, 1, 2, 2};
    auto v1 = std::vector<int64_t>{ 1, 2, 3, 1, -1};  // numeric columns
    auto v2 = std::vector<const char*>{"1", "2", "3", "1", "-1"};  // decimal
    auto v3 = std::vector<const char*>{"2020-01-02", "2020-05-01", "2020-06-01", "1990-11-25", "1998-09-01"};
    auto v4 = std::vector<const char*>{"2020-01-01 01:01:01", "2020-01-01 01:02:01", "2020-01-01 01:03:01",
                                       "1990-01-01 01:01:01", "1991-01-01 01:01:01"};
    auto v5 = std::vector<const char*>{"a", "b", "c", "x", "y"};  // varchar

    auto child_iter = std::make_shared<VectorChunkIterator>(schema,
                                                            COL_BIGINT(pk),
                                                            COL_SMALLINT(v1),
                                                            COL_LARGEINT(v1),
                                                            COL_FLOAT(v1),
                                                            COL_DOUBLE(v1),
                                                            COL_DECIMAL(v2),
                                                            COL_DATE(v3),
                                                            COL_DATETIME(v4),
                                                            COL_VARCHAR(v5));
    // clang-format on
    auto agg_iter = new_aggregate_iterator(child_iter);
    ASSERT_TRUE(agg_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
    ChunkPtr chunk = ChunkHelper::new_chunk(agg_iter->schema(), config::vector_chunk_size);
    Status st = agg_iter->get_next(chunk.get());
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(2u, chunk->num_rows());
    ASSERT_EQ(9u, chunk->num_columns());

    // first row
    ASSERT_EQ(1, row(*chunk, 0)[0].get_int64());                                               // k1
    ASSERT_EQ(3, row(*chunk, 0)[1].get_int16());                                               // c1
    ASSERT_EQ(3, row(*chunk, 0)[2].get_int128());                                              // c2
    ASSERT_EQ(3.0, row(*chunk, 0)[3].get_float());                                             // c3
    ASSERT_EQ(3.0, row(*chunk, 0)[4].get_double());                                            // c4
    ASSERT_EQ(DecimalV2Value("3"), row(*chunk, 0)[5].get_decimal());                           // c5
    ASSERT_EQ(DateValue::create(2020, 6, 1), row(*chunk, 0)[6].get_date());                    // c6
    ASSERT_EQ(TimestampValue::create(2020, 1, 1, 1, 3, 1), row(*chunk, 0)[7].get_timestamp()); // c7
    ASSERT_EQ("c", row(*chunk, 0)[8].get_slice());                                             // c8

    // second row
    ASSERT_EQ(2, row(*chunk, 1)[0].get_int64());                                               // k1
    ASSERT_EQ(-1, row(*chunk, 1)[1].get_int16());                                              // c1
    ASSERT_EQ(-1, row(*chunk, 1)[2].get_int128());                                             // c2
    ASSERT_EQ(-1.0, row(*chunk, 1)[3].get_float());                                            // c3
    ASSERT_EQ(-1.0, row(*chunk, 1)[4].get_double());                                           // c4
    ASSERT_EQ(DecimalV2Value("-1"), row(*chunk, 1)[5].get_decimal());                          // c5
    ASSERT_EQ(DateValue::create(1998, 9, 1), row(*chunk, 1)[6].get_date());                    // c6
    ASSERT_EQ(TimestampValue::create(1991, 1, 1, 1, 1, 1), row(*chunk, 1)[7].get_timestamp()); // c7
    ASSERT_EQ("y", row(*chunk, 1)[8].get_slice());
    agg_iter->close();
}

// NOLINTNEXTLINE
TEST_F(AggregateIteratorTest, agg_max_no_duplicate) {
    config::vector_chunk_size = 1024;

    auto k1 = std::make_shared<Field>(0, "k1", TYPE_BIGINT, false);
    auto c1 = std::make_shared<Field>(1, "c1", TYPE_SMALLINT, false);

    k1->set_is_key(true);
    k1->set_aggregate_method(STORAGE_AGGREGATE_NONE);
    c1->set_aggregate_method(STORAGE_AGGREGATE_MAX);

    Schema schema({k1, c1});

    // clang-format off
    auto pk = std::vector<int64_t>{1, 2, 3, 4, 5};
    auto v1 = std::vector<int64_t>{ 1, 2, 3, 4, 5};

    auto child_iter = std::make_shared<VectorChunkIterator>(schema,
                                                            COL_BIGINT(pk),
                                                            COL_SMALLINT(v1));
    // clang-format on
    auto agg_iter = new_aggregate_iterator(child_iter);
    ASSERT_TRUE(agg_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
    ChunkPtr chunk = ChunkHelper::new_chunk(agg_iter->schema(), config::vector_chunk_size);
    Status st = agg_iter->get_next(chunk.get());
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(5u, chunk->num_rows());
    ASSERT_EQ(2u, chunk->num_columns());

    // 1st row
    ASSERT_EQ(1, row(*chunk, 0)[0].get_int64()); // k1
    ASSERT_EQ(1, row(*chunk, 0)[1].get_int16()); // c1

    // 2nd row
    ASSERT_EQ(2, row(*chunk, 1)[0].get_int64()); // k1
    ASSERT_EQ(2, row(*chunk, 1)[1].get_int16()); // c1

    // 3rd row
    ASSERT_EQ(3, row(*chunk, 2)[0].get_int64()); // k1
    ASSERT_EQ(3, row(*chunk, 2)[1].get_int16()); // c1

    // 4th row
    ASSERT_EQ(4, row(*chunk, 3)[0].get_int64()); // k1
    ASSERT_EQ(4, row(*chunk, 3)[1].get_int16()); // c1

    // 5th row
    ASSERT_EQ(5, row(*chunk, 4)[0].get_int64()); // k1
    ASSERT_EQ(5, row(*chunk, 4)[1].get_int16()); // c1
    agg_iter->close();
}

// NOLINTNEXTLINE
TEST_F(AggregateIteratorTest, agg_max_empty) {
    config::vector_chunk_size = 1024;

    auto k1 = std::make_shared<Field>(0, "k1", TYPE_BIGINT, false);
    auto c1 = std::make_shared<Field>(1, "c1", TYPE_SMALLINT, false);

    k1->set_is_key(true);
    k1->set_aggregate_method(STORAGE_AGGREGATE_NONE);
    c1->set_aggregate_method(STORAGE_AGGREGATE_MAX);

    Schema schema({k1, c1});

    // clang-format off
    auto pk = std::vector<int64_t>{};
    auto v1 = std::vector<int64_t>{};

    auto child_iter = std::make_shared<VectorChunkIterator>(schema,
                                                            COL_BIGINT(pk),
                                                            COL_SMALLINT(v1));
    // clang-format on
    auto agg_iter = new_aggregate_iterator(child_iter);
    ASSERT_TRUE(agg_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
    ChunkPtr chunk = ChunkHelper::new_chunk(agg_iter->schema(), config::vector_chunk_size);
    Status st = agg_iter->get_next(chunk.get());
    ASSERT_TRUE(st.is_end_of_file());
    agg_iter->close();
}

// NOLINTNEXTLINE
TEST_F(AggregateIteratorTest, agg_max_small_chunk) {
    config::vector_chunk_size = 2;

    auto k1 = std::make_shared<Field>(0, "k1", TYPE_BIGINT, false);
    auto c1 = std::make_shared<Field>(1, "c1", TYPE_SMALLINT, false);

    k1->set_is_key(true);
    k1->set_aggregate_method(STORAGE_AGGREGATE_NONE);
    c1->set_aggregate_method(STORAGE_AGGREGATE_MAX);

    Schema schema({k1, c1});

    // clang-format off
    auto pk = std::vector<int64_t>{+1, +2, +3, +4, +5, +6, +7, +8, +9};
    auto v1 = std::vector<int16_t>{-1, -2, -3, -4, -5, -6, -7, -8, -9};

    auto child_iter = std::make_shared<VectorChunkIterator>(schema,
                                                            COL_BIGINT(pk),
                                                            COL_SMALLINT(v1));
    // clang-format on
    auto agg_iter = new_aggregate_iterator(child_iter);
    ASSERT_TRUE(agg_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
    ChunkPtr chunk = ChunkHelper::new_chunk(agg_iter->schema(), config::vector_chunk_size);
    Status st;
    std::vector<int16_t> values;
    while (true) {
        chunk->reset();
        st = agg_iter->get_next(chunk.get());
        if (!st.ok()) {
            break;
        }
        auto& c = chunk->get_column_by_index(1);
        // std::vector<int16_t>
        auto& v = FixedLengthColumn<int16_t>::dynamic_pointer_cast(c)->get_data();
        values.insert(values.end(), v.begin(), v.end());
    }
    ASSERT_TRUE(st.is_end_of_file());
    EXPECT_EQ(9u, values.size());
    for (int i = 0; i < values.size(); i++) {
        EXPECT_EQ(i + 1, -1 * values[i]);
    }
    agg_iter->close();
}

// NOLINTNEXTLINE
TEST_F(AggregateIteratorTest, agg_max_all_duplicate) {
    config::vector_chunk_size = 2;

    auto k1 = std::make_shared<Field>(0, "k1", TYPE_BIGINT, false);
    auto c1 = std::make_shared<Field>(1, "c1", TYPE_SMALLINT, false);

    k1->set_is_key(true);
    k1->set_aggregate_method(STORAGE_AGGREGATE_NONE);
    c1->set_aggregate_method(STORAGE_AGGREGATE_MAX);

    Schema schema({k1, c1});

    // clang-format off
    auto pk = std::vector<int64_t>{+1, +1, +1, +1, +1, +1, +1, +1, +1};
    auto v1 = std::vector<int16_t>{-1, +2, -3, -4, -5, -6, -7, -8, -9};

    auto child_iter = std::make_shared<VectorChunkIterator>(schema,
                                                            COL_BIGINT(pk),
                                                            COL_SMALLINT(v1));
    // clang-format on
    auto agg_iter = new_aggregate_iterator(child_iter);
    ASSERT_TRUE(agg_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
    ChunkPtr chunk = ChunkHelper::new_chunk(agg_iter->schema(), config::vector_chunk_size);
    Status st;
    std::vector<int16_t> values;
    while (true) {
        chunk->reset();
        st = agg_iter->get_next(chunk.get());
        if (!st.ok()) {
            break;
        }
        auto& c = chunk->get_column_by_index(1);
        // std::vector<int16_t>
        auto& v = FixedLengthColumn<int16_t>::dynamic_pointer_cast(c)->get_data();
        values.insert(values.end(), v.begin(), v.end());
    }
    ASSERT_TRUE(st.is_end_of_file());
    EXPECT_EQ(1u, values.size());
    ASSERT_EQ(2, values[0]);
    agg_iter->close();
}

// NOLINTNEXTLINE
TEST_F(AggregateIteratorTest, agg_boolean_key) {
    config::vector_chunk_size = 2;

    auto k1 = std::make_shared<Field>(0, "k1", TYPE_BOOLEAN, false);
    auto c1 = std::make_shared<Field>(1, "c1", TYPE_SMALLINT, false);

    k1->set_is_key(true);
    k1->set_aggregate_method(STORAGE_AGGREGATE_NONE);
    c1->set_aggregate_method(STORAGE_AGGREGATE_MAX);

    Schema schema({k1, c1});

    // clang-format off
    auto pk = std::vector<uint8_t>{0, 0, 0, 1, 1, 1, 1, 1, 1};
    auto v1 = std::vector<int16_t>{1, 2, 3, 4, 5, 6, 7, 8, 9};

    auto child_iter = std::make_shared<VectorChunkIterator>(schema,
                                                            COL_BOOLEAN(pk),
                                                            COL_SMALLINT(v1));
    // clang-format on
    auto agg_iter = new_aggregate_iterator(child_iter);
    ASSERT_TRUE(agg_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
    ChunkPtr chunk = ChunkHelper::new_chunk(agg_iter->schema(), config::vector_chunk_size);
    Status st;
    std::vector<int16_t> values;
    while (true) {
        chunk->reset();
        st = agg_iter->get_next(chunk.get());
        if (!st.ok()) {
            break;
        }
        auto& c = chunk->get_column_by_index(1);
        // std::vector<int16_t>
        auto& v = FixedLengthColumn<int16_t>::dynamic_pointer_cast(c)->get_data();
        values.insert(values.end(), v.begin(), v.end());
    }
    ASSERT_TRUE(st.is_end_of_file());
    EXPECT_EQ(2u, values.size());
    ASSERT_EQ(3, values[0]);
    ASSERT_EQ(9, values[1]);
    agg_iter->close();
}

// NOLINTNEXTLINE
TEST_F(AggregateIteratorTest, agg_varchar_key) {
    config::vector_chunk_size = 2;

    auto k1 = std::make_shared<Field>(0, "k1", TYPE_VARCHAR, false);
    auto c1 = std::make_shared<Field>(1, "c1", TYPE_SMALLINT, false);

    k1->set_is_key(true);
    k1->set_aggregate_method(STORAGE_AGGREGATE_NONE);
    c1->set_aggregate_method(STORAGE_AGGREGATE_MAX);

    Schema schema({k1, c1});

    {
        auto pk = std::vector<std::string>{"a", "b", "c", "d", "e", "f"};
        auto v1 = std::vector<int16_t>{1, 2, 3, 4, 5, 6};
        auto child_iter = std::make_shared<VectorChunkIterator>(schema, COL_VARCHAR(pk), COL_SMALLINT(v1));
        auto agg_iter = new_aggregate_iterator(child_iter);
        ASSERT_TRUE(agg_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
        ChunkPtr chunk = ChunkHelper::new_chunk(agg_iter->schema(), config::vector_chunk_size);
        Status st;
        std::vector<int16_t> values;
        while (true) {
            chunk->reset();
            st = agg_iter->get_next(chunk.get());
            if (!st.ok()) {
                break;
            }
            auto& c = chunk->get_column_by_index(1);
            // std::vector<int16_t>
            auto& v = FixedLengthColumn<int16_t>::dynamic_pointer_cast(c)->get_data();
            values.insert(values.end(), v.begin(), v.end());
        }
        ASSERT_TRUE(st.is_end_of_file());
        EXPECT_EQ(6u, values.size());
        ASSERT_EQ(1, values[0]);
        ASSERT_EQ(2, values[1]);
        ASSERT_EQ(3, values[2]);
        ASSERT_EQ(4, values[3]);
        ASSERT_EQ(5, values[4]);
        ASSERT_EQ(6, values[5]);
        agg_iter->close();
    }
    {
        auto pk = std::vector<std::string>{"abc", "abc", "abc", "abc", "abc", "abc"};
        auto v1 = std::vector<int16_t>{1, 2, 3, 4, 5, 6};
        auto child_iter = std::make_shared<VectorChunkIterator>(schema, COL_VARCHAR(pk), COL_SMALLINT(v1));
        auto agg_iter = new_aggregate_iterator(child_iter);
        ASSERT_TRUE(agg_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
        ChunkPtr chunk = ChunkHelper::new_chunk(agg_iter->schema(), config::vector_chunk_size);
        Status st;
        std::vector<int16_t> values;
        while (true) {
            chunk->reset();
            st = agg_iter->get_next(chunk.get());
            if (!st.ok()) {
                break;
            }
            auto& c = chunk->get_column_by_index(1);
            // std::vector<int16_t>
            auto& v = FixedLengthColumn<int16_t>::dynamic_pointer_cast(c)->get_data();
            values.insert(values.end(), v.begin(), v.end());
        }
        ASSERT_TRUE(st.is_end_of_file());
        EXPECT_EQ(1u, values.size());
        ASSERT_EQ(6, values[0]);
        agg_iter->close();
    }
    {
        auto pk = std::vector<std::string>{"abc", "def", "def", "abc", "abc", "xyz"};
        auto v1 = std::vector<int16_t>{1, 2, 3, 4, 5, 6};
        auto child_iter = std::make_shared<VectorChunkIterator>(schema, COL_VARCHAR(pk), COL_SMALLINT(v1));
        auto agg_iter = new_aggregate_iterator(child_iter);
        ASSERT_TRUE(agg_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
        ChunkPtr chunk = ChunkHelper::new_chunk(agg_iter->schema(), config::vector_chunk_size);
        Status st;
        std::vector<int16_t> values;
        while (true) {
            chunk->reset();
            st = agg_iter->get_next(chunk.get());
            if (!st.ok()) {
                break;
            }
            auto& c = chunk->get_column_by_index(1);
            // std::vector<int16_t>
            auto& v = FixedLengthColumn<int16_t>::dynamic_pointer_cast(c)->get_data();
            values.insert(values.end(), v.begin(), v.end());
        }
        ASSERT_TRUE(st.is_end_of_file());
        EXPECT_EQ(4u, values.size());
        ASSERT_EQ(1, values[0]);
        ASSERT_EQ(3, values[1]);
        ASSERT_EQ(5, values[2]);
        ASSERT_EQ(6, values[3]);
        agg_iter->close();
    }
}

// NOLINTNEXTLINE
TEST_F(AggregateIteratorTest, agg_date_key) {
    config::vector_chunk_size = 2;

    auto k1 = std::make_shared<Field>(0, "k1", TYPE_DATE, false);
    auto c1 = std::make_shared<Field>(1, "c1", TYPE_SMALLINT, false);

    k1->set_is_key(true);
    k1->set_aggregate_method(STORAGE_AGGREGATE_NONE);
    c1->set_aggregate_method(STORAGE_AGGREGATE_MAX);

    Schema schema({k1, c1});

    {
        auto pk = std::vector<std::string>{"1990-01-01", "1991-01-01", "1992-01-01", "1993-01-01"};
        auto v1 = std::vector<int16_t>{1, 2, 3, 4};
        auto child_iter = std::make_shared<VectorChunkIterator>(schema, COL_DATE(pk), COL_SMALLINT(v1));
        auto agg_iter = new_aggregate_iterator(child_iter);
        ASSERT_TRUE(agg_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
        ChunkPtr chunk = ChunkHelper::new_chunk(agg_iter->schema(), config::vector_chunk_size);
        Status st;
        std::vector<int16_t> values;
        while (true) {
            chunk->reset();
            st = agg_iter->get_next(chunk.get());
            if (!st.ok()) {
                break;
            }
            auto& c = chunk->get_column_by_index(1);
            // std::vector<int16_t>
            auto& v = FixedLengthColumn<int16_t>::dynamic_pointer_cast(c)->get_data();
            values.insert(values.end(), v.begin(), v.end());
        }
        ASSERT_TRUE(st.is_end_of_file());
        EXPECT_EQ(4u, values.size());
        ASSERT_EQ(1, values[0]);
        ASSERT_EQ(2, values[1]);
        ASSERT_EQ(3, values[2]);
        ASSERT_EQ(4, values[3]);
        agg_iter->close();
    }
    {
        auto pk = std::vector<std::string>{"1990-01-01", "1990-01-01", "1990-01-01", "1990-01-01"};
        auto v1 = std::vector<int16_t>{1, 2, 3, 4};
        auto child_iter = std::make_shared<VectorChunkIterator>(schema, COL_DATE(pk), COL_SMALLINT(v1));
        auto agg_iter = new_aggregate_iterator(child_iter);
        ASSERT_TRUE(agg_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
        ChunkPtr chunk = ChunkHelper::new_chunk(agg_iter->schema(), config::vector_chunk_size);
        Status st;
        std::vector<int16_t> values;
        while (true) {
            chunk->reset();
            st = agg_iter->get_next(chunk.get());
            if (!st.ok()) {
                break;
            }
            auto& c = chunk->get_column_by_index(1);
            // std::vector<int16_t>
            auto& v = FixedLengthColumn<int16_t>::dynamic_pointer_cast(c)->get_data();
            values.insert(values.end(), v.begin(), v.end());
        }
        ASSERT_TRUE(st.is_end_of_file());
        EXPECT_EQ(1u, values.size());
        ASSERT_EQ(4, values[0]);
        agg_iter->close();
    }
    {
        auto pk = std::vector<std::string>{"1990-01-01", "1990-01-01", "1990-01-02", "1990-01-03"};
        auto v1 = std::vector<int16_t>{1, 2, 3, 4};
        auto child_iter = std::make_shared<VectorChunkIterator>(schema, COL_DATE(pk), COL_SMALLINT(v1));
        auto agg_iter = new_aggregate_iterator(child_iter);
        ASSERT_TRUE(agg_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
        ChunkPtr chunk = ChunkHelper::new_chunk(agg_iter->schema(), config::vector_chunk_size);
        Status st;
        std::vector<int16_t> values;
        while (true) {
            chunk->reset();
            st = agg_iter->get_next(chunk.get());
            if (!st.ok()) {
                break;
            }
            auto& c = chunk->get_column_by_index(1);
            // std::vector<int16_t>
            auto& v = FixedLengthColumn<int16_t>::dynamic_pointer_cast(c)->get_data();
            values.insert(values.end(), v.begin(), v.end());
        }
        ASSERT_TRUE(st.is_end_of_file());
        EXPECT_EQ(3u, values.size());
        ASSERT_EQ(2, values[0]);
        ASSERT_EQ(3, values[1]);
        ASSERT_EQ(4, values[2]);
        agg_iter->close();
    }
}

// NOLINTNEXTLINE
TEST_F(AggregateIteratorTest, agg_decimal_key) {
    config::vector_chunk_size = 2;

    auto k1 = std::make_shared<Field>(0, "k1", TYPE_DECIMALV2, false);
    auto c1 = std::make_shared<Field>(1, "c1", TYPE_SMALLINT, false);

    k1->set_is_key(true);
    k1->set_aggregate_method(STORAGE_AGGREGATE_NONE);
    c1->set_aggregate_method(STORAGE_AGGREGATE_MAX);

    Schema schema({k1, c1});

    {
        auto pk = std::vector<std::string>{"0.0001", "0.0002", "0.0003", "0.0004"};
        auto v1 = std::vector<int16_t>{1, 2, 3, 4};
        auto child_iter = std::make_shared<VectorChunkIterator>(schema, COL_DECIMAL(pk), COL_SMALLINT(v1));
        auto agg_iter = new_aggregate_iterator(child_iter);
        ASSERT_TRUE(agg_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
        ChunkPtr chunk = ChunkHelper::new_chunk(agg_iter->schema(), config::vector_chunk_size);
        Status st;
        std::vector<int16_t> values;
        while (true) {
            chunk->reset();
            st = agg_iter->get_next(chunk.get());
            if (!st.ok()) {
                break;
            }
            auto& c = chunk->get_column_by_index(1);
            // std::vector<int16_t>
            auto& v = FixedLengthColumn<int16_t>::dynamic_pointer_cast(c)->get_data();
            values.insert(values.end(), v.begin(), v.end());
        }
        ASSERT_TRUE(st.is_end_of_file());
        EXPECT_EQ(4u, values.size());
        ASSERT_EQ(1, values[0]);
        ASSERT_EQ(2, values[1]);
        ASSERT_EQ(3, values[2]);
        ASSERT_EQ(4, values[3]);
        agg_iter->close();
    }
    {
        auto pk = std::vector<std::string>{"1.0001", "1.0001", "1.0001", "1.0001"};
        auto v1 = std::vector<int16_t>{1, 2, 3, 4};
        auto child_iter = std::make_shared<VectorChunkIterator>(schema, COL_DECIMAL(pk), COL_SMALLINT(v1));
        auto agg_iter = new_aggregate_iterator(child_iter);
        ASSERT_TRUE(agg_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
        ChunkPtr chunk = ChunkHelper::new_chunk(agg_iter->schema(), config::vector_chunk_size);
        Status st;
        std::vector<int16_t> values;
        while (true) {
            chunk->reset();
            st = agg_iter->get_next(chunk.get());
            if (!st.ok()) {
                break;
            }
            auto& c = chunk->get_column_by_index(1);
            // std::vector<int16_t>
            auto& v = FixedLengthColumn<int16_t>::dynamic_pointer_cast(c)->get_data();
            values.insert(values.end(), v.begin(), v.end());
        }
        ASSERT_TRUE(st.is_end_of_file());
        EXPECT_EQ(1u, values.size());
        ASSERT_EQ(4, values[0]);
        agg_iter->close();
    }
    {
        auto pk = std::vector<std::string>{"1.2345", "1.2345", "3.2345", "4.2345"};
        auto v1 = std::vector<int16_t>{1, 2, 3, 4};
        auto child_iter = std::make_shared<VectorChunkIterator>(schema, COL_DECIMAL(pk), COL_SMALLINT(v1));
        auto agg_iter = new_aggregate_iterator(child_iter);
        ASSERT_TRUE(agg_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
        ChunkPtr chunk = ChunkHelper::new_chunk(agg_iter->schema(), config::vector_chunk_size);
        Status st;
        std::vector<int16_t> values;
        while (true) {
            chunk->reset();
            st = agg_iter->get_next(chunk.get());
            if (!st.ok()) {
                break;
            }
            auto& c = chunk->get_column_by_index(1);
            // std::vector<int16_t>
            auto& v = FixedLengthColumn<int16_t>::dynamic_pointer_cast(c)->get_data();
            values.insert(values.end(), v.begin(), v.end());
        }
        ASSERT_TRUE(st.is_end_of_file());
        EXPECT_EQ(3u, values.size());
        ASSERT_EQ(2, values[0]);
        ASSERT_EQ(3, values[1]);
        ASSERT_EQ(4, values[2]);
        agg_iter->close();
    }
}

// NOLINTNEXTLINE
TEST_F(AggregateIteratorTest, agg_varchar_date_key) {
    config::vector_chunk_size = 2;

    auto k1 = std::make_shared<Field>(0, "k1", TYPE_VARCHAR, false);
    auto k2 = std::make_shared<Field>(1, "k2", TYPE_DATE, false);
    auto c1 = std::make_shared<Field>(2, "c1", TYPE_SMALLINT, false);

    k1->set_is_key(true);
    k2->set_is_key(true);
    k1->set_aggregate_method(STORAGE_AGGREGATE_NONE);
    k2->set_aggregate_method(STORAGE_AGGREGATE_NONE);
    c1->set_aggregate_method(STORAGE_AGGREGATE_MAX);

    Schema schema({k1, k2, c1});

    {
        auto pk1 = std::vector<std::string>{"aaa", "aaa", "aaa", "aaa"};
        auto pk2 = std::vector<std::string>{"1990-11-25", "1991-11-25", "1992-11-25", "1993-11-25"};
        auto v1 = std::vector<int16_t>{1, 2, 3, 4};
        auto child_iter =
                std::make_shared<VectorChunkIterator>(schema, COL_VARCHAR(pk1), COL_DATE(pk2), COL_SMALLINT(v1));
        auto agg_iter = new_aggregate_iterator(child_iter);
        ASSERT_TRUE(agg_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
        ChunkPtr chunk = ChunkHelper::new_chunk(agg_iter->schema(), config::vector_chunk_size);
        Status st;
        std::vector<int16_t> values;
        while (true) {
            chunk->reset();
            st = agg_iter->get_next(chunk.get());
            if (!st.ok()) {
                break;
            }
            auto& c = chunk->get_column_by_index(2);
            // std::vector<int16_t>
            auto& v = FixedLengthColumn<int16_t>::dynamic_pointer_cast(c)->get_data();
            values.insert(values.end(), v.begin(), v.end());
        }
        ASSERT_TRUE(st.is_end_of_file());
        EXPECT_EQ(4u, values.size());
        ASSERT_EQ(1, values[0]);
        ASSERT_EQ(2, values[1]);
        ASSERT_EQ(3, values[2]);
        ASSERT_EQ(4, values[3]);
        agg_iter->close();
    }
    {
        auto pk1 = std::vector<std::string>{"abc", "abd", "bcd", "cde"};
        auto pk2 = std::vector<std::string>{"1990-11-25", "1990-11-25", "1990-11-25", "1990-11-25"};
        auto v1 = std::vector<int16_t>{1, 2, 3, 4};
        auto child_iter =
                std::make_shared<VectorChunkIterator>(schema, COL_VARCHAR(pk1), COL_DATE(pk2), COL_SMALLINT(v1));
        auto agg_iter = new_aggregate_iterator(child_iter);
        ASSERT_TRUE(agg_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
        ChunkPtr chunk = ChunkHelper::new_chunk(agg_iter->schema(), config::vector_chunk_size);
        Status st;
        std::vector<int16_t> values;
        while (true) {
            chunk->reset();
            st = agg_iter->get_next(chunk.get());
            if (!st.ok()) {
                break;
            }
            auto& c = chunk->get_column_by_index(2);
            // std::vector<int16_t>
            auto& v = FixedLengthColumn<int16_t>::dynamic_pointer_cast(c)->get_data();
            values.insert(values.end(), v.begin(), v.end());
        }
        ASSERT_TRUE(st.is_end_of_file());
        EXPECT_EQ(4u, values.size());
        ASSERT_EQ(1, values[0]);
        ASSERT_EQ(2, values[1]);
        ASSERT_EQ(3, values[2]);
        ASSERT_EQ(4, values[3]);
        ASSERT_FALSE(agg_iter->get_next(chunk.get()).ok());
    }
    {
        auto pk1 = std::vector<std::string>{"abc", "abc", "abc", "abc"};
        auto pk2 = std::vector<std::string>{"1990-11-25", "1990-11-25", "1990-11-25", "1990-11-25"};
        auto v1 = std::vector<int16_t>{1, 2, 3, 4};
        auto child_iter =
                std::make_shared<VectorChunkIterator>(schema, COL_VARCHAR(pk1), COL_DATE(pk2), COL_SMALLINT(v1));
        auto agg_iter = new_aggregate_iterator(child_iter);
        ASSERT_TRUE(agg_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
        ChunkPtr chunk = ChunkHelper::new_chunk(agg_iter->schema(), config::vector_chunk_size);
        Status st;
        std::vector<int16_t> values;
        while (true) {
            chunk->reset();
            st = agg_iter->get_next(chunk.get());
            if (!st.ok()) {
                break;
            }
            auto& c = chunk->get_column_by_index(2);
            // std::vector<int16_t>
            auto& v = FixedLengthColumn<int16_t>::dynamic_pointer_cast(c)->get_data();
            values.insert(values.end(), v.begin(), v.end());
        }
        ASSERT_TRUE(st.is_end_of_file());
        EXPECT_EQ(1u, values.size());
        ASSERT_EQ(4, values[0]);
        agg_iter->close();
    }
    {
        auto pk1 = std::vector<std::string>{"abc", "abc", "abc", "def"};
        auto pk2 = std::vector<std::string>{"1990-11-25", "1990-11-25", "1990-11-25", "1990-11-25"};
        auto v1 = std::vector<int16_t>{1, 2, 3, 4};
        auto child_iter =
                std::make_shared<VectorChunkIterator>(schema, COL_VARCHAR(pk1), COL_DATE(pk2), COL_SMALLINT(v1));
        auto agg_iter = new_aggregate_iterator(child_iter);
        ASSERT_TRUE(agg_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
        ChunkPtr chunk = ChunkHelper::new_chunk(agg_iter->schema(), config::vector_chunk_size);
        Status st;
        std::vector<int16_t> values;
        while (true) {
            chunk->reset();
            st = agg_iter->get_next(chunk.get());
            if (!st.ok()) {
                break;
            }
            auto& c = chunk->get_column_by_index(2);
            // std::vector<int16_t>
            auto& v = FixedLengthColumn<int16_t>::dynamic_pointer_cast(c)->get_data();
            values.insert(values.end(), v.begin(), v.end());
        }
        ASSERT_TRUE(st.is_end_of_file());
        EXPECT_EQ(2u, values.size());
        ASSERT_EQ(3, values[0]);
        ASSERT_EQ(4, values[1]);
        agg_iter->close();
    }
}

// NOLINTNEXTLINE
TEST_F(AggregateIteratorTest, agg_varchar_date_key_with_null) {
    config::vector_chunk_size = 2;

    auto k1 = std::make_shared<Field>(0, "k1", TYPE_VARCHAR, true);
    auto k2 = std::make_shared<Field>(1, "k2", TYPE_DATE, true);
    auto c1 = std::make_shared<Field>(2, "c1", TYPE_SMALLINT, false);

    k1->set_is_key(true);
    k2->set_is_key(true);
    k1->set_aggregate_method(STORAGE_AGGREGATE_NONE);
    k2->set_aggregate_method(STORAGE_AGGREGATE_NONE);
    c1->set_aggregate_method(STORAGE_AGGREGATE_SUM);

    Schema schema({k1, k2, c1});

    {
        auto pk1 = std::vector<std::string>{"aaa", "aaa", "aaa", "aaa"};
        auto pk2 = std::vector<std::string>{"1990-11-25", "1991-11-26", "1992-11-27", "1993-11-27"};
        auto v1 = std::vector<int16_t>{1, 2, 3, 4};
        auto pkd1 = COL_VARCHAR(pk1);
        auto pkd2 = COL_DATE(pk2);
        auto vd1 = COL_SMALLINT(v1);

        for (int i = 2; i < 4; ++i) {
            pkd1[i].set_null();
            pkd2[i].set_null();
        }

        auto child_iter = std::make_shared<VectorChunkIterator>(schema, pkd1, pkd2, vd1);

        auto agg_iter = new_aggregate_iterator(child_iter);
        ASSERT_TRUE(agg_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());
        ChunkPtr chunk = ChunkHelper::new_chunk(agg_iter->schema(), config::vector_chunk_size);
        Status st;
        std::vector<int16_t> values;
        while (true) {
            chunk->reset();
            st = agg_iter->get_next(chunk.get());
            if (!st.ok()) {
                break;
            }
            auto& c = chunk->get_column_by_index(2);
            // std::vector<int16_t>
            auto& v = FixedLengthColumn<int16_t>::dynamic_pointer_cast(c)->get_data();
            values.insert(values.end(), v.begin(), v.end());
        }
        ASSERT_TRUE(st.is_end_of_file());
        EXPECT_EQ(3u, values.size());
        ASSERT_EQ(1, values[0]);
        ASSERT_EQ(2, values[1]);
        ASSERT_EQ(7, values[2]);
        agg_iter->close();
    }
}

// NOLINTNEXTLINE
TEST_F(AggregateIteratorTest, gen_source_masks) {
    config::vector_chunk_size = 2;

    auto k1 = std::make_shared<Field>(0, "k1", TYPE_BIGINT, false);
    k1->set_is_key(true);
    k1->set_aggregate_method(STORAGE_AGGREGATE_NONE);

    Schema schema({k1});

    auto pk = std::vector<int64_t>{1, 1, 2, 3, 3, 3, 4, 5, 5};
    auto child_iter = std::make_shared<VectorChunkIterator>(schema, COL_BIGINT(pk));
    child_iter->chunk_size(1024);
    std::vector<RowSourceMask> source_masks{1, 1, 1, 2, 2, 2, 3, 3, 3};
    auto agg_iter = new_aggregate_iterator(child_iter, true);
    ASSERT_TRUE(agg_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());

    ChunkPtr chunk = ChunkHelper::new_chunk(agg_iter->schema(), config::vector_chunk_size);
    Status st;
    while (true) {
        chunk->reset();
        st = agg_iter->get_next(chunk.get(), &source_masks);
        if (!st.ok()) {
            break;
        }
    }
    ASSERT_TRUE(st.is_end_of_file());

    // check agg flag
    std::vector<bool> expected{false, true, false, false, true, true, false, false, true};
    for (size_t i = 0; i < source_masks.size(); ++i) {
        ASSERT_EQ(expected[i], source_masks[i].get_agg_flag());
    }

    agg_iter->close();
}

// NOLINTNEXTLINE
TEST_F(AggregateIteratorTest, sum_from_source_masks) {
    config::vector_chunk_size = 2;

    auto c1 = std::make_shared<Field>(1, "c1", TYPE_SMALLINT, false);
    c1->set_aggregate_method(STORAGE_AGGREGATE_SUM);

    Schema schema({c1});

    auto v1 = std::vector<int16_t>{1, 2, 3, 4, 5, 6, 7, 8, 9};
    auto child_iter = std::make_shared<VectorChunkIterator>(schema, COL_SMALLINT(v1));
    child_iter->chunk_size(1024);
    // pk is {1, 1, 2, 3, 3, 3, 4, 5, 5}
    // only last 9 masks are used to represent pk.
    std::vector<RowSourceMask> source_masks{{1, false}, {2, false}, {1, false}, {1, true},  {1, false}, {2, false},
                                            {2, true},  {2, true},  {3, false}, {3, false}, {3, true}};
    auto agg_iter = new_aggregate_iterator(child_iter, false);
    ASSERT_TRUE(agg_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());

    ChunkPtr chunk = ChunkHelper::new_chunk(agg_iter->schema(), config::vector_chunk_size);
    Status st;
    std::vector<int16_t> values;
    while (true) {
        chunk->reset();
        st = agg_iter->get_next(chunk.get(), &source_masks);
        if (!st.ok()) {
            break;
        }
        auto& c = chunk->get_column_by_index(0);
        // std::vector<int16_t>
        auto& v = FixedLengthColumn<int16_t>::dynamic_pointer_cast(c)->get_data();
        values.insert(values.end(), v.begin(), v.end());
    }
    ASSERT_TRUE(st.is_end_of_file());

    // check agg result
    ASSERT_EQ(5, values.size());
    ASSERT_EQ(3, values[0]);
    ASSERT_EQ(3, values[1]);
    ASSERT_EQ(15, values[2]);
    ASSERT_EQ(7, values[3]);
    ASSERT_EQ(17, values[4]);
    agg_iter->close();
}

// NOLINTNEXTLINE
TEST_F(AggregateIteratorTest, max_from_source_masks) {
    config::vector_chunk_size = 2;

    auto c1 = std::make_shared<Field>(1, "c1", TYPE_SMALLINT, false);
    c1->set_aggregate_method(STORAGE_AGGREGATE_MAX);

    Schema schema({c1});

    auto v1 = std::vector<int16_t>{2, 1, 3, 4, 6, 5, 7, 8, 9};
    auto child_iter = std::make_shared<VectorChunkIterator>(schema, COL_SMALLINT(v1));
    child_iter->chunk_size(1024);
    // pk is {1, 1, 2, 3, 3, 3, 4, 5, 5}
    std::vector<RowSourceMask> source_masks{{1, false}, {1, true},  {1, false}, {2, false}, {2, true},
                                            {2, true},  {3, false}, {3, false}, {3, true}};
    auto agg_iter = new_aggregate_iterator(child_iter, false);
    ASSERT_TRUE(agg_iter->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS).ok());

    ChunkPtr chunk = ChunkHelper::new_chunk(agg_iter->schema(), config::vector_chunk_size);
    Status st;
    std::vector<int16_t> values;
    while (true) {
        chunk->reset();
        st = agg_iter->get_next(chunk.get(), &source_masks);
        if (!st.ok()) {
            break;
        }
        auto& c = chunk->get_column_by_index(0);
        // std::vector<int16_t>
        auto& v = FixedLengthColumn<int16_t>::dynamic_pointer_cast(c)->get_data();
        values.insert(values.end(), v.begin(), v.end());
    }
    ASSERT_TRUE(st.is_end_of_file());

    // check agg result
    ASSERT_EQ(5, values.size());
    ASSERT_EQ(2, values[0]);
    ASSERT_EQ(3, values[1]);
    ASSERT_EQ(6, values[2]);
    ASSERT_EQ(7, values[3]);
    ASSERT_EQ(9, values[4]);
    agg_iter->close();
}

} // namespace starrocks
