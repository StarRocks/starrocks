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

#include "column/column_pool.h"

#include "gtest/gtest.h"

namespace starrocks {

class ColumnPoolTest : public ::testing::Test {
protected:
    void SetUp() override { TEST_clear_all_columns_this_thread(); }
    void TearDown() override { TEST_clear_all_columns_this_thread(); }
};

// NOLINTNEXTLINE
TEST_F(ColumnPoolTest, mem_statistics) {
    auto* pool = ColumnPool<BinaryColumn>::singleton();
    std::string str(20, '1');
    auto* c1 = get_column<BinaryColumn>();
    c1->reserve(config::vector_chunk_size);
    for (size_t i = 0; i < config::vector_chunk_size; i++) {
        c1->append_string(str);
    }

    pool->return_column(c1, config::vector_chunk_size);
    size_t usage_1 = pool->mem_tracker()->consumption();

    pool->release_large_columns(config::vector_chunk_size * 10);
    size_t usage_2 = pool->mem_tracker()->consumption();

    ASSERT_EQ(usage_1 - usage_2, 81920);
}

// NOLINTNEXTLINE
TEST_F(ColumnPoolTest, single_thread) {
    auto c1 = get_column<Int32Column>();
    ASSERT_EQ(0u, c1->get_data().capacity());

    c1->reserve(3);
    c1->append_datum(Datum((int32_t)1));
    c1->append_datum(Datum((int32_t)2));
    c1->set_delete_state(DEL_PARTIAL_SATISFIED);
    return_column<Int32Column>(c1, config::vector_chunk_size);

    auto c2 = get_column<Int32Column>();
    ASSERT_EQ(c1, c2);
    ASSERT_EQ(0u, c2->size());
    ASSERT_EQ(DEL_NOT_SATISFIED, c2->delete_state());
    ASSERT_EQ(3, c2->get_data().capacity());

    auto c3 = get_column<Int32Column>();
    ASSERT_NE(c2, c3);

    auto c4 = get_column<Int32Column>();
    ASSERT_NE(c3, c4);

    auto c5 = get_column<Int32Column>();
    ASSERT_NE(c4, c5);

    auto c6 = get_column<Int32Column>();
    ASSERT_NE(c5, c6);

    return_column<Int32Column>(c6, config::vector_chunk_size);
    return_column<Int32Column>(c5, config::vector_chunk_size);

    auto c7 = get_column<Int32Column>();
    auto c8 = get_column<Int32Column>();
    ASSERT_EQ(c5, c7);
    ASSERT_EQ(c6, c8);

    return_column<Int32Column>(c8, config::vector_chunk_size);
    return_column<Int32Column>(c7, config::vector_chunk_size);

    delete c2;
    delete c3;
    delete c4;
}

} // namespace starrocks
