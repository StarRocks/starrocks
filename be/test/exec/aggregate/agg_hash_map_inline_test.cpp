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

#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "exec/aggregate/agg_hash_map.h"
#include "exec/aggregate/agg_hash_variant.h"
#include "exec/aggregator.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class AggHashMapInlineTest : public ::testing::Test {
protected:
    static constexpr size_t CHUNK_SIZE = 4096;
};

// Correctness test: verify inline state produces correct counts.
TEST_F(AggHashMapInlineTest, InlineStateCorrectness) {
    using HashMap = Int32AggHashMap<PhmapSeed1>;
    using MapWithKey = AggHashMapWithOneNumberKey<TYPE_INT, HashMap>;

    RuntimeProfile profile("test");
    AggStatistics stat(&profile);
    MapWithKey map(CHUNK_SIZE, &stat);
    MemPool pool;

    // Create a small dataset: keys 0-9, each appearing 10 times.
    auto column = Int32Column::create();
    for (int rep = 0; rep < 10; rep++) {
        for (int key = 0; key < 10; key++) {
            column->append(key);
        }
    }
    Columns key_columns = {column};
    Buffer<AggDataPtr> agg_states(100);

    map.build_hash_map_inline(100, key_columns, &pool, &agg_states);

    // Simulate count update at offset 0.
    for (size_t i = 0; i < 100; i++) {
        ASSERT_NE(agg_states[i], nullptr);
        (*reinterpret_cast<int64_t*>(agg_states[i])) += 1;
    }

    // Verify: iterate hash map, each key should have count = 10.
    for (auto it = map.hash_map.begin(); it != map.hash_map.end(); ++it) {
        int64_t count = *reinterpret_cast<int64_t*>(&it->second);
        ASSERT_EQ(count, 10) << "Key " << it->first << " has count " << count << " (expected 10)";
    }
}

} // namespace starrocks
