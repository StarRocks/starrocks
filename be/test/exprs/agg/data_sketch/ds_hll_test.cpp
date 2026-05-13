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

#include "exprs/agg/data_sketch/ds_hll.h"

#include <gtest/gtest.h>

#include <vector>

#include "base/string/slice.h"
#include "types/constexpr.h"

namespace starrocks {

TEST(DataSketchesHllTest, SerializeDeserialize) {
    int64_t memory_usage = 0;
    DataSketchesHll hll(DEFAULT_HLL_LOG_K, DataSketchesHll::DEFAULT_HLL_TGT_TYPE, &memory_usage);
    for (int i = 0; i < 100; ++i) {
        hll.update(i);
    }

    std::vector<uint8_t> buffer(hll.max_serialized_size());
    const size_t size = hll.serialize(buffer.data());
    ASSERT_EQ(size, hll.serialize_size());
    ASSERT_TRUE(DataSketchesHll::is_valid(Slice(buffer.data(), size)));
    ASSERT_NEAR(hll.estimate_cardinality(), 100, 1);

    DataSketchesHll deserialized(Slice(buffer.data(), size), &memory_usage);
    ASSERT_EQ(deserialized.serialize_size(), size);
    ASSERT_NEAR(deserialized.estimate_cardinality(), 100, 1);
}

TEST(DataSketchesHllTest, Merge) {
    int64_t memory_usage = 0;
    DataSketchesHll hll1(DEFAULT_HLL_LOG_K, DataSketchesHll::DEFAULT_HLL_TGT_TYPE, &memory_usage);
    DataSketchesHll hll2(DEFAULT_HLL_LOG_K, DataSketchesHll::DEFAULT_HLL_TGT_TYPE, &memory_usage);
    for (int i = 0; i < 100; ++i) {
        hll1.update(i);
        hll2.update(i);
    }

    DataSketchesHll merged(DEFAULT_HLL_LOG_K, DataSketchesHll::DEFAULT_HLL_TGT_TYPE, &memory_usage);
    merged.merge(hll1);
    merged.merge(hll2);

    ASSERT_NEAR(merged.estimate_cardinality(), 100, 1);
}

TEST(DataSketchesHllTest, RejectsInvalidSlice) {
    ASSERT_FALSE(DataSketchesHll::is_valid(Slice()));
}

} // namespace starrocks
