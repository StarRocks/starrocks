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

#include <cstdint>
#include <string>
#include <string_view>

#include "util/murmur_hash3.h"

namespace starrocks {

// DataCache keys embed murmur_hash3_x64_64(file_name): they are persisted in the local disk cache
// and are sent to other BEs in PFetchDataCacheRequest.cache_key. If this test fails, the change
// under review silently orphans every cached block on upgrade and breaks peer-cache hits between
// mixed-version BEs -- fix the change, do not update the constants.
TEST(HashUtilTest, MurmurHash3X64_64GoldenVectors) {
    const std::string long_path(137, 'x');
    const struct {
        std::string_view data;
        uint64_t seed;
        uint64_t expected;
    } cases[] = {
            {"a", 0UL, 0x1d3547705c43c07eUL},
            {"abc", 0UL, 0x37cf0eb5f11c0ee3UL},
            {"hdfs://ns/warehouse/db/tbl/part-00000.parquet", 0UL, 0xd4436bccbc0623ceUL},
            {"s3://bucket/db/tbl/data/00000-0-1a2b3c.parquet", 0UL, 0xeb93e55c35c63745UL},
            {long_path, 0UL, 0x0b58ebb2a26c715dUL},
    };
    for (const auto& c : cases) {
        uint64_t hash = 0;
        murmur_hash3_x64_64(c.data.data(), static_cast<int>(c.data.size()), c.seed, &hash);
        EXPECT_EQ(hash, c.expected);
    }
}

} // namespace starrocks
