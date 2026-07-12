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

#include "base/testutil/assert.h"
#include "common/object_pool.h"
#include "exec/schema_scanner/schema_be_datacache_metrics_scanner.h"

namespace starrocks {

TEST(SchemaScannerCacheTest, InitializesNineColumnSchemaWithoutCache) {
    SchemaBeDataCacheMetricsScanner scanner;
    SchemaScannerParam params;
    ObjectPool pool;

    EXPECT_OK(scanner.init(&params, &pool));

    const auto& slots = scanner.get_slot_descs();
    ASSERT_EQ(9, slots.size());

    constexpr const char* expected_names[] = {"BE_ID",           "STATUS",          "DISK_QUOTA_BYTES",
                                              "DISK_USED_BYTES", "MEM_QUOTA_BYTES", "MEM_USED_BYTES",
                                              "META_USED_BYTES", "DIR_SPACES",      "USED_BYTES_DETAIL"};
    for (size_t i = 0; i < slots.size(); ++i) {
        EXPECT_EQ(expected_names[i], slots[i]->col_name());
    }
}

} // namespace starrocks
