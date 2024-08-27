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

#include "storage/replication_utils.h"

#include <gtest/gtest.h>

namespace starrocks {

class ReplicationUtilsTest : public testing::Test {
public:
    ReplicationUtilsTest() = default;
    ~ReplicationUtilsTest() override = default;

    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(ReplicationUtilsTest, test_convert_column_unique_ids) {
    std::vector<uint32_t> column_unique_ids = {1, 2};
    std::unordered_map<uint32_t, uint32_t> column_unique_id_map = {{1, 10}, {2, 20}};

    auto status = ReplicationUtils::convert_column_unique_ids(&column_unique_ids, column_unique_id_map);
    EXPECT_TRUE(status.ok()) << status;

    column_unique_id_map.erase(1);
    status = ReplicationUtils::convert_column_unique_ids(&column_unique_ids, column_unique_id_map);
    EXPECT_FALSE(status.ok()) << status;

    column_unique_id_map.clear();
    status = ReplicationUtils::convert_column_unique_ids(&column_unique_ids, column_unique_id_map);
    EXPECT_TRUE(status.ok()) << status;
}

} // namespace starrocks
