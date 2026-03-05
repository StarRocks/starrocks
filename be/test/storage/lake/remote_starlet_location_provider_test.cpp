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

#ifdef USE_STAROS

#include "storage/lake/remote_starlet_location_provider.h"

#include <gtest/gtest.h>

namespace starrocks::lake {

class RemoteStarletLocationProviderTest : public testing::Test {
public:
    RemoteStarletLocationProviderTest() = default;
    ~RemoteStarletLocationProviderTest() override = default;
};

TEST_F(RemoteStarletLocationProviderTest, test_metadata_root_location) {
    RemoteStarletLocationProvider provider;

    int64_t tablet_id = 12345;
    int64_t db_id = 100;
    int64_t table_id = 200;
    int64_t partition_id = 300;

    std::string meta_location = provider.metadata_root_location(tablet_id, db_id, table_id, partition_id);

    // Expected format: staros://{tablet_id}/db{db_id}/{table_id}/{partition_id}/meta
    std::string expected = "staros://12345/db100/200/300/meta";
    EXPECT_EQ(expected, meta_location);
}

TEST_F(RemoteStarletLocationProviderTest, test_segment_root_location) {
    RemoteStarletLocationProvider provider;

    int64_t tablet_id = 12345;
    int64_t db_id = 100;
    int64_t table_id = 200;
    int64_t partition_id = 300;

    std::string segment_location = provider.segment_root_location(tablet_id, db_id, table_id, partition_id);

    // Expected format: staros://{tablet_id}/db{db_id}/{table_id}/{partition_id}/data
    std::string expected = "staros://12345/db100/200/300/data";
    EXPECT_EQ(expected, segment_location);
}

TEST_F(RemoteStarletLocationProviderTest, test_root_location) {
    RemoteStarletLocationProvider provider;

    int64_t tablet_id = 12345;

    std::string root_loc = provider.root_location(tablet_id);

    // Expected format: staros://{tablet_id}
    std::string expected = "staros://12345";
    EXPECT_EQ(expected, root_loc);
}

} // namespace starrocks::lake

#endif // USE_STAROS
