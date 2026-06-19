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

#include "service/core_dump_resource_releaser.h"

#include <gtest/gtest.h>

namespace starrocks {

TEST(CoreDumpResourceSelectorTest, EmptyConfigReleasesNothing) {
    CoreDumpResourceSelector selector("");

    EXPECT_FALSE(selector.release_all());
    EXPECT_TRUE(selector.modules().empty());
    EXPECT_FALSE(selector.should_release("data_cache"));
    EXPECT_FALSE(selector.should_release("connector_scan_executor"));
}

TEST(CoreDumpResourceSelectorTest, StarReleasesAllResources) {
    CoreDumpResourceSelector selector("*");

    EXPECT_TRUE(selector.release_all());
    EXPECT_TRUE(selector.should_release("data_cache"));
    EXPECT_TRUE(selector.should_release("connector_scan_executor"));
    EXPECT_TRUE(selector.should_release("unknown_resource"));
}

TEST(CoreDumpResourceSelectorTest, ParsesTrimmedCommaSeparatedResources) {
    CoreDumpResourceSelector selector(" data_cache, connector_scan_executor ,olap_scan_executor ");

    EXPECT_FALSE(selector.release_all());
    EXPECT_TRUE(selector.should_release("data_cache"));
    EXPECT_TRUE(selector.should_release("connector_scan_executor"));
    EXPECT_TRUE(selector.should_release("olap_scan_executor"));
    EXPECT_FALSE(selector.should_release("query_rpc_thread_pool"));
}

TEST(CoreDumpResourceSelectorTest, LowercasesResourceNames) {
    CoreDumpResourceSelector selector("DATA_CACHE, Query_RPC_Thread_Pool");

    EXPECT_TRUE(selector.should_release("data_cache"));
    EXPECT_TRUE(selector.should_release("query_rpc_thread_pool"));
    EXPECT_FALSE(selector.should_release("DATA_CACHE"));
}

TEST(CoreDumpResourceSelectorTest, KeepsConfiguredResourceNames) {
    CoreDumpResourceSelector selector("unknown,, , data_cache");

    EXPECT_TRUE(selector.should_release("data_cache"));
    EXPECT_TRUE(selector.should_release("unknown"));
    EXPECT_FALSE(selector.should_release("connector_scan_executor"));
    EXPECT_EQ(2, selector.modules().size());
}

} // namespace starrocks
