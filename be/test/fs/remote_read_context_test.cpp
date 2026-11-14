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

#include "fs/remote_read_context.h"

#include <gtest/gtest.h>

#include <thread>

namespace starrocks {

static bool same(const RemoteReadContext& a, const RemoteReadContext& b) {
    return a.is_set == b.is_set && a.workload == b.workload && a.group_id == b.group_id &&
           a.group_weight == b.group_weight && a.group_config_version == b.group_config_version &&
           a.flow_id == b.flow_id;
}

TEST(RemoteReadContextTest, factories) {
    auto query = RemoteReadContext::for_query(7, 0, 3, 11);
    EXPECT_TRUE(query.is_set);
    EXPECT_EQ(RemoteReadContext::Workload::QUERY, query.workload);
    EXPECT_EQ(7, query.group_id);
    // A zero weight would starve the group: it is raised to 1.
    EXPECT_EQ(1, query.group_weight);
    EXPECT_EQ(3, query.group_config_version);
    EXPECT_EQ(11, query.flow_id);

    auto load = RemoteReadContext::for_load(5);
    EXPECT_TRUE(load.is_set);
    EXPECT_EQ(RemoteReadContext::Workload::LOAD, load.workload);
    EXPECT_EQ(0, load.group_id);
    EXPECT_EQ(1, load.group_weight);
    EXPECT_EQ(0, load.group_config_version);
    EXPECT_EQ(5, load.flow_id);

    auto compaction = RemoteReadContext::for_background(RemoteReadContext::Workload::COMPACTION, 9);
    EXPECT_EQ(RemoteReadContext::Workload::COMPACTION, compaction.workload);
    EXPECT_EQ(9, compaction.flow_id);

    RemoteReadContext unset;
    EXPECT_FALSE(unset.is_set);
    EXPECT_EQ(RemoteReadContext::Workload::OTHER_BACKGROUND, unset.workload);

    // One id, one flow; the halves are not interchangeable.
    EXPECT_EQ(RemoteReadContext::flow_id_of(1, 2), RemoteReadContext::flow_id_of(1, 2));
    EXPECT_NE(RemoteReadContext::flow_id_of(1, 2), RemoteReadContext::flow_id_of(2, 1));
}

TEST(RemoteReadContextTest, scopes_nest_and_restore) {
    const RemoteReadContext none;
    EXPECT_FALSE(ScopedRemoteReadContext::or_current(none).is_set);
    // Without any scope, the reads of a load are a load of their own.
    EXPECT_TRUE(same(RemoteReadContext::for_load(0), ScopedRemoteReadContext::current_or_load()));

    const auto outer = RemoteReadContext::for_query(1, 2, 3, 4);
    const auto inner = RemoteReadContext::for_load(5);
    {
        ScopedRemoteReadContext outer_scope(outer);
        EXPECT_TRUE(same(outer, ScopedRemoteReadContext::or_current(none)));
        EXPECT_TRUE(same(outer, ScopedRemoteReadContext::current_or_load()));
        {
            ScopedRemoteReadContext inner_scope(inner);
            EXPECT_TRUE(same(inner, ScopedRemoteReadContext::or_current(none)));
            {
                // An unset context leaves the enclosing one in place.
                ScopedRemoteReadContext unset_scope(none);
                EXPECT_TRUE(same(inner, ScopedRemoteReadContext::or_current(none)));
            }
            EXPECT_TRUE(same(inner, ScopedRemoteReadContext::or_current(none)));
        }
        EXPECT_TRUE(same(outer, ScopedRemoteReadContext::or_current(none)));
        // A context of the file's own wins over the scope.
        EXPECT_TRUE(same(inner, ScopedRemoteReadContext::or_current(inner)));
    }
    EXPECT_FALSE(ScopedRemoteReadContext::or_current(none).is_set);
}

TEST(RemoteReadContextTest, scope_is_per_thread) {
    ScopedRemoteReadContext scope(RemoteReadContext::for_query(1, 1, 0, 2));
    bool other_thread_set = true;
    std::thread t([&]() { other_thread_set = ScopedRemoteReadContext::or_current(RemoteReadContext{}).is_set; });
    t.join();
    EXPECT_FALSE(other_thread_set);
    EXPECT_TRUE(ScopedRemoteReadContext::or_current(RemoteReadContext{}).is_set);
}

} // namespace starrocks
