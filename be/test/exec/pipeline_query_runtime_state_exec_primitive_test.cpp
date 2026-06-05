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

#include <chrono>
#include <thread>

#include "exec/pipeline/primitives/query_runtime_state.h"

namespace starrocks::pipeline {

TEST(QueryRuntimeStateTest, StoresQueryId) {
    QueryRuntimeState state;
    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 2;

    state.set_query_id(query_id);

    EXPECT_EQ(1, state.query_id().hi);
    EXPECT_EQ(2, state.query_id().lo);
}

TEST(QueryRuntimeStateTest, TracksQueryAndDeliveryExpiry) {
    QueryRuntimeState state;

    EXPECT_EQ(QueryRuntimeState::DEFAULT_EXPIRE_SECONDS, state.get_query_expire_seconds());

    state.set_delivery_expire_seconds(1);
    state.set_query_expire_seconds(1);
    state.extend_delivery_lifetime();
    state.extend_query_lifetime();

    EXPECT_FALSE(state.is_delivery_expired());
    EXPECT_FALSE(state.is_query_expired());
}

TEST(QueryRuntimeStateTest, ExpiresDeliveryAndQueryIndependently) {
    QueryRuntimeState state;

    state.set_delivery_expire_seconds(0);
    state.set_query_expire_seconds(1);
    state.extend_delivery_lifetime();
    state.extend_query_lifetime();
    std::this_thread::sleep_for(std::chrono::milliseconds(2));

    EXPECT_TRUE(state.is_delivery_expired());
    EXPECT_FALSE(state.is_query_expired());

    state.set_delivery_expire_seconds(1);
    state.set_query_expire_seconds(0);
    state.extend_delivery_lifetime();
    state.extend_query_lifetime();
    std::this_thread::sleep_for(std::chrono::milliseconds(2));

    EXPECT_FALSE(state.is_delivery_expired());
    EXPECT_TRUE(state.is_query_expired());
}

} // namespace starrocks::pipeline
