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

} // namespace starrocks::pipeline
