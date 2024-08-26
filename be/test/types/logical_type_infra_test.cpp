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

#include "types/logical_type_infra.h"

#include <gtest/gtest.h>

#include "common/object_pool.h"
#include "exprs/runtime_filter.h"

namespace starrocks {
class LogicalTypeInfraTest : public ::testing::Test {
protected:
    struct FilterBuilder {
        template <LogicalType lt>
        JoinRuntimeFilter* operator()() {
            return new RuntimeBloomFilter<lt>();
        }
    };
};

TEST_F(LogicalTypeInfraTest, test_type_dispatch_filter) {
    ObjectPool pool;

    auto* filter = type_dispatch_filter(TYPE_VARCHAR, (JoinRuntimeFilter*)nullptr, FilterBuilder());
    ASSERT_TRUE(filter != nullptr);

    filter = type_dispatch_filter(TYPE_JSON, (JoinRuntimeFilter*)nullptr, FilterBuilder());
    ASSERT_TRUE(filter == nullptr);
}
} // namespace starrocks