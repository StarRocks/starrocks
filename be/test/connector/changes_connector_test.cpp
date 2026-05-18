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

#include "connector/changes_connector.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <climits>
#include <vector>

#include "column/fixed_length_column.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace starrocks::connector {

class ChangesConnectorTest : public ::testing::Test {
public:
    void SetUp() override { _exec_env = ExecEnv::GetInstance(); }

protected:
    ExecEnv* _exec_env = nullptr;
};

// Test ChangesConnector type
TEST_F(ChangesConnectorTest, test_connector_type) {
    ChangesConnector connector;
    EXPECT_EQ(connector.connector_type(), ConnectorType::CHANGES);
}

} // namespace starrocks::connector
