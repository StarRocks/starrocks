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

#include "base/testutil/assert.h"
#include "exprs/table_function/list_rowsets.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class MockRuntimeState : public RuntimeState {
public:
    MockRuntimeState(ExecEnv* execEnv) : RuntimeState(execEnv) {}
    MOCK_METHOD((ExecEnv*), exec_env, ());
};

class ListRowsetsTableFunctionTest : public ::testing::Test {
public:
    void SetUp() override {
        _state = new ListRowsets::MyState();
        _execEnv = new ExecEnv();
        _runtimeState = new MockRuntimeState(_execEnv);
    }

    void TearDown() override {
        delete _state;
        delete _execEnv;
        delete _runtimeState;
    }

protected:
    ListRowsets::MyState* _state;
    MockRuntimeState* _runtimeState;
    ExecEnv* _execEnv;
};

TEST_F(ListRowsetsTableFunctionTest, basic_test) {
    auto tablet_id_column = Int64Column::create();
    tablet_id_column->append(12345);
    auto tablet_version_column = Int64Column::create();
    tablet_version_column->append(1);

    Columns input_columns;
    input_columns.push_back(tablet_id_column);
    input_columns.push_back(tablet_version_column);

    _state->set_params(input_columns);
    EXPECT_CALL(*_runtimeState, exec_env()).WillRepeatedly(testing::Return(nullptr));

    ListRowsets list_rowsets_func;
    auto result = list_rowsets_func.process(_runtimeState, _state);
    ASSERT_EQ("Only works for tablets in the cloud-native table", _state->status().message());
}
} // namespace starrocks
