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

#include "exec/pipeline/sink/connector_sink_operator.h"

#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <future>
#include <thread>

#include "testutil/assert.h"
#include "util/defer_op.h"
#include "connector_sink/connector_chunk_sink.h"

namespace starrocks::pipeline {
namespace {

class ConnectorSinkOperatorTest : public ::testing::Test {
protected:
    void SetUp() override {
        _fragment_context = _pool.add(new FragmentContext);
        _fragment_context->set_runtime_state(std::make_shared<RuntimeState>());
        _runtime_state = _fragment_context->runtime_state();
    }

    void TearDown() override {}

    ObjectPool _pool;
    FragmentContext* _fragment_context;
    RuntimeState* _runtime_state;
};

class MockConnectorChunkSink : public connector::ConnectorChunkSink {
public:
    MOCK_METHOD(Status, init, (), (override));
    MOCK_METHOD(StatusOr<Futures>, add, (ChunkPtr), (override));
    MOCK_METHOD(Futures, finish, (), (override));
    MOCK_METHOD(std::function<void(const formats::FileWriter::CommitResult& result)>, callback_on_success, (), (override));
};

using ::testing::Return;

TEST_F(ConnectorSinkOperatorTest, test_basic) {
    {
        auto mock_sink = std::make_unique<MockConnectorChunkSink>();
        ON_CALL(*mock_sink, init()).WillByDefault(Return(Status::Unknown("error")));
        auto op = std::make_unique<ConnectorSinkOperator>(nullptr, 0, 0, 0, std::move(mock_sink), nullptr);
        EXPECT_ERROR(op->prepare(_runtime_state));
    }
}

} // namespace
} // namespace starrocks::pipeline
