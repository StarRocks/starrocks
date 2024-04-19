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

#include <gmock/gmock.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

#include <future>
#include <thread>

#include "connector/connector_chunk_sink.h"
#include "connector/hive_chunk_sink.h"
#include "formats/utils.h"
#include "testutil/assert.h"
#include "util/defer_op.h"

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
    MOCK_METHOD(std::function<void(const formats::FileWriter::CommitResult& result)>, callback_on_success, (),
                (override));
};

using ::testing::Return;
using ::testing::ByMove;
using Futures = connector::ConnectorChunkSink::Futures;
using CommitResult = formats::FileWriter::CommitResult;
using ::testing::_;

TEST_F(ConnectorSinkOperatorTest, test_prepare) {
    {
        auto mock_sink = std::make_unique<MockConnectorChunkSink>();
        EXPECT_CALL(*mock_sink, init()).WillOnce(Return(Status::Unknown("error")));
        auto op = std::make_unique<ConnectorSinkOperator>(nullptr, 0, 0, 0, std::move(mock_sink), nullptr);
        ASSERT_ERROR(op->prepare(_runtime_state));
        ASSERT_FALSE(op->has_output());
    }

    {
        auto mock_sink = std::make_unique<MockConnectorChunkSink>();
        EXPECT_CALL(*mock_sink, init()).WillOnce(Return(Status::OK()));
        auto op = std::make_unique<ConnectorSinkOperator>(nullptr, 0, 0, 0, std::move(mock_sink), nullptr);
        ASSERT_OK(op->prepare(_runtime_state));
        ASSERT_FALSE(op->has_output());
    }
}

TEST_F(ConnectorSinkOperatorTest, test_push_chunk) {
    {
        auto mock_sink = std::make_unique<MockConnectorChunkSink>();
        EXPECT_CALL(*mock_sink, add(_)).WillOnce(Return(ByMove(Futures{})));   // don't block
        EXPECT_CALL(*mock_sink, finish()).WillOnce(Return(ByMove(Futures{}))); // don't block
        auto op = std::make_unique<ConnectorSinkOperator>(nullptr, 0, 0, 0, std::move(mock_sink), nullptr);
        auto chunk = std::make_shared<Chunk>();
        EXPECT_TRUE(op->need_input());
        EXPECT_OK(op->push_chunk(_runtime_state, chunk));
        EXPECT_TRUE(op->need_input()); // ready immediately
        EXPECT_FALSE(op->is_finished());
        EXPECT_OK(op->set_finishing(_runtime_state));
        EXPECT_TRUE(op->is_finished());
    }

    {
        auto mock_sink = std::make_unique<MockConnectorChunkSink>();
        auto promise = std::promise<Status>();
        auto futures = Futures{};
        futures.add_chunk_futures.push_back(promise.get_future()); // block
        EXPECT_CALL(*mock_sink, add(_)).WillOnce(Return(ByMove(std::move(futures))));
        EXPECT_CALL(*mock_sink, finish()).WillOnce(Return(ByMove(Futures{}))); // don't block
        auto op = std::make_unique<ConnectorSinkOperator>(nullptr, 0, 0, 0, std::move(mock_sink), nullptr);
        auto chunk = std::make_shared<Chunk>();
        EXPECT_TRUE(op->need_input());
        EXPECT_OK(op->push_chunk(_runtime_state, chunk));
        EXPECT_FALSE(op->need_input()); // block on flushing rowgroup
        promise.set_value(Status::OK());
        EXPECT_TRUE(op->need_input()); // can accept more chunks
        EXPECT_OK(op->set_finishing(_runtime_state));
        EXPECT_TRUE(op->is_finished());
        EXPECT_FALSE(op->pending_finish());
    }

    {
        auto mock_sink = std::make_unique<MockConnectorChunkSink>();
        auto promise = std::promise<CommitResult>();
        auto futures = Futures{};
        futures.commit_file_futures.push_back(promise.get_future()); // block
        EXPECT_CALL(*mock_sink, add(_)).WillOnce(Return(ByMove(std::move(futures))));
        EXPECT_CALL(*mock_sink, finish()).WillOnce(Return(ByMove(Futures{})));                  // don't block
        EXPECT_CALL(*mock_sink, callback_on_success()).WillOnce(Return([](CommitResult r) {})); // don't block
        auto op = std::make_unique<ConnectorSinkOperator>(nullptr, 0, 0, 0, std::move(mock_sink), nullptr);
        auto chunk = std::make_shared<Chunk>();
        EXPECT_TRUE(op->need_input());
        EXPECT_OK(op->push_chunk(_runtime_state, chunk));
        EXPECT_TRUE(op->need_input());
        EXPECT_OK(op->set_finishing(_runtime_state));
        EXPECT_FALSE(op->is_finished());
        promise.set_value(CommitResult{
                .io_status = Status::OK(),
                .rollback_action = []() {},
        });
        EXPECT_TRUE(op->is_finished());
        EXPECT_FALSE(op->pending_finish());
    }
}

TEST_F(ConnectorSinkOperatorTest, test_push_chunk_error) {
    {
        auto mock_sink = std::make_unique<MockConnectorChunkSink>();
        auto promise = std::promise<Status>();
        auto futures = Futures{};
        futures.add_chunk_futures.push_back(promise.get_future()); // block
        EXPECT_CALL(*mock_sink, add(_)).WillOnce(Return(ByMove(std::move(futures))));
        auto op = std::make_unique<ConnectorSinkOperator>(nullptr, 0, 0, 0, std::move(mock_sink), _fragment_context);
        auto chunk = std::make_shared<Chunk>();
        EXPECT_TRUE(op->need_input());
        EXPECT_OK(op->push_chunk(_runtime_state, chunk));
        EXPECT_FALSE(op->need_input()); // block on flushing rowgroup
        promise.set_value(Status::IOError("io error"));
        EXPECT_TRUE(op->need_input()); // can accept more chunks
        EXPECT_TRUE(_fragment_context->is_canceled());
    }
}

TEST_F(ConnectorSinkOperatorTest, test_cleanup_after_cancel) {
    {
        bool cleanup = false;
        auto mock_sink = std::make_unique<MockConnectorChunkSink>();
        auto futures = Futures{};
        futures.commit_file_futures.push_back(make_ready_future(CommitResult{
                .io_status = Status::OK(),
                .rollback_action = [&]() { cleanup = true; },
        }));
        EXPECT_CALL(*mock_sink, finish()).WillOnce(Return(ByMove(std::move(futures))));         // don't block
        EXPECT_CALL(*mock_sink, callback_on_success()).WillOnce(Return([](CommitResult r) {})); // don't block
        auto op = std::make_unique<ConnectorSinkOperator>(nullptr, 0, 0, 0, std::move(mock_sink), _fragment_context);

        EXPECT_OK(op->set_finishing(_runtime_state));
        EXPECT_TRUE(op->is_finished());
        EXPECT_OK(op->set_cancelled(_runtime_state));
        EXPECT_FALSE(cleanup);
        op->close(_runtime_state); // execute rollback action
        EXPECT_TRUE(cleanup);
    }
}

TEST_F(ConnectorSinkOperatorTest, test_factory) {
    {
        auto provider = std::make_unique<connector::HiveChunkSinkProvider>();
        auto sink_ctx = std::make_shared<connector::HiveChunkSinkContext>();
        sink_ctx->path = "/path/to/directory/";
        sink_ctx->data_column_names = {"k1"};
        sink_ctx->partition_column_names = {"k2"};
        sink_ctx->data_column_evaluators =
                ColumnSlotIdEvaluator::from_types({TypeDescriptor::from_logical_type(TYPE_VARCHAR)});
        sink_ctx->partition_column_evaluators =
                ColumnSlotIdEvaluator::from_types({TypeDescriptor::from_logical_type(TYPE_INT)});
        sink_ctx->executor = nullptr;
        sink_ctx->format = formats::PARQUET;
        sink_ctx->compression_type = TCompressionType::NO_COMPRESSION;
        sink_ctx->options = {}; // default for now
        sink_ctx->max_file_size = 1 << 30;
        sink_ctx->fragment_context = _fragment_context;
        auto op_factory =
                std::make_unique<ConnectorSinkOperatorFactory>(0, std::move(provider), sink_ctx, _fragment_context);
        auto mock_sink = std::make_unique<MockConnectorChunkSink>();
        auto op = op_factory->create(1, 0);
        EXPECT_OK(op->prepare(_runtime_state));
    }
}

} // namespace
} // namespace starrocks::pipeline
