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

#include "exec/multi_olap_table_sink.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "runtime/runtime_state.h"

using ::testing::_;
using ::testing::Return;
using ::testing::AtLeast;
using ::testing::DoAll;
using ::testing::SetArgPointee;

namespace starrocks {

class MockOlapTableSink : public OlapTableSink {
public:
    MockOlapTableSink(ObjectPool* pool, const std::vector<TExpr>& texprs, Status* status, RuntimeState* state)
            : OlapTableSink(pool, texprs, status, state) {}

    MOCK_METHOD(Status, init, (const TDataSink& sink, RuntimeState* state), (override));
    MOCK_METHOD(Status, prepare, (RuntimeState * state), (override));
    MOCK_METHOD(Status, open, (RuntimeState * state), (override));
    MOCK_METHOD(Status, send_chunk_nonblocking, (RuntimeState * state, Chunk* chunk), (override));
    MOCK_METHOD(Status, close, (RuntimeState * state, Status close_status), (override));
    // Add other methods as needed for your tests
};

class MultiOlapTableSinkTest : public ::testing::Test {
protected:
    std::unique_ptr<MultiOlapTableSink> multi_sink;
    ObjectPool pool;
    std::vector<TExpr> texprs;
    TDataSink t_data_sink;
    RuntimeState state;

    void SetUp() override {
        multi_sink = std::make_unique<MultiOlapTableSink>(&pool, texprs);
        // Configure t_data_sink and state as necessary
    }

    void TearDown() override { multi_sink.reset(); }

    void addMockSinks(int num_sinks) {
        for (int i = 0; i < num_sinks; ++i) {
            auto mock_sink = std::make_unique<MockOlapTableSink>(&pool, texprs, nullptr, &state);
            multi_sink->add_olap_table_sink(std::move(mock_sink));
        }
    }
};

TEST_F(MultiOlapTableSinkTest, SendChunkFailure) {
    addMockSinks(1);
    EXPECT_CALL(*(static_cast<MockOlapTableSink*>(multi_sink->get_olap_table_sink(0).get())),
                send_chunk_nonblocking(_, _))
            .WillOnce(Return(Status::InternalError("Send failed")));

    Chunk chunk;
    EXPECT_FALSE(multi_sink->send_chunk_nonblocking(&state, &chunk).ok());
}

TEST_F(MultiOlapTableSinkTest, PreparationSuccess) {
    addMockSinks(2); // Assuming multiple sinks could be tested for preparation
    for (int i = 0; i < 2; ++i) {
        EXPECT_CALL(*(static_cast<MockOlapTableSink*>(multi_sink->get_olap_table_sink(i).get())), prepare(_))
                .WillOnce(Return(Status::OK()));
    }

    EXPECT_TRUE(multi_sink->prepare(&state).ok());
}

TEST_F(MultiOlapTableSinkTest, OpenSuccess) {
    addMockSinks(1);
    EXPECT_CALL(*(static_cast<MockOlapTableSink*>(multi_sink->get_olap_table_sink(0).get())), open(_))
            .WillOnce(Return(Status::OK()));

    EXPECT_TRUE(multi_sink->open(&state).ok());
}

TEST_F(MultiOlapTableSinkTest, CloseSuccess) {
    addMockSinks(1);
    EXPECT_CALL(*(static_cast<MockOlapTableSink*>(multi_sink->get_olap_table_sink(0).get())), close(_, _))
            .WillOnce(Return(Status::OK()));

    EXPECT_TRUE(multi_sink->close(&state, Status::OK()).ok());
}

TEST_F(MultiOlapTableSinkTest, MultipleSinksSendChunk) {
    addMockSinks(2); // Testing with two sinks
    Chunk chunk;
    for (int i = 0; i < 2; ++i) {
        EXPECT_CALL(*(static_cast<MockOlapTableSink*>(multi_sink->get_olap_table_sink(i).get())),
                    send_chunk_nonblocking(_, &chunk))
                .WillOnce(Return(Status::OK()));
    }

    EXPECT_TRUE(multi_sink->send_chunk_nonblocking(&state, &chunk).ok());
}

TEST_F(MultiOlapTableSinkTest, PreparationPartialFailure) {
    addMockSinks(2);
    EXPECT_CALL(*(static_cast<MockOlapTableSink*>(multi_sink->get_olap_table_sink(0).get())), prepare(_))
            .WillOnce(Return(Status::OK()));
    EXPECT_CALL(*(static_cast<MockOlapTableSink*>(multi_sink->get_olap_table_sink(1).get())), prepare(_))
            .WillOnce(Return(Status::RuntimeError("Preparation failed")));

    EXPECT_FALSE(multi_sink->prepare(&state).ok());
}

TEST_F(MultiOlapTableSinkTest, OpenPartialFailure) {
    addMockSinks(2);
    EXPECT_CALL(*(static_cast<MockOlapTableSink*>(multi_sink->get_olap_table_sink(0).get())), open(_))
            .WillOnce(Return(Status::OK()));
    EXPECT_CALL(*(static_cast<MockOlapTableSink*>(multi_sink->get_olap_table_sink(1).get())), open(_))
            .WillOnce(Return(Status::IOError("Open failed")));

    EXPECT_FALSE(multi_sink->open(&state).ok());
}

TEST_F(MultiOlapTableSinkTest, SendChunkPartialFailure) {
    addMockSinks(2);
    Chunk chunk;
    EXPECT_CALL(*(static_cast<MockOlapTableSink*>(multi_sink->get_olap_table_sink(0).get())),
                send_chunk_nonblocking(_, &chunk))
            .WillOnce(Return(Status::OK()));
    EXPECT_CALL(*(static_cast<MockOlapTableSink*>(multi_sink->get_olap_table_sink(1).get())),
                send_chunk_nonblocking(_, &chunk))
            .WillOnce(Return(Status::InternalError("Send chunk failed")));

    EXPECT_FALSE(multi_sink->send_chunk_nonblocking(&state, &chunk).ok());
}

TEST_F(MultiOlapTableSinkTest, ClosePartialFailure) {
    addMockSinks(2);
    EXPECT_CALL(*(static_cast<MockOlapTableSink*>(multi_sink->get_olap_table_sink(0).get())), close(_, _))
            .WillOnce(Return(Status::OK()));
    EXPECT_CALL(*(static_cast<MockOlapTableSink*>(multi_sink->get_olap_table_sink(1).get())), close(_, _))
            .WillOnce(Return(Status::Cancelled("Close cancelled")));

    EXPECT_FALSE(multi_sink->close(&state, Status::OK()).ok());
}

} // namespace starrocks