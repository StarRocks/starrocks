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

#include "base/brpc/recoverable_closure.h"

#include <errno.h>
#include <gtest/gtest.h>

#include <memory>

namespace starrocks {

namespace {

class CountingClosure final : public google::protobuf::Closure {
public:
    explicit CountingClosure(int* run_count) : _run_count(run_count) {}

    void Run() override { ++(*_run_count); }

private:
    int* _run_count;
};

class FakeRecoverableStub {
public:
    explicit FakeRecoverableStub(int64_t connection_group) : _connection_group(connection_group) {}

    int64_t connection_group() const { return _connection_group; }

    Status reset_channel(int64_t next_connection_group) {
        reset_called = true;
        reset_connection_group = next_connection_group;
        return reset_status;
    }

    bool reset_called = false;
    int64_t reset_connection_group = 0;
    Status reset_status = Status::OK();

private:
    int64_t _connection_group;
};

} // namespace

TEST(RecoverableClosureTest, HostDownTriggersReset) {
    auto stub = std::make_shared<FakeRecoverableStub>(7);
    brpc::Controller cntl;
    cntl.SetFailed(EHOSTDOWN, "host down");

    int run_count = 0;
    CountingClosure done(&run_count);

    auto* closure = new RecoverableClosure<FakeRecoverableStub>(stub, &cntl, &done);
    closure->Run();

    EXPECT_TRUE(stub->reset_called);
    EXPECT_EQ(8, stub->reset_connection_group);
    EXPECT_EQ(1, run_count);
}

TEST(RecoverableClosureTest, NonHostDownDoesNotReset) {
    auto stub = std::make_shared<FakeRecoverableStub>(11);
    brpc::Controller cntl;
    cntl.SetFailed(EINVAL, "not host down");

    int run_count = 0;
    CountingClosure done(&run_count);

    auto* closure = new RecoverableClosure<FakeRecoverableStub>(stub, &cntl, &done);
    closure->Run();

    EXPECT_FALSE(stub->reset_called);
    EXPECT_EQ(1, run_count);
}

TEST(RecoverableClosureTest, SuccessDoesNotResetAndRunsDone) {
    auto stub = std::make_shared<FakeRecoverableStub>(3);
    brpc::Controller cntl;

    int run_count = 0;
    CountingClosure done(&run_count);

    auto* closure = new RecoverableClosure<FakeRecoverableStub>(stub, &cntl, &done);
    closure->Run();

    EXPECT_FALSE(stub->reset_called);
    EXPECT_EQ(1, run_count);
}

} // namespace starrocks
