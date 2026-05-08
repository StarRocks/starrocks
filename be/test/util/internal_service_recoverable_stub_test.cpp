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

#include "common/brpc/internal_service_recoverable_stub.h"

#include <gtest/gtest.h>

#include "base/brpc/ref_count_closure.h"

using namespace starrocks;

class PInternalService_RecoverableStubTest : public testing::Test {
public:
    PInternalService_RecoverableStubTest() = default;
    ~PInternalService_RecoverableStubTest() override = default;
};

template <typename Result, typename Callback>
void invoke_and_release_closure(Callback&& callback) {
    auto* closure = new starrocks::RefCountClosure<Result>();
    closure->ref();
    closure->ref();
    callback(closure);
    closure->join();
    if (closure->unref()) {
        delete closure;
    }
}

TEST_F(PInternalService_RecoverableStubTest, execute_command) {
    std::shared_ptr<starrocks::PInternalService_RecoverableStub> stub;

    butil::EndPoint point;
    auto res = butil::str2endpoint("127.0.0.1", 8000, &point);
    ASSERT_EQ(res, 0);

    stub = std::make_shared<starrocks::PInternalService_RecoverableStub>(point);

    auto st = stub->reset_channel();
    ASSERT_TRUE(st.ok());

    ExecuteCommandRequestPB request;
    invoke_and_release_closure<starrocks::ExecuteCommandResultPB>(
            [&](auto* closure) { stub->execute_command(&closure->cntl, &request, &closure->result, closure); });
}

TEST_F(PInternalService_RecoverableStubTest, tablet_writer_add_chunks_via_http) {
    std::shared_ptr<starrocks::PInternalService_RecoverableStub> stub;

    butil::EndPoint point;
    auto res = butil::str2endpoint("127.0.0.1", 8000, &point);
    ASSERT_EQ(res, 0);

    stub = std::make_shared<starrocks::PInternalService_RecoverableStub>(point);

    auto st = stub->reset_channel();
    ASSERT_TRUE(st.ok());

    invoke_and_release_closure<starrocks::PTabletWriterAddBatchResult>([&](auto* closure) {
        stub->tablet_writer_add_chunks_via_http(&closure->cntl, nullptr, &closure->result, closure);
    });

    invoke_and_release_closure<starrocks::PTabletWriterAddBatchResult>([&](auto* closure) {
        stub->tablet_writer_add_chunk_via_http(&closure->cntl, nullptr, &closure->result, closure);
    });

    invoke_and_release_closure<starrocks::PTabletWriterAddBatchResult>(
            [&](auto* closure) { stub->tablet_writer_add_chunk(&closure->cntl, nullptr, &closure->result, closure); });
}

TEST_F(PInternalService_RecoverableStubTest, test_load_diagnose) {
    std::shared_ptr<starrocks::PInternalService_RecoverableStub> stub;

    butil::EndPoint point;
    auto res = butil::str2endpoint("127.0.0.1", 8000, &point);
    ASSERT_EQ(res, 0);

    stub = std::make_shared<starrocks::PInternalService_RecoverableStub>(point);

    auto st = stub->reset_channel();
    ASSERT_TRUE(st.ok());

    PLoadDiagnoseRequest request;
    invoke_and_release_closure<starrocks::PLoadDiagnoseResult>(
            [&](auto* closure) { stub->load_diagnose(&closure->cntl, &request, &closure->result, closure); });
}

TEST_F(PInternalService_RecoverableStubTest, test_get_load_replica_status) {
    std::shared_ptr<starrocks::PInternalService_RecoverableStub> stub;

    butil::EndPoint point;
    auto res = butil::str2endpoint("127.0.0.1", 8000, &point);
    ASSERT_EQ(res, 0);

    stub = std::make_shared<starrocks::PInternalService_RecoverableStub>(point);

    auto st = stub->reset_channel();
    ASSERT_TRUE(st.ok());

    PLoadReplicaStatusRequest request;
    invoke_and_release_closure<starrocks::PLoadReplicaStatusResult>(
            [&](auto* closure) { stub->get_load_replica_status(&closure->cntl, &request, &closure->result, closure); });
}
