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
#include "base/testutil/scoped_updater.h"
#include "common/config_network_fwd.h"

using namespace starrocks;

class PInternalService_RecoverableStubTest : public testing::Test {
public:
    PInternalService_RecoverableStubTest() = default;
    ~PInternalService_RecoverableStubTest() override = default;
};

TEST_F(PInternalService_RecoverableStubTest, execute_command) {
    std::shared_ptr<starrocks::PInternalService_RecoverableStub> stub;

    butil::EndPoint point;
    auto res = butil::str2endpoint("127.0.0.1", 8000, &point);
    ASSERT_EQ(res, 0);

    stub = std::make_shared<starrocks::PInternalService_RecoverableStub>(point);

    auto st = stub->reset_channel();
    ASSERT_TRUE(st.ok());

    auto* closure = new starrocks::RefCountClosure<starrocks::ExecuteCommandResultPB>();
    ExecuteCommandRequestPB request;
    stub->execute_command(&closure->cntl, &request, &closure->result, closure);
}

TEST_F(PInternalService_RecoverableStubTest, tablet_writer_add_chunks_via_http) {
    std::shared_ptr<starrocks::PInternalService_RecoverableStub> stub;

    butil::EndPoint point;
    auto res = butil::str2endpoint("127.0.0.1", 8000, &point);
    ASSERT_EQ(res, 0);

    stub = std::make_shared<starrocks::PInternalService_RecoverableStub>(point);

    auto st = stub->reset_channel();
    ASSERT_TRUE(st.ok());

    auto* closure = new starrocks::RefCountClosure<starrocks::PTabletWriterAddBatchResult>();
    stub->tablet_writer_add_chunks_via_http(&closure->cntl, nullptr, &closure->result, closure);

    auto* closure1 = new starrocks::RefCountClosure<starrocks::PTabletWriterAddBatchResult>();
    stub->tablet_writer_add_chunk_via_http(&closure1->cntl, nullptr, &closure1->result, closure1);

    auto* closure2 = new starrocks::RefCountClosure<starrocks::PTabletWriterAddBatchResult>();
    stub->tablet_writer_add_chunk(&closure2->cntl, nullptr, &closure2->result, closure2);
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
    auto* closure = new starrocks::RefCountClosure<starrocks::PLoadDiagnoseResult>();
    stub->load_diagnose(&closure->cntl, &request, &closure->result, closure);
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
    auto* closure = new starrocks::RefCountClosure<starrocks::PLoadReplicaStatusResult>();
    stub->get_load_replica_status(&closure->cntl, &request, &closure->result, closure);
}

// The limiter is what makes brpc_max_inflight_rpc_per_stub a hard cap on connections under
// connection_type=pooled, where a stub holds one pooled connection per in-flight RPC. A refused
// acquire that still consumed a slot would wedge the stub permanently, so the counter after a
// rejection is asserted explicitly.
TEST_F(PInternalService_RecoverableStubTest, inflight_limit) {
    butil::EndPoint point;
    ASSERT_EQ(0, butil::str2endpoint("127.0.0.1", 8000, &point));
    auto stub = std::make_shared<starrocks::PInternalService_RecoverableStub>(point);

    {
        // Disabled by default. Acquires never fail but are still counted, so that callers may
        // always pair acquire with release and the in-flight count stays observable.
        SCOPED_UPDATE(int32_t, config::brpc_max_inflight_rpc_per_stub, 0);
        for (int i = 0; i < 8; ++i) {
            ASSERT_TRUE(stub->try_acquire_inflight());
        }
        ASSERT_EQ(8, stub->inflight());
        for (int i = 0; i < 8; ++i) {
            stub->release_inflight();
        }
        ASSERT_EQ(0, stub->inflight());
        ASSERT_EQ(0, stub->rejected());
    }

    {
        SCOPED_UPDATE(int32_t, config::brpc_max_inflight_rpc_per_stub, 2);
        ASSERT_TRUE(stub->try_acquire_inflight());
        ASSERT_TRUE(stub->try_acquire_inflight());
        ASSERT_FALSE(stub->try_acquire_inflight());
        // The refused acquire must roll its increment back, otherwise the stub loses a slot forever.
        ASSERT_EQ(2, stub->inflight());
        ASSERT_EQ(1, stub->rejected());

        // Releasing one admits exactly one more.
        stub->release_inflight();
        ASSERT_TRUE(stub->try_acquire_inflight());
        ASSERT_FALSE(stub->try_acquire_inflight());
        ASSERT_EQ(2, stub->inflight());
        ASSERT_EQ(2, stub->rejected());

        // The limit is read on every acquire, so raising it takes effect without a restart.
        config::brpc_max_inflight_rpc_per_stub = 3;
        ASSERT_TRUE(stub->try_acquire_inflight());
        ASSERT_EQ(3, stub->inflight());

        stub->release_inflight();
        stub->release_inflight();
        stub->release_inflight();
        ASSERT_EQ(0, stub->inflight());
    }
}
