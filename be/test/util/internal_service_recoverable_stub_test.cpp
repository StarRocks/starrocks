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

#include "util/internal_service_recoverable_stub.h"

#include <gtest/gtest.h>

#include "util/ref_count_closure.h"

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
