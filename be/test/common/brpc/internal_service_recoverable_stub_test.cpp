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

#include <brpc/closure_guard.h>
#include <brpc/server.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>

#include "base/brpc/ref_count_closure.h"
#include "common/brpc/presend_backoff_retry_policy.h"
#include "common/config_rpc_client_fwd.h"

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

namespace {

// Binds port 0, reads the assigned port back and closes the socket, so the port stays free for a
// while. Only used right before the RPC, so no other process is expected to take it.
int find_free_tcp_port() {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    struct sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    socklen_t addr_len = sizeof(addr);
    if (::bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) != 0 ||
        ::getsockname(fd, reinterpret_cast<struct sockaddr*>(&addr), &addr_len) != 0) {
        ::close(fd);
        return -1;
    }
    ::close(fd);
    return ntohs(addr.sin_port);
}

// Answers every execute_command RPC with an empty response.
class FakeInternalService : public PInternalService {
public:
    void execute_command(google::protobuf::RpcController* controller, const ExecuteCommandRequestPB* request,
                         ExecuteCommandResultPB* response, google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
    }
};

class BlockingClosure : public google::protobuf::Closure {
public:
    void Run() override { _promise.set_value(); }

    void wait() { _future.wait(); }

    brpc::Controller cntl;

private:
    std::promise<void> _promise;
    std::shared_future<void> _future = _promise.get_future();
};

} // namespace

TEST_F(PInternalService_RecoverableStubTest, pre_send_backoff_retry_policy) {
    const int32_t saved_backoff_ms = config::brpc_retry_backoff_ms;
    PreSendBackoffRetryPolicy policy;

    config::brpc_retry_backoff_ms = 2000;
    brpc::Controller host_down;
    host_down.SetFailed(EHOSTDOWN, "closed");
    EXPECT_EQ(2000, policy.GetBackoffTimeMs(&host_down));

    brpc::Controller conn_refused;
    conn_refused.SetFailed(ECONNREFUSED, "closed");
    EXPECT_EQ(2000, policy.GetBackoffTimeMs(&conn_refused));

    // Errors raised after the request may have been written are retried without any delay.
    for (const int error_code : {ECONNRESET, EPIPE, ENOENT, ETIMEDOUT}) {
        brpc::Controller cntl;
        cntl.SetFailed(error_code, "post-send");
        EXPECT_EQ(0, policy.GetBackoffTimeMs(&cntl)) << error_code;
    }
    brpc::Controller eolgoff;
    eolgoff.SetFailed(brpc::ELOGOFF, "post-send");
    EXPECT_EQ(0, policy.GetBackoffTimeMs(&eolgoff));

    EXPECT_TRUE(policy.CanRetryBackoffInPthread());

    config::brpc_retry_backoff_ms = 0;
    EXPECT_EQ(0, policy.GetBackoffTimeMs(&host_down));
    config::brpc_retry_backoff_ms = -1;
    EXPECT_EQ(0, policy.GetBackoffTimeMs(&host_down));

    config::brpc_retry_backoff_ms = saved_backoff_ms;
}

TEST_F(PInternalService_RecoverableStubTest, retries_with_backoff_before_send) {
    const int32_t saved_backoff_ms = config::brpc_retry_backoff_ms;
    config::brpc_retry_backoff_ms = 20;

    const int port = find_free_tcp_port();
    ASSERT_GT(port, 0);
    butil::EndPoint point;
    ASSERT_EQ(0, butil::str2endpoint("127.0.0.1", port, &point));

    auto stub = std::make_shared<PInternalService_RecoverableStub>(point);
    ASSERT_TRUE(stub->reset_channel().ok());

    ExecuteCommandRequestPB request;
    ExecuteCommandResultPB response;
    BlockingClosure closure;
    closure.cntl.set_timeout_ms(30000);

    const auto begin = std::chrono::steady_clock::now();
    stub->execute_command(&closure.cntl, &request, &response, &closure);
    closure.wait();
    const auto elapsed_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin).count();

    EXPECT_TRUE(closure.cntl.Failed());
    // max_retry = 3 means two backoffs before the RPC gives up.
    EXPECT_GE(elapsed_ms, 40);

    config::brpc_retry_backoff_ms = saved_backoff_ms;
}

TEST_F(PInternalService_RecoverableStubTest, backoff_lets_a_restarted_peer_succeed) {
    const int32_t saved_backoff_ms = config::brpc_retry_backoff_ms;
    // A single backoff must outlast one health check interval (3s), otherwise the retried attempt
    // still finds the socket marked failed.
    config::brpc_retry_backoff_ms = 5000;

    const int port = find_free_tcp_port();
    ASSERT_GT(port, 0);
    butil::EndPoint point;
    ASSERT_EQ(0, butil::str2endpoint("127.0.0.1", port, &point));

    auto stub = std::make_shared<PInternalService_RecoverableStub>(point);
    ASSERT_TRUE(stub->reset_channel().ok());

    brpc::Server server;
    FakeInternalService service;
    ASSERT_EQ(0, server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE));

    // The first attempt hits the closed port, the peer listens again before the retries run out.
    std::atomic<bool> started{false};
    std::thread starter([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        started = server.Start(point, nullptr) == 0;
    });

    ExecuteCommandRequestPB request;
    ExecuteCommandResultPB response;
    BlockingClosure closure;
    closure.cntl.set_timeout_ms(30000);
    stub->execute_command(&closure.cntl, &request, &response, &closure);
    closure.wait();

    starter.join();
    EXPECT_TRUE(started.load());
    EXPECT_FALSE(closure.cntl.Failed()) << closure.cntl.ErrorText();
    server.Stop(0);
    server.Join();

    config::brpc_retry_backoff_ms = saved_backoff_ms;
}
