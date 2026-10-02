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

#include "exec/pipeline/exchange/exchange_sink_operator.h"

#include <brpc/server.h>
#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "common/config.h"
#include "exec/pipeline/exchange/sink_buffer.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/query_context.h"
#include "gen_cpp/DataSinks_types.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/Partitions_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/data.pb.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "service/backend_options.h"
#include "testutil/assert.h"
#include "util/countdown_latch.h"
#include "util/defer_op.h"
#include "util/internal_service_recoverable_stub.h"

namespace starrocks::pipeline {

class ExchangeSinkOperatorTest : public ::testing::Test {
public:
    void SetUp() override {
        BackendOptions::set_localhost("0.0.0.0");

        _exec_env = ExecEnv::GetInstance();

        _query_context = std::make_shared<QueryContext>();
        _query_context->set_exec_env(_exec_env);
        _query_context->init_mem_tracker(-1, GlobalEnv::GetInstance()->process_mem_tracker());

        TQueryOptions query_options;
        query_options.__set_query_timeout(300);
        TQueryGlobals query_globals;
        _runtime_state = std::make_shared<RuntimeState>(_fragment_id, query_options, query_globals, _exec_env);
        _runtime_state->set_query_ctx(_query_context.get());
        _runtime_state->init_instance_mem_tracker();

        _fragment_context = std::make_shared<FragmentContext>();
        _fragment_context->set_fragment_instance_id(_fragment_id);
        _fragment_context->set_runtime_state(std::shared_ptr<RuntimeState>{_runtime_state});
        _runtime_state->set_fragment_ctx(_fragment_context.get());
    }

protected:
    TUniqueId _fragment_id;
    ExecEnv* _exec_env = nullptr;
    std::shared_ptr<QueryContext> _query_context;
    std::shared_ptr<RuntimeState> _runtime_state;
    std::shared_ptr<FragmentContext> _fragment_context;
};

class HangingInternalService : public PInternalService {
public:
    void transmit_chunk(google::protobuf::RpcController* /*controller*/, const PTransmitChunkParams* /*request*/,
                        PTransmitChunkResult* response, google::protobuf::Closure* done) override {
        _received.count_down();
        _release.wait();
        response->mutable_status()->set_status_code(0);
        done->Run();
    }

    CountDownLatch _received{1};
    CountDownLatch _release{1};
};

class SinkBufferCancelTest : public ExchangeSinkOperatorTest {
protected:
    std::shared_ptr<SinkBuffer> make_remote_sink_buffer(int port, const TUniqueId& dest_id) {
        TNetworkAddress addr;
        addr.__set_hostname("127.0.0.1");
        addr.__set_port(port);

        TPlanFragmentDestination dest;
        dest.__set_fragment_instance_id(dest_id);
        dest.__set_brpc_server(addr);
        return std::make_shared<SinkBuffer>(_fragment_context.get(), std::vector<TPlanFragmentDestination>{dest},
                                            false);
    }

    static TUniqueId make_dest_id(int64_t lo) {
        TUniqueId id;
        id.__set_hi(0);
        id.__set_lo(lo);
        return id;
    }

    static TransmitChunkInfo make_request(const TUniqueId& dest_id, int port,
                                          std::shared_ptr<PInternalService_RecoverableStub> stub) {
        TNetworkAddress addr;
        addr.__set_hostname("127.0.0.1");
        addr.__set_port(port);

        auto params = std::make_shared<PTransmitChunkParams>();
        params->set_eos(false);
        return TransmitChunkInfo{dest_id, std::move(stub), std::move(params), butil::IOBuf(), 0, addr};
    }
};

TEST_F(SinkBufferCancelTest, cancel_aborts_inflight_rpc) {
    brpc::Server server;
    HangingInternalService service;
    brpc::ServerOptions options;
    options.num_threads = 2;
    ASSERT_EQ(server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server.Start(0, &options), 0);
    DeferOp stop_server([&] {
        service._release.count_down();
        server.Stop(0);
        server.Join();
    });

    const auto dest_id = make_dest_id(987654321);
    auto buffer = make_remote_sink_buffer(server.listen_address().port, dest_id);
    buffer->incr_sinker(_runtime_state.get());

    auto stub = std::make_shared<PInternalService_RecoverableStub>(server.listen_address(), "");
    ASSERT_OK(stub->reset_channel());
    auto request = make_request(dest_id, server.listen_address().port, std::move(stub));
    ASSERT_OK(buffer->add_request(request));

    ASSERT_TRUE(service._received.wait_for(std::chrono::seconds(10)));
    EXPECT_FALSE(buffer->is_finished());

    const auto cancel_start = std::chrono::steady_clock::now();
    buffer->cancel_one_sinker(_runtime_state.get());

    const auto deadline = cancel_start + std::chrono::seconds(30);
    while (!buffer->is_finished() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(buffer->is_finished());
    EXPECT_LT(std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - cancel_start).count(),
              30);
}

TEST_F(SinkBufferCancelTest, cancel_with_no_inflight_rpc_is_safe) {
    auto buffer = make_remote_sink_buffer(1, make_dest_id(123456789));
    buffer->incr_sinker(_runtime_state.get());
    buffer->cancel_one_sinker(_runtime_state.get());
    EXPECT_TRUE(buffer->is_finished());
}

TEST_F(SinkBufferCancelTest, cancel_waits_for_all_sinkers) {
    auto buffer = make_remote_sink_buffer(1, make_dest_id(123456789));
    buffer->incr_sinker(_runtime_state.get());
    buffer->incr_sinker(_runtime_state.get());

    buffer->cancel_one_sinker(_runtime_state.get());
    EXPECT_FALSE(buffer->is_finished());

    buffer->cancel_one_sinker(_runtime_state.get());
    EXPECT_TRUE(buffer->is_finished());
}

} // namespace starrocks::pipeline
