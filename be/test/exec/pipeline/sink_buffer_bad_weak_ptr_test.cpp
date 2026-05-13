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

#include <gtest/gtest.h>

#include <thread>

#include "base/brpc/disposable_closure.h"
#include "base/testutil/assert.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "common/system/backend_options.h"
#include "exec/pipeline/exchange/sink_buffer.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/query_context.h"
#include "gen_cpp/data.pb.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

using SyncPointArg = std::pair<bool, DisposableClosure<PTransmitChunkResult, ClosureContext>*>;

class SinkBufferBadWeakPtrTest : public ::testing::Test {
public:
    void SetUp() override {
        BackendOptions::set_localhost("0.0.0.0");

        _exec_env = ExecEnv::GetInstance();

        _query_context = std::make_shared<QueryContext>();
        _query_context->set_query_execution_services(&_exec_env->query_execution_services());
        _query_context->init_mem_tracker(-1, GlobalEnv::GetInstance()->process_mem_tracker());

        TQueryOptions query_options;
        query_options.__set_query_timeout(60);
        TQueryGlobals query_globals;
        _runtime_state = std::make_shared<RuntimeState>(_fragment_id, query_options, query_globals,
                                                        &_exec_env->query_execution_services(), _exec_env);
        _runtime_state->set_query_ctx(_query_context.get());
        _runtime_state->init_instance_mem_tracker();

        _fragment_context = std::make_shared<FragmentContext>();
        _fragment_context->set_fragment_instance_id(_fragment_id);
        _fragment_context->set_runtime_state(std::shared_ptr{_runtime_state});
        _runtime_state->set_fragment_ctx(_fragment_context.get());

        TNetworkAddress address;
        address.__set_hostname("127.0.0.1");
        address.__set_port(1);
        TUniqueId dest_fragment_id;
        dest_fragment_id.__set_lo(1);
        dest_fragment_id.__set_hi(0);
        _destination.__set_fragment_instance_id(dest_fragment_id);
        _destination.__set_brpc_server(address);

        _destinations = {_destination};
    }

    void TearDown() override {
        if (_query_context) {
            _query_context->set_query_execution_services(nullptr);
        }
    }

    TransmitChunkInfo build_request() {
        TUniqueId dest_id;
        dest_id.__set_lo(1);
        dest_id.__set_hi(0);
        auto params = std::make_shared<PTransmitChunkParams>();
        params->set_eos(false);
        TNetworkAddress brpc_addr;
        brpc_addr.__set_hostname("127.0.0.1");
        brpc_addr.__set_port(1);
        return TransmitChunkInfo{dest_id, nullptr, std::move(params), butil::IOBuf(), 64, brpc_addr};
    }

protected:
    TUniqueId _fragment_id;
    ExecEnv* _exec_env = nullptr;
    std::shared_ptr<QueryContext> _query_context;
    std::shared_ptr<RuntimeState> _runtime_state;
    std::shared_ptr<FragmentContext> _fragment_context;
    std::vector<TPlanFragmentDestination> _destinations;
    TPlanFragmentDestination _destination;
};

TEST_F(SinkBufferBadWeakPtrTest, QueryContextWeakPtrExpiredAfterDestroy) {
    auto query_ctx = _runtime_state->query_ctx();
    auto wp = query_ctx->weak_from_this();
    ASSERT_FALSE(wp.expired());
    _query_context.reset();
    ASSERT_TRUE(wp.expired());
    ASSERT_EQ(wp.lock(), nullptr);
}

TEST_F(SinkBufferBadWeakPtrTest, SharedFromThisThrowsAfterDestroy) {
    auto query_ctx_raw = _query_context.get();
    auto wp = query_ctx_raw->weak_from_this();
    _query_context.reset();
    ASSERT_TRUE(wp.expired());
    EXPECT_THROW(query_ctx_raw->shared_from_this(), std::bad_weak_ptr);
}

TEST_F(SinkBufferBadWeakPtrTest, ConcurrentWeakLockDuringDestroy) {
    auto wp = _runtime_state->query_ctx()->weak_from_this();
    std::atomic<bool> start{false};
    std::atomic<int> null_count{0};
    std::atomic<int> valid_count{0};

    std::vector<std::thread> threads;
    for (int i = 0; i < 8; i++) {
        threads.emplace_back([&]() {
            while (!start.load()) {
            }
            for (int j = 0; j < 1000; j++) {
                auto locked = wp.lock();
                if (locked) {
                    valid_count++;
                } else {
                    null_count++;
                }
            }
        });
    }

    start = true;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    _query_context.reset();

    for (auto& t : threads) {
        t.join();
    }

    ASSERT_GT(null_count + valid_count, 0);
    ASSERT_TRUE(wp.expired());
}

TEST_F(SinkBufferBadWeakPtrTest, FailedCallbackWithValidQueryContext) {
    auto sink_buffer = std::make_unique<SinkBuffer>(_fragment_context.get(), _destinations, false);
    sink_buffer->incr_sinker(_runtime_state.get());

    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("SinkBuffer::_try_to_send_rpc::before_send_rpc");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    SyncPoint::GetInstance()->SetCallBack("SinkBuffer::_try_to_send_rpc::before_send_rpc", [&](void* arg) {
        auto* sync_arg = static_cast<SyncPointArg*>(arg);
        auto* closure = sync_arg->second;
        closure->cntl.SetFailed(ECONNREFUSED, "test connection refused");
        closure->Run();
        sync_arg->first = true;
    });

    auto request = build_request();
    ASSERT_OK(sink_buffer->add_request(request));

    ASSERT_TRUE(sink_buffer->_is_finishing);

    sink_buffer->cancel_one_sinker(_runtime_state.get());
}

TEST_F(SinkBufferBadWeakPtrTest, FailedCallbackWithExpiredQueryContext) {
    auto sink_buffer = std::make_unique<SinkBuffer>(_fragment_context.get(), _destinations, false);
    sink_buffer->incr_sinker(_runtime_state.get());

    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("SinkBuffer::_try_to_send_rpc::before_send_rpc");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    SyncPoint::GetInstance()->SetCallBack("SinkBuffer::_try_to_send_rpc::before_send_rpc", [&](void* arg) {
        auto* sync_arg = static_cast<SyncPointArg*>(arg);
        auto* closure = sync_arg->second;
        _query_context.reset();
        closure->cntl.SetFailed(ECONNREFUSED, "test connection refused");
        closure->Run();
        sync_arg->first = true;
    });

    auto request = build_request();
    ASSERT_OK(sink_buffer->add_request(request));

    ASSERT_TRUE(sink_buffer->_is_finishing);
    ASSERT_EQ(sink_buffer->_total_in_flight_rpc, 0);

    sink_buffer->cancel_one_sinker(nullptr);
}

TEST_F(SinkBufferBadWeakPtrTest, SuccessCallbackWithValidQueryContext) {
    auto sink_buffer = std::make_unique<SinkBuffer>(_fragment_context.get(), _destinations, false);
    sink_buffer->incr_sinker(_runtime_state.get());

    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("SinkBuffer::_try_to_send_rpc::before_send_rpc");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    SyncPoint::GetInstance()->SetCallBack("SinkBuffer::_try_to_send_rpc::before_send_rpc", [&](void* arg) {
        auto* sync_arg = static_cast<SyncPointArg*>(arg);
        auto* closure = sync_arg->second;
        closure->result.mutable_status()->set_status_code(0);
        closure->Run();
        sync_arg->first = true;
    });

    auto request = build_request();
    ASSERT_OK(sink_buffer->add_request(request));

    ASSERT_EQ(sink_buffer->_total_in_flight_rpc, 0);

    sink_buffer->cancel_one_sinker(_runtime_state.get());
}

TEST_F(SinkBufferBadWeakPtrTest, SuccessCallbackWithExpiredQueryContext) {
    auto sink_buffer = std::make_unique<SinkBuffer>(_fragment_context.get(), _destinations, false);
    sink_buffer->incr_sinker(_runtime_state.get());

    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("SinkBuffer::_try_to_send_rpc::before_send_rpc");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    SyncPoint::GetInstance()->SetCallBack("SinkBuffer::_try_to_send_rpc::before_send_rpc", [&](void* arg) {
        auto* sync_arg = static_cast<SyncPointArg*>(arg);
        auto* closure = sync_arg->second;
        _query_context.reset();
        closure->result.mutable_status()->set_status_code(0);
        closure->Run();
        sync_arg->first = true;
    });

    auto request = build_request();
    ASSERT_OK(sink_buffer->add_request(request));

    ASSERT_TRUE(sink_buffer->_is_finishing);
    ASSERT_EQ(sink_buffer->_total_in_flight_rpc, 0);

    sink_buffer->cancel_one_sinker(nullptr);
}

TEST_F(SinkBufferBadWeakPtrTest, SinkBufferFinishesWhenCancelled) {
    auto sink_buffer = std::make_unique<SinkBuffer>(_fragment_context.get(), _destinations, false);
    sink_buffer->incr_sinker(_runtime_state.get());
    ASSERT_FALSE(sink_buffer->is_finished());

    sink_buffer->cancel_one_sinker(_runtime_state.get());
    ASSERT_TRUE(sink_buffer->is_finished());
}

} // namespace starrocks::pipeline
