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

#include "exec/pipeline/fetch_task.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#define private public
#include "exec/pipeline/fetch_processor.h"
#undef private
#include "exec/pipeline/lookup_request.h"
#include "exec/tablet_info.h"
#include "gen_cpp/Descriptors_types.h"
#include "gtest/gtest.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
namespace {

constexpr int32_t kSourceNodeId = 1;

int reserve_unused_local_port() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    EXPECT_NE(fd, -1);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    EXPECT_EQ(bind(fd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)), 0);
    socklen_t len = sizeof(addr);
    EXPECT_EQ(getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len), 0);
    int port = ntohs(addr.sin_port);
    EXPECT_EQ(close(fd), 0);
    return port;
}

std::shared_ptr<StarRocksNodesInfo> create_nodes_info(int port) {
    TNodesInfo t_nodes;
    TNodeInfo node;
    node.__set_id(kSourceNodeId);
    node.__set_option(0);
    node.__set_host("127.0.0.1");
    node.__set_async_internal_port(port);
    t_nodes.nodes.emplace_back(std::move(node));
    return std::make_shared<StarRocksNodesInfo>(t_nodes);
}

std::shared_ptr<FetchProcessor> create_fetch_processor(const std::shared_ptr<StarRocksNodesInfo>& nodes_info) {
    phmap::flat_hash_map<TupleId, RowPositionDescriptor*> row_pos_descs;
    phmap::flat_hash_map<SlotId, SlotDescriptor*> slot_descs;
    auto processor = std::make_shared<FetchProcessor>(100, row_pos_descs, slot_descs, nodes_info, nullptr);

    auto profile = std::make_shared<RuntimeProfile>("fetch_task_test");
    processor->_rpc_count =
            profile->add_counter("RpcCount", TUnit::UNIT, RuntimeProfile::Counter::create_strategy(TUnit::UNIT));
    processor->_network_timer = profile->add_counter("NetworkTime", TUnit::TIME_NS,
                                                     RuntimeProfile::Counter::create_strategy(TUnit::TIME_NS));
    return processor;
}

std::unique_ptr<RuntimeState> create_runtime_state(int query_timeout_s) {
    TUniqueId fragment_instance_id;
    fragment_instance_id.hi = 1;
    fragment_instance_id.lo = 2;
    TQueryOptions query_options;
    query_options.__set_query_timeout(query_timeout_s);
    TQueryGlobals query_globals;
    return std::make_unique<RuntimeState>(fragment_instance_id, query_options, query_globals, ExecEnv::GetInstance());
}

bool wait_task_done(const FetchTaskPtr& task, int timeout_ms) {
    constexpr int kCheckIntervalMs = 10;
    int elapsed = 0;
    while (elapsed < timeout_ms) {
        if (task->is_done()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(kCheckIntervalMs));
        elapsed += kCheckIntervalMs;
    }
    return task->is_done();
}

} // namespace

// Verify the shared_ptr cycle between BatchUnit and FetchTaskContext is broken
// after changing FetchTaskContext::unit to weak_ptr.
TEST(FetchTaskTest, batch_unit_released_after_outer_refs_drop) {
    std::weak_ptr<BatchUnit> weak_unit;

    {
        auto unit = std::make_shared<BatchUnit>();
        weak_unit = unit;

        auto ctx = std::make_shared<FetchTaskContext>();
        ctx->unit = unit;

        auto task = std::make_shared<FetchTask>(ctx);
        auto tasks = std::make_shared<std::vector<FetchTaskPtr>>();
        tasks->emplace_back(task);
        unit->fetch_tasks.emplace(1, tasks);

        // Drop all external references; weak_ptr in ctx should not prevent release.
        task.reset();
        tasks.reset();
        ctx.reset();
        unit.reset();
    }

    EXPECT_TRUE(weak_unit.expired());
}

// Simulate the RPC closure holding FetchTask alive via shared_from_this()
// even after the owning BatchUnit drops its reference.
TEST(FetchTaskTest, shared_from_this_keeps_task_alive_after_batch_drops) {
    std::weak_ptr<FetchTask> weak_task;

    auto unit = std::make_shared<BatchUnit>();
    auto ctx = std::make_shared<FetchTaskContext>();
    ctx->unit = unit;

    auto task = std::make_shared<FetchTask>(ctx);
    weak_task = task;

    // Simulate what the RPC closure does: hold a shared_ptr copy.
    auto closure_hold = task->shared_from_this();

    // Store task in BatchUnit, then release all external refs except closure_hold.
    auto tasks = std::make_shared<std::vector<FetchTaskPtr>>();
    tasks->emplace_back(std::move(task));
    unit->fetch_tasks.emplace(1, tasks);

    tasks.reset();
    unit.reset();
    ctx.reset();

    // FetchTask should still be alive because closure_hold keeps it.
    EXPECT_FALSE(weak_task.expired());
    EXPECT_FALSE(closure_hold->is_done());

    // After closure finishes and releases, FetchTask is destroyed.
    closure_hold.reset();
    EXPECT_TRUE(weak_task.expired());
}

// LocalLookUpRequestContext::callback should safely increment finished_request_num
// when the BatchUnit is still alive.
TEST(FetchTaskTest, local_callback_increments_counter_when_unit_alive) {
    auto unit = std::make_shared<BatchUnit>();
    unit->total_request_num = 2;
    unit->finished_request_num = 0;

    auto ctx = std::make_shared<FetchTaskContext>();
    ctx->unit = unit;

    LocalLookUpRequestContext local_ctx(ctx);
    local_ctx.callback(Status::OK());

    EXPECT_EQ(unit->finished_request_num.load(), 1);

    local_ctx.callback(Status::OK());
    EXPECT_EQ(unit->finished_request_num.load(), 2);
}

// LocalLookUpRequestContext::callback should not crash when the BatchUnit
// has already been released (weak_ptr expired).
TEST(FetchTaskTest, local_callback_safe_when_unit_expired) {
    auto ctx = std::make_shared<FetchTaskContext>();

    {
        auto unit = std::make_shared<BatchUnit>();
        unit->total_request_num = 1;
        ctx->unit = unit;
        // unit goes out of scope here, weak_ptr expires.
    }

    LocalLookUpRequestContext local_ctx(ctx);
    // Should not crash or modify anything.
    EXPECT_NO_FATAL_FAILURE(local_ctx.callback(Status::OK()));
}

TEST(FetchTaskTest, submit_remote_rpc_failure_marks_done_and_updates_status) {
    ASSERT_NE(ExecEnv::GetInstance(), nullptr);
    ASSERT_NE(ExecEnv::GetInstance()->brpc_stub_cache(), nullptr);

    const int unused_port = reserve_unused_local_port();
    auto processor = create_fetch_processor(create_nodes_info(unused_port));
    auto unit = std::make_shared<BatchUnit>();
    unit->total_request_num = 1;
    auto ctx = std::make_shared<FetchTaskContext>();
    ctx->processor = processor.get();
    ctx->unit = unit;
    ctx->source_node_id = kSourceNodeId;
    ctx->request_tuple_id = 10;
    ctx->request_chunk = std::make_shared<Chunk>();

    auto task = std::make_shared<FetchTask>(ctx);
    auto state = create_runtime_state(1);
    ASSERT_TRUE(task->submit(state.get()).ok());
    ASSERT_TRUE(wait_task_done(task, 5000));

    EXPECT_TRUE(task->is_done());
    EXPECT_EQ(unit->finished_request_num.load(), 1);
    EXPECT_FALSE(processor->_io_task_status.ok());
}

TEST(FetchTaskTest, submit_remote_rpc_failure_handles_expired_unit) {
    ASSERT_NE(ExecEnv::GetInstance(), nullptr);
    ASSERT_NE(ExecEnv::GetInstance()->brpc_stub_cache(), nullptr);

    const int unused_port = reserve_unused_local_port();
    auto processor = create_fetch_processor(create_nodes_info(unused_port));
    auto ctx = std::make_shared<FetchTaskContext>();
    ctx->processor = processor.get();
    {
        auto unit = std::make_shared<BatchUnit>();
        ctx->unit = unit;
    }
    ctx->source_node_id = kSourceNodeId;
    ctx->request_tuple_id = 11;
    ctx->request_chunk = std::make_shared<Chunk>();

    auto task = std::make_shared<FetchTask>(ctx);
    auto state = create_runtime_state(1);
    ASSERT_TRUE(task->submit(state.get()).ok());
    ASSERT_TRUE(wait_task_done(task, 5000));

    EXPECT_TRUE(task->is_done());
}

} // namespace starrocks::pipeline
