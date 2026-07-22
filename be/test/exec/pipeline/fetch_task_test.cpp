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
#include <butil/iobuf.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <memory>
#include <ranges>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "base/utility/defer_op.h"
#include "common/config_exec_flow_fwd.h"
#include "exec/pipeline/lookup_request.h"
#define private public
#include "exec/pipeline/fetch_processor.h"
#undef private
#include "exec/exec_env.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/Status_types.h"
#include "gtest/gtest.h"
#include "platform/platform_env.h"
#include "runtime/runtime_state.h"
#include "storage_primitive/tablet_info.h"
#include "testutil/desc_tbl_builder.h"

namespace starrocks::pipeline {
namespace {

constexpr int32_t kSourceNodeId = 1;

int reserve_unused_local_port() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    EXPECT_NE(fd, -1);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    EXPECT_EQ(::bind(fd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)), 0);
    socklen_t len = sizeof(addr);
    EXPECT_EQ(::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len), 0);
    int port = ntohs(addr.sin_port);
    EXPECT_EQ(::close(fd), 0);
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
    auto processor = std::make_shared<FetchProcessor>(100, row_pos_descs, slot_descs, nodes_info);

    auto profile = std::make_shared<RuntimeProfile>("fetch_task_test");
    processor->_rpc_count =
            profile->add_counter("RpcCount", TUnit::UNIT, RuntimeProfile::Counter::create_strategy(TUnit::UNIT));
    processor->_network_timer = profile->add_counter("NetworkTime", TUnit::TIME_NS,
                                                     RuntimeProfile::Counter::create_strategy(TUnit::TIME_NS));
    return processor;
}

std::unique_ptr<RuntimeState> create_runtime_state(int query_timeout_s) {
    ExecEnv* exec_env = ExecEnv::GetInstance();
    TUniqueId fragment_instance_id;
    fragment_instance_id.hi = 1;
    fragment_instance_id.lo = 2;
    TQueryOptions query_options;
    query_options.__set_query_timeout(query_timeout_s);
    TQueryGlobals query_globals;
    return std::make_unique<RuntimeState>(fragment_instance_id, query_options, query_globals,
                                          &exec_env->query_execution_services(), exec_env);
}

std::unique_ptr<RuntimeState> create_runtime_state_with_descs(const std::vector<TypeDescriptor>& slot_types) {
    auto state = create_runtime_state(1);
    DescriptorTblBuilder builder(state.get(), state->obj_pool());
    auto& tuple_builder = builder.declare_tuple();
    for (const auto& slot_type : slot_types) {
        tuple_builder << slot_type;
    }
    state->set_desc_tbl(builder.build());
    return state;
}

phmap::flat_hash_map<TupleId, RowPositionDescriptor*> create_row_pos_descs(RowPositionDescriptor* row_pos_desc) {
    phmap::flat_hash_map<TupleId, RowPositionDescriptor*> row_pos_descs;
    row_pos_descs.emplace(0, row_pos_desc);
    return row_pos_descs;
}

class ScopedLookupHttpFallbackConfig {
public:
    explicit ScopedLookupHttpFallbackConfig(bool enabled) : _old(config::enable_glm_lookup_http_fallback) {
        config::enable_glm_lookup_http_fallback = enabled;
    }

    ~ScopedLookupHttpFallbackConfig() { config::enable_glm_lookup_http_fallback = _old; }

private:
    bool _old;
};

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

void append_size_to_iobuf(butil::IOBuf* output, size_t value) {
    output->append(&value, sizeof(value));
}

template <typename Message>
void append_http_frame_for_test(const Message& message, size_t attachment_size, std::string_view attachment,
                                butil::IOBuf* output) {
    const std::string protobuf = message.SerializeAsString();
    append_size_to_iobuf(output, protobuf.size());
    output->append(protobuf.data(), protobuf.size());
    append_size_to_iobuf(output, attachment_size);
    if (!attachment.empty()) {
        output->append(attachment.data(), attachment.size());
    }
}

} // namespace

TEST(LookUpHttpCodecTest, request_round_trip_preserves_column_attachment) {
    PLookUpRequest request;
    request.set_lookup_node_id(17);
    request.set_request_tuple_id(3);
    auto* column = request.add_request_columns();
    column->set_slot_id(5);
    column->set_data_size(3);

    const std::string payload = "abc";
    butil::IOBuf encoded;
    ASSERT_TRUE(LookUpHttpCodec::encode_request(request, payload.data(), payload.size(), &encoded).ok());

    PLookUpRequest decoded;
    ASSERT_TRUE(LookUpHttpCodec::decode_request(&encoded, &decoded).ok());

    EXPECT_TRUE(encoded.empty());
    EXPECT_EQ(17, decoded.lookup_node_id());
    EXPECT_EQ(3, decoded.request_tuple_id());
    ASSERT_EQ(1, decoded.request_columns_size());
    EXPECT_EQ(5, decoded.request_columns(0).slot_id());
    EXPECT_EQ(payload, decoded.request_columns(0).data());
}

TEST(LookUpHttpCodecTest, response_round_trip_leaves_column_attachment_for_deserialization) {
    PLookUpResponse response;
    response.mutable_status()->set_status_code(TStatusCode::OK);
    auto* column = response.add_columns();
    column->set_slot_id(7);
    column->set_data_size(3);

    const std::string payload = "xyz";
    butil::IOBuf attachment;
    attachment.append(payload.data(), payload.size());

    butil::IOBuf encoded;
    ASSERT_TRUE(LookUpHttpCodec::encode_response(response, &attachment, &encoded).ok());

    PLookUpResponse decoded;
    ASSERT_TRUE(LookUpHttpCodec::decode_response(&encoded, &decoded).ok());

    EXPECT_EQ(TStatusCode::OK, decoded.status().status_code());
    ASSERT_EQ(1, decoded.columns_size());
    EXPECT_EQ(7, decoded.columns(0).slot_id());
    EXPECT_EQ(3, decoded.columns(0).data_size());
    ASSERT_EQ(payload.size(), encoded.size());

    std::string decoded_payload(payload.size(), char{0});
    ASSERT_EQ(payload.size(), encoded.cutn(decoded_payload.data(), decoded_payload.size()));
    EXPECT_EQ(payload, decoded_payload);
}

TEST(LookUpHttpCodecTest, decode_request_columns_rejects_short_attachment) {
    PLookUpRequest request;
    auto* column = request.add_request_columns();
    column->set_slot_id(1);
    column->set_data_size(3);

    const std::string payload = "a";
    butil::IOBuf input;
    input.append(payload.data(), payload.size());

    auto st = decode_lookup_request_columns_from_iobuf(&input, &request);
    EXPECT_FALSE(st.ok());
}

TEST(LookUpHttpCodecTest, decode_request_rejects_malformed_frames) {
    {
        butil::IOBuf input;
        PLookUpRequest request;
        EXPECT_FALSE(LookUpHttpCodec::decode_request(&input, &request).ok());
    }
    {
        butil::IOBuf input;
        append_size_to_iobuf(&input, 3);
        const std::string payload = "a";
        input.append(payload.data(), payload.size());

        PLookUpRequest request;
        EXPECT_FALSE(LookUpHttpCodec::decode_request(&input, &request).ok());
    }
    {
        butil::IOBuf input;
        append_size_to_iobuf(&input, 1);
        const char invalid_protobuf = static_cast<char>(0);
        input.append(&invalid_protobuf, 1);
        append_size_to_iobuf(&input, 0);

        PLookUpRequest request;
        EXPECT_FALSE(LookUpHttpCodec::decode_request(&input, &request).ok());
    }
    {
        PLookUpRequest encoded_request;
        const std::string protobuf = encoded_request.SerializeAsString();
        butil::IOBuf input;
        append_size_to_iobuf(&input, protobuf.size());
        input.append(protobuf.data(), protobuf.size());

        PLookUpRequest request;
        EXPECT_FALSE(LookUpHttpCodec::decode_request(&input, &request).ok());
    }
    {
        PLookUpRequest encoded_request;
        butil::IOBuf input;
        append_http_frame_for_test(encoded_request, 1, "", &input);

        PLookUpRequest request;
        EXPECT_FALSE(LookUpHttpCodec::decode_request(&input, &request).ok());
    }
    {
        PLookUpRequest encoded_request;
        auto* column = encoded_request.add_request_columns();
        column->set_slot_id(3);
        column->set_data_size(1);

        butil::IOBuf input;
        append_http_frame_for_test(encoded_request, 2, "ab", &input);

        PLookUpRequest request;
        EXPECT_FALSE(LookUpHttpCodec::decode_request(&input, &request).ok());
    }
}

TEST(LookUpHttpCodecTest, decode_response_rejects_malformed_frames) {
    {
        butil::IOBuf input;
        PLookUpResponse response;
        EXPECT_FALSE(LookUpHttpCodec::decode_response(&input, &response).ok());
    }
    {
        butil::IOBuf input;
        append_size_to_iobuf(&input, 3);
        const std::string payload = "a";
        input.append(payload.data(), payload.size());

        PLookUpResponse response;
        EXPECT_FALSE(LookUpHttpCodec::decode_response(&input, &response).ok());
    }
    {
        butil::IOBuf input;
        append_size_to_iobuf(&input, 1);
        const char invalid_protobuf = static_cast<char>(0);
        input.append(&invalid_protobuf, 1);
        append_size_to_iobuf(&input, 0);

        PLookUpResponse response;
        EXPECT_FALSE(LookUpHttpCodec::decode_response(&input, &response).ok());
    }
    {
        PLookUpResponse encoded_response;
        const std::string protobuf = encoded_response.SerializeAsString();
        butil::IOBuf input;
        append_size_to_iobuf(&input, protobuf.size());
        input.append(protobuf.data(), protobuf.size());

        PLookUpResponse response;
        EXPECT_FALSE(LookUpHttpCodec::decode_response(&input, &response).ok());
    }
    {
        PLookUpResponse encoded_response;
        butil::IOBuf input;
        append_http_frame_for_test(encoded_response, 1, "", &input);

        PLookUpResponse response;
        EXPECT_FALSE(LookUpHttpCodec::decode_response(&input, &response).ok());
    }
}

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

TEST(FetchTaskTest, submit_remote_rpc_failure_marks_done_and_updates_status) {
    ASSERT_NE(ExecEnv::GetInstance(), nullptr);
    ASSERT_NE(PlatformEnv::GetInstance()->brpc_stub_cache(), nullptr);

    const int unused_port = reserve_unused_local_port();
    auto processor = create_fetch_processor(create_nodes_info(unused_port));
    auto unit = std::make_shared<BatchUnit>();
    unit->total_request_num = 1;
    auto ctx = std::make_shared<FetchTaskContext>();
    ctx->processor = processor;
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
    ASSERT_NE(PlatformEnv::GetInstance()->brpc_stub_cache(), nullptr);

    const int unused_port = reserve_unused_local_port();
    auto processor = create_fetch_processor(create_nodes_info(unused_port));
    auto ctx = std::make_shared<FetchTaskContext>();
    ctx->processor = processor;
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

TEST(FetchTaskTest, lookup_http_fallback_returns_false_without_row_position_descriptor) {
    ScopedLookupHttpFallbackConfig config_guard(true);
    auto state = create_runtime_state(1);
    phmap::flat_hash_map<TupleId, RowPositionDescriptor*> row_pos_descs;

    EXPECT_FALSE(FetchTask::should_use_lookup_http_rpc_for_test(state.get(), 0, row_pos_descs));
}

TEST(FetchTaskTest, lookup_http_fallback_disabled_by_config) {
    ScopedLookupHttpFallbackConfig config_guard(false);
    auto state =
            create_runtime_state_with_descs({TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH)});
    RowPositionDescriptor row_pos_desc(RowPositionDescriptor::OLAP_SCAN, 1, -1, {}, {});
    auto row_pos_descs = create_row_pos_descs(&row_pos_desc);

    EXPECT_FALSE(FetchTask::should_use_lookup_http_rpc_for_test(state.get(), 0, row_pos_descs));
}

TEST(FetchTaskTest, lookup_http_fallback_enabled_for_max_varchar_fetch_slot) {
    ScopedLookupHttpFallbackConfig config_guard(true);
    auto state = create_runtime_state_with_descs(
            {TYPE_INT_DESC, TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH)});
    RowPositionDescriptor row_pos_desc(RowPositionDescriptor::OLAP_SCAN, 1, -1, {}, {0});
    auto row_pos_descs = create_row_pos_descs(&row_pos_desc);

    EXPECT_TRUE(FetchTask::should_use_lookup_http_rpc_for_test(state.get(), 0, row_pos_descs));
}

TEST(FetchTaskTest, lookup_http_fallback_rejects_small_varchar_fetch_slot) {
    ScopedLookupHttpFallbackConfig config_guard(true);
    auto state = create_runtime_state_with_descs(
            {TypeDescriptor::create_varchar_type(TypeDescriptor::LARGE_VARCHAR_LENGTH_THRESHOLD - 1)});
    RowPositionDescriptor row_pos_desc(RowPositionDescriptor::OLAP_SCAN, 1, -1, {}, {});
    auto row_pos_descs = create_row_pos_descs(&row_pos_desc);

    EXPECT_FALSE(FetchTask::should_use_lookup_http_rpc_for_test(state.get(), 0, row_pos_descs));
}

TEST(FetchTaskTest, lookup_http_fallback_skips_lookup_ref_slots) {
    ScopedLookupHttpFallbackConfig config_guard(true);
    auto state = create_runtime_state_with_descs(
            {TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH), TYPE_INT_DESC});
    RowPositionDescriptor row_pos_desc(RowPositionDescriptor::OLAP_SCAN, 1, -1, {}, {0});
    auto row_pos_descs = create_row_pos_descs(&row_pos_desc);

    EXPECT_FALSE(FetchTask::should_use_lookup_http_rpc_for_test(state.get(), 0, row_pos_descs));
}

} // namespace starrocks::pipeline
