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

#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "exec/exec_node.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/fragment_executor.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/plan_fragment_executor.h"
#include "runtime/runtime_state.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "testutil/assert.h"
#include "util/defer_op.h"
#include "util/uid_util.h"

namespace starrocks {
namespace {

using namespace std::chrono_literals;

enum class ExecutionMode { NON_PIPELINE, PIPELINE_NODE_RANGES, PIPELINE_DRIVER_RANGES };

// Stand in for a plan whose close must wait for its scanner. The callback has a
// bounded wait, so a regression fails instead of hanging the test process.
class CloseProbeNode final : public ExecNode {
public:
    CloseProbeNode(ObjectPool* pool, const DescriptorTbl& desc, std::function<void()> on_close)
            : ExecNode(pool, make_node(), desc), _on_close(std::move(on_close)) {}

    void close(RuntimeState*) override { _on_close(); }

private:
    static TPlanNode make_node() {
        TPlanNode node;
        node.__set_node_id(0);
        node.__set_node_type(TPlanNodeType::EMPTY_SET_NODE);
        node.__set_limit(-1);
        return node;
    }

    std::function<void()> _on_close;
};

class StreamLoadCancelTest : public testing::TestWithParam<ExecutionMode> {
protected:
    void SetUp() override {
        const auto fragment_id = UniqueId::gen_uid().to_thrift();
        TQueryOptions options;
        options.__set_query_type(TQueryType::LOAD);
        options.__set_load_job_type(TLoadJobType::STREAM_LOAD);
        _state = std::make_shared<RuntimeState>(fragment_id, options, TQueryGlobals{}, _exec_env);

        _params.__set_backend_num(0);
        _params.__set_pipeline_dop(2);
        _params.__set_query_options(options);
        TPlanFragmentExecParams exec_params;
        exec_params.__set_fragment_instance_id(fragment_id);
        exec_params.__set_sender_id(0);
        _params.__set_params(exec_params);

        if (is_pipeline()) {
            _pipeline = std::make_unique<pipeline::FragmentExecutor>();
            _pipeline->_fragment_ctx = std::make_shared<pipeline::FragmentContext>();
            _pipeline->_fragment_ctx->set_fragment_instance_id(fragment_id);
            _pipeline->_fragment_ctx->set_runtime_state(std::shared_ptr<RuntimeState>(_state));
            _state->set_fragment_ctx(_pipeline->_fragment_ctx.get());
        } else {
            _legacy = std::make_unique<PlanFragmentExecutor>(_exec_env, nullptr);
            _legacy->set_runtime_state(_state.get());
            _legacy->_prepared = true;
        }
    }

    void TearDown() override {
        // Executors hold non-owning references to RuntimeState and service objects.
        _legacy.reset();
        _pipeline.reset();
        _state.reset();
        for (const auto& id : _load_ids) {
            _exec_env->load_stream_mgr()->remove(id);
        }
    }

    bool is_pipeline() const { return GetParam() != ExecutionMode::NON_PIPELINE; }

    static TScanRangeParams make_range(const TUniqueId& id, TFileType::type type = TFileType::FILE_STREAM) {
        TBrokerRangeDesc range;
        range.__set_file_type(type);
        range.__set_format_type(TFileFormatType::FORMAT_CSV_PLAIN);
        range.__set_load_id(id);
        TBrokerScanRange broker;
        broker.ranges.emplace_back(std::move(range));
        TScanRange scan;
        scan.__set_broker_scan_range(broker);
        TScanRangeParams params;
        params.__set_scan_range(scan);
        return params;
    }

    std::shared_ptr<StreamLoadPipe> add_pipe() {
        auto id = UniqueId::gen_uid().to_thrift();
        auto pipe = std::make_shared<StreamLoadPipe>(4, 4);
        EXPECT_OK(_exec_env->load_stream_mgr()->put(id, pipe));
        _ranges.emplace_back(make_range(id));
        _load_ids.emplace_back(id);
        return pipe;
    }

    Status prepare_pipes(int32_t node_id = 0) {
        if (GetParam() == ExecutionMode::PIPELINE_DRIVER_RANGES) {
            _params.params.__isset.node_to_per_driver_seq_scan_ranges = true;
            for (size_t i = 0; i < _ranges.size(); ++i) {
                _params.params.node_to_per_driver_seq_scan_ranges[node_id][static_cast<int32_t>(i % 2)].push_back(
                        _ranges[i]);
            }
        } else {
            _params.params.per_node_scan_ranges[node_id] = _ranges;
        }
        if (is_pipeline()) {
            pipeline::UnifiedExecPlanFragmentParams request(_params, _params);
            return _pipeline->_prepare_stream_load_pipe(_exec_env, request);
        }
        return _legacy->_prepare_stream_load_pipe(_params);
    }

    void cancel() {
        if (is_pipeline()) {
            _pipeline->_fragment_ctx->cancel(Status::Cancelled("Fragment cancelled"));
        } else {
            _legacy->cancel();
        }
    }

    ByteBufferPtr make_buffer(std::string_view data) {
        ASSIGN_OR_ABORT(auto buffer, ByteBuffer::allocate_with_tracker(data.size()));
        buffer->put_bytes(data.data(), data.size());
        buffer->flip_to_read();
        return buffer;
    }

    void check_blocked_reader(bool byte_read) {
        auto pipe = add_pipe();
        ASSERT_OK(prepare_pipes());
        std::promise<void> started;
        auto entered = started.get_future();
        std::promise<Status> result;
        auto done = result.get_future();
        std::thread reader([&] {
            started.set_value();
            if (byte_read) {
                uint8_t byte;
                size_t size = 1;
                bool eof = false;
                result.set_value(pipe->read(&byte, &size, &eof));
            } else {
                result.set_value(pipe->read().status());
            }
        });
        DeferOp cleanup([&] {
            pipe->cancel(Status::Cancelled("test cleanup"));
            reader.join();
        });
        ASSERT_EQ(std::future_status::ready, entered.wait_for(5s));
        ASSERT_EQ(std::future_status::timeout, done.wait_for(20ms));

        cancel();

        ASSERT_EQ(std::future_status::ready, done.wait_for(5s));
        const auto status = done.get();
        EXPECT_TRUE(status.is_cancelled()) << status;
        EXPECT_EQ("Fragment cancelled", status.message());
    }

    ExecEnv* _exec_env = ExecEnv::GetInstance();
    std::shared_ptr<RuntimeState> _state;
    std::unique_ptr<PlanFragmentExecutor> _legacy;
    std::unique_ptr<pipeline::FragmentExecutor> _pipeline;
    TExecPlanFragmentParams _params;
    std::vector<TScanRangeParams> _ranges;
    std::vector<TUniqueId> _load_ids;
};

TEST_P(StreamLoadCancelTest, CancelWakesBlockedBufferReader) {
    check_blocked_reader(false);
}

TEST_P(StreamLoadCancelTest, CancelWakesBlockedByteReader) {
    check_blocked_reader(true);
}

TEST_P(StreamLoadCancelTest, CancelWakesBlockedWriter) {
    auto pipe = add_pipe();
    ASSERT_OK(prepare_pipes());
    ASSERT_OK(pipe->append(make_buffer("full")));
    auto next = make_buffer("x");
    std::promise<Status> result;
    auto done = result.get_future();
    std::thread writer([&] { result.set_value(pipe->append(std::move(next))); });
    DeferOp cleanup([&] {
        pipe->cancel(Status::Cancelled("test cleanup"));
        writer.join();
    });
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (pipe->num_waiting_append_buffer() == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(1ms);
    }
    ASSERT_EQ(1, pipe->num_waiting_append_buffer());

    cancel();

    ASSERT_EQ(std::future_status::ready, done.wait_for(5s));
    EXPECT_TRUE(done.get().is_cancelled());
}

TEST_P(StreamLoadCancelTest, BindingSurvivesRegistryRemoval) {
    auto pipe = add_pipe();
    ASSERT_OK(prepare_pipes());
    std::weak_ptr<StreamLoadPipe> weak_pipe = pipe;
    _exec_env->load_stream_mgr()->remove(_load_ids.front());
    pipe.reset();
    ASSERT_FALSE(weak_pipe.expired());

    // Exercise cancellation before the first read, using the reference held by the executor.
    cancel();
    pipe = weak_pipe.lock();
    ASSERT_NE(nullptr, pipe);
    EXPECT_TRUE(pipe->no_block_read().status().is_cancelled());
}

TEST_P(StreamLoadCancelTest, BindsAllPipesAndIgnoresLocalFiles) {
    auto first = add_pipe();
    auto second = add_pipe();
    // FILE_LOCAL has no registered pipe and must not make preparation fail.
    _ranges.emplace_back(make_range(UniqueId::gen_uid().to_thrift(), TFileType::FILE_LOCAL));
    ASSERT_OK(prepare_pipes());

    cancel();

    EXPECT_TRUE(first->no_block_read().status().is_cancelled());
    EXPECT_TRUE(second->no_block_read().status().is_cancelled());
}

TEST_P(StreamLoadCancelTest, BindsPipesAfterEmptyNode) {
    _params.params.per_node_scan_ranges[0] = {};
    auto pipe = add_pipe();
    ASSERT_OK(prepare_pipes(1));
    cancel();
    EXPECT_TRUE(pipe->no_block_read().status().is_cancelled());
}

TEST_P(StreamLoadCancelTest, BindsPipesAfterNonBrokerNode) {
    _params.params.per_node_scan_ranges[0] = {TScanRangeParams{}};
    auto pipe = add_pipe();
    ASSERT_OK(prepare_pipes(1));
    cancel();
    EXPECT_TRUE(pipe->no_block_read().status().is_cancelled());
}

TEST_P(StreamLoadCancelTest, LegacyRejectsChannelBeforeBindingPipes) {
    if (is_pipeline()) {
        GTEST_SKIP() << "The legacy engine does not support channel inputs";
    }
    auto channel = make_range(UniqueId::gen_uid().to_thrift());
    channel.scan_range.broker_scan_range.__set_channel_id(0);
    _ranges.emplace_back(std::move(channel));
    add_pipe();
    EXPECT_TRUE(prepare_pipes().is_not_supported());
    EXPECT_TRUE(_legacy->_stream_load_sinks.empty());
}

TEST_P(StreamLoadCancelTest, ChannelOnlyUsesChannelContext) {
    if (GetParam() != ExecutionMode::PIPELINE_DRIVER_RANGES) {
        GTEST_SKIP() << "Channel inputs use per-driver pipeline ranges";
    }
    const auto load_id = UniqueId::gen_uid().to_thrift();
    auto channel = make_range(load_id);
    auto& broker = channel.scan_range.broker_scan_range;
    broker.__set_channel_id(0);
    broker.params.__set_label("channel_only");
    broker.params.__set_db_name("db");
    broker.params.__set_table_name("table");
    broker.params.__set_txn_id(1);
    _ranges.emplace_back(std::move(channel));
    ASSERT_OK(prepare_pipes());

    EXPECT_TRUE(_pipeline->_fragment_ctx->_stream_load_pipes.empty());
    ASSERT_EQ(1, _pipeline->_fragment_ctx->_stream_load_contexts.size());
    auto pipe = _exec_env->load_stream_mgr()->get(load_id);
    ASSERT_NE(nullptr, pipe);
    _pipeline->_fragment_ctx.reset();
    EXPECT_TRUE(pipe->no_block_read().status().is_cancelled());
    EXPECT_EQ(nullptr, _exec_env->load_stream_mgr()->get(load_id));
}

TEST_P(StreamLoadCancelTest, MissingPipeFailsPrepare) {
    _ranges.emplace_back(make_range(UniqueId::gen_uid().to_thrift()));
    auto status = prepare_pipes();
    EXPECT_TRUE(status.is_internal_error()) << status;
    EXPECT_NE(std::string::npos, status.message().find("Invalid or outdated load id"));
}

TEST_P(StreamLoadCancelTest, PartialBindingFailureCancelsPipeWithOriginalError) {
    auto pipe = add_pipe();
    _ranges.emplace_back(make_range(UniqueId::gen_uid().to_thrift()));
    const auto status = prepare_pipes();
    ASSERT_TRUE(status.is_internal_error()) << status;
    EXPECT_EQ(status.to_string(false), pipe->no_block_read().status().to_string(false));
    if (is_pipeline()) {
        EXPECT_TRUE(_pipeline->_fragment_ctx->_stream_load_pipes.empty());
    }
}

TEST_P(StreamLoadCancelTest, RepeatedCancellationPreservesFirstError) {
    auto pipe = add_pipe();
    ASSERT_OK(prepare_pipes());
    const auto error = Status::InternalError("injected load failure");
    if (is_pipeline()) {
        _pipeline->_fragment_ctx->cancel(error);
    } else {
        _legacy->_status = error;
        _legacy->cancel();
    }
    cancel();

    auto result = pipe->no_block_read();
    EXPECT_EQ(error.to_string(), result.status().to_string());
}

TEST_P(StreamLoadCancelTest, NormalCompletionPreservesDataAndEof) {
    auto pipe = add_pipe();
    ASSERT_OK(prepare_pipes());
    ASSERT_OK(pipe->append(make_buffer("data")));
    ASSERT_OK(pipe->finish());
    if (is_pipeline()) {
        // finish() calls cancel(OK); the new attachment must not turn it into failure.
        _pipeline->_fragment_ctx->finish();
    }

    auto result = pipe->read();
    ASSERT_OK(result.status());
    EXPECT_EQ("data", std::string(result.value()->ptr, result.value()->remaining()));
    EXPECT_TRUE(pipe->read().status().is_end_of_file());
}

TEST_P(StreamLoadCancelTest, CloseWakesReaderBeforeWaitingForScanner) {
    auto pipe = add_pipe();
    ASSERT_OK(prepare_pipes());
    std::promise<void> started;
    auto entered = started.get_future();
    std::promise<Status> result;
    auto done = result.get_future();
    std::thread reader([&] {
        started.set_value();
        result.set_value(pipe->read().status());
    });
    DeferOp cleanup([&] {
        pipe->cancel(Status::Cancelled("test cleanup"));
        reader.join();
    });
    ASSERT_EQ(std::future_status::ready, entered.wait_for(5s));
    ASSERT_EQ(std::future_status::timeout, done.wait_for(20ms));

    if (is_pipeline()) {
        _pipeline->_fragment_ctx.reset();
    } else {
        DescriptorTbl desc;
        CloseProbeNode plan(_state->obj_pool(), desc, [&] {
            EXPECT_EQ(std::future_status::ready, done.wait_for(5s))
                    << "pipe must be interrupted before plan->close() waits for scanner exit";
        });
        _legacy->_plan = &plan;
        DeferOp reset_plan([&] { _legacy->_plan = nullptr; });
        _legacy->close();
    }

    ASSERT_EQ(std::future_status::ready, done.wait_for(5s));
    const auto status = done.get();
    EXPECT_TRUE(status.is_cancelled()) << status;
    EXPECT_EQ("Close the stream load pipe", status.message());
}

INSTANTIATE_TEST_SUITE_P(ExecutionModes, StreamLoadCancelTest,
                         testing::Values(ExecutionMode::NON_PIPELINE, ExecutionMode::PIPELINE_NODE_RANGES,
                                         ExecutionMode::PIPELINE_DRIVER_RANGES),
                         [](const testing::TestParamInfo<ExecutionMode>& info) {
                             switch (info.param) {
                             case ExecutionMode::NON_PIPELINE:
                                 return "NonPipeline";
                             case ExecutionMode::PIPELINE_NODE_RANGES:
                                 return "PipelineNodeRanges";
                             case ExecutionMode::PIPELINE_DRIVER_RANGES:
                                 return "PipelineDriverRanges";
                             }
                             return "Unknown";
                         });

} // namespace
} // namespace starrocks
