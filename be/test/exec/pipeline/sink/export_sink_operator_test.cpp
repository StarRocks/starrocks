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

#include "exec/pipeline/sink/export_sink_operator.h"

#include <chrono>
#include <thread>

#include "gen_cpp/RuntimeProfile_types.h"
#include "gtest/gtest.h"
#include "testutil/assert.h"
#include "util/await.h"

namespace starrocks::pipeline {

TEST(ExportSinkOperatorTest, test_set_finishing) {
    using namespace std::chrono_literals;
    TExecPlanFragmentParams _request;

    const auto& params = _request.params;
    const auto& query_id = params.query_id;
    const auto& fragment_id = params.fragment_instance_id;

    pipeline::QueryContext* _query_ctx;
    pipeline::FragmentContext* _fragment_ctx;
    ExecEnv* _exec_env = ExecEnv::GetInstance();
    RuntimeState _runtime_state(_exec_env);

    ASSIGN_OR_ASSERT_FAIL(_query_ctx, _exec_env->query_context_mgr()->get_or_register(query_id));
    _query_ctx->set_query_id(query_id);
    _query_ctx->set_total_fragments(1);
    _query_ctx->set_delivery_expire_seconds(60);
    _query_ctx->set_query_expire_seconds(60);
    _query_ctx->extend_delivery_lifetime();
    _query_ctx->extend_query_lifetime();
    _query_ctx->set_final_sink();
    _query_ctx->init_mem_tracker(GlobalEnv::GetInstance()->query_pool_mem_tracker()->limit(),
                                 GlobalEnv::GetInstance()->query_pool_mem_tracker());

    _fragment_ctx = _query_ctx->fragment_mgr()->get_or_register(fragment_id);
    _fragment_ctx->set_query_id(query_id);
    _fragment_ctx->set_fragment_instance_id(fragment_id);
    _fragment_ctx->set_runtime_state(
            std::make_unique<RuntimeState>(_request.params.query_id, _request.params.fragment_instance_id,
                                           _request.query_options, _request.query_globals, _exec_env));
    _fragment_ctx->set_is_stream_pipeline(true);

    _runtime_state.set_query_ctx(_query_ctx);
    _runtime_state.set_fragment_ctx(_fragment_ctx);

    TExportSink t_sink;
    std::vector<TExpr> t_output_expr;
    ExportSinkOperatorFactory factory(1, t_sink, t_output_expr, 1, _fragment_ctx);
    EXPECT_TRUE(factory.prepare(&_runtime_state).ok());

    auto export_op = factory.create(1, 1);
    EXPECT_TRUE(export_op->prepare(&_runtime_state).ok());

    // push a chunk, fail to create the file writer, so the context will be in cancel state
    ChunkPtr chunk = std::make_shared<Chunk>();
    EXPECT_TRUE(export_op->push_chunk(&_runtime_state, chunk).ok());

    int timeout_us = 5 * 1000 * 1000; // 5s

    Awaitility await;
    EXPECT_TRUE(await.timeout(timeout_us).until([&] { return _fragment_ctx->is_canceled(); }));

    // now cancel the operator
    export_op->set_finishing(&_runtime_state);
    export_op->set_cancelled(&_runtime_state);

    Awaitility await2;
    EXPECT_TRUE(await2.timeout(timeout_us).until([&] { return !export_op->pending_finish(); }));
}

} // namespace starrocks::pipeline
