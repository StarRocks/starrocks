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

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "exec/pipeline/exchange/local_exchange.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/stream_epoch_manager.h"
#include "gtest/gtest.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace starrocks {
class ConnectorScanNode;
}

namespace starrocks::stream {

using InitiliazeFunc = std::function<Status()>;

class StreamPipelineTest {
public:
    Status prepare();
    Status execute();

    Status start_mv(InitiliazeFunc&& init_func);
    void stop_mv();
    void cancel_mv();

    Status start_epoch(const std::vector<int64_t>& tablet_ids, const EpochInfo& epoch_info);
    Status wait_until_epoch_finished(const EpochInfo& epoch_info);

    template <typename T>
    std::vector<ChunkPtr> fetch_results(const EpochInfo& epoch_info);

    size_t next_operator_id() { return _pipeline_context->next_operator_id(); }
    size_t next_plan_node_id() { return _next_operator_id++; }
    uint32_t next_pipeline_id() { return _pipeline_context->next_pipe_id(); }

protected:
    OpFactories maybe_interpolate_local_passthrough_exchange(OpFactories& pred_operators);

    ExecEnv* _exec_env = nullptr;
    pipeline::QueryContext* _query_ctx = nullptr;
    pipeline::FragmentContext* _fragment_ctx = nullptr;
    pipeline::FragmentFuture _fragment_future;
    RuntimeState* _runtime_state = nullptr;
    ObjectPool* _obj_pool = nullptr;
    TExecPlanFragmentParams _request;
    // lambda used to init _pipelines
    std::function<void(RuntimeState*)> _pipeline_builder;
    pipeline::Pipelines _pipelines;
    std::vector<int64_t> _tablet_ids;
    std::shared_ptr<starrocks::ConnectorScanNode> _connector_node;
    size_t _degree_of_parallelism;
    pipeline::PipelineBuilderContext* _pipeline_context = nullptr;

private:
    size_t _next_operator_id = 0;
};

template <typename T>
std::vector<ChunkPtr> StreamPipelineTest::fetch_results(const EpochInfo& epoch_info) {
    VLOG_ROW << "FetchResults: " << epoch_info.debug_string();
    std::vector<ChunkPtr> result_chunks;
    const auto& pipelines = _fragment_ctx->pipelines();
    for (auto& pipeline : pipelines) {
        for (auto& driver : pipeline->drivers()) {
            auto* sink_op = driver->sink_operator();
            if (auto* stream_sink_op = dynamic_cast<T*>(sink_op); stream_sink_op != nullptr) {
                result_chunks = stream_sink_op->output_chunks();
                for (auto& chunk : result_chunks) {
                    VLOG_ROW << "FetchResults, result: " << chunk->debug_columns();
                }
                CHECK(stream_sink_op->reset_epoch(nullptr).ok());
                break;
            }
        }
    }
    return result_chunks;
}

} // namespace starrocks::stream
