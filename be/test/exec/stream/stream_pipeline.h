// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include <gtest/gtest.h>

#include <chrono>

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "exec/pipeline/exchange/local_exchange.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/stream/stream_operator.h"
#include "gen_cpp/InternalService_types.h"
#include "gtest/gtest.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/storage_engine.h"

namespace starrocks::stream {

using InitiliazeFunc = std::function<Status()>;

class StreamPipelineTest {
public:
    Status StartMV(InitiliazeFunc&& init_func) {
        RETURN_IF_ERROR(init_func());
        RETURN_IF_ERROR(PreparePipeline());
        RETURN_IF_ERROR(ExecutePipeline());
        return Status::OK();
    }
    Status PreparePipeline();
    Status ExecutePipeline();
    void StopMV();
    void CancelMV();

    Status StartEpoch(const std::vector<int64_t>& tablet_ids, const EpochInfo& epoch_info);
    Status WaitUntilEpochEnd(const EpochInfo& epoch_info);

    template <typename T>
    std::vector<ChunkPtr> FetchResults(const EpochInfo& epoch_info);

    size_t next_operator_id() { return ++_next_operator_id; }
    size_t next_plan_node_id() { return ++_next_plan_node_id; }
    uint32_t next_pipeline_id() { return ++_next_pipeline_id; }

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
    size_t _next_operator_id = 0;
    size_t _next_plan_node_id = 0;
    uint32_t _next_pipeline_id = 0;
    std::vector<int64_t> _tablet_ids;
};

template <typename T>
std::vector<ChunkPtr> StreamPipelineTest::FetchResults(const EpochInfo& epoch_info) {
    VLOG_ROW << "FetchResults: " << epoch_info.debug_string();
    std::vector<ChunkPtr> result_chunks;
    auto drivers = _fragment_ctx->drivers();
    for (auto& driver : drivers) {
        auto* sink_op = driver->sink_operator();
        if (auto* stream_sink_op = dynamic_cast<T*>(sink_op); stream_sink_op != nullptr) {
            result_chunks = stream_sink_op->output_chunks();
            for (auto& chunk : result_chunks) {
                VLOG_ROW << "FetchResults, result: " << chunk->debug_columns();
            }
            stream_sink_op->reset_epoch(nullptr);
            break;
        }
    }
    return result_chunks;
}

} // namespace starrocks::stream