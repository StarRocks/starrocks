// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "exec/pipeline/exchange/local_exchange.h"
#include "exec/pipeline/exchange/local_exchange_sink_operator.h"
#include "exec/pipeline/exchange/local_exchange_source_operator.h"
#include "gen_cpp/InternalService_types.h"
#include "gtest/gtest.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/storage_engine.h"

namespace starrocks::pipeline {

class PipelineTestBase : public ::testing::Test {
public:
    static vectorized::ChunkPtr _create_and_fill_chunk(const std::vector<SlotDescriptor*>& slots, size_t row_num);
    static vectorized::ChunkPtr _create_and_fill_chunk(size_t row_num);

public:
    virtual void SetUp() final;
    virtual void TearDown() final;

protected:
    ExecEnv* _exec_env = nullptr;
    QueryContext* _query_ctx = nullptr;
    FragmentContext* _fragment_ctx = nullptr;
    FragmentFuture _fragment_future;
    RuntimeState* _runtime_state = nullptr;
    ObjectPool* _obj_pool = nullptr;

    TExecPlanFragmentParams _request;

    // Entry of test, subclass should call this method to start test
    void start_test();

    size_t next_operator_id() { return ++_next_operator_id; }
    size_t next_plan_node_id() { return ++_next_plan_node_id; }
    uint32_t next_pipeline_id() { return ++_next_pipeline_id; }

    OpFactories maybe_interpolate_local_passthrough_exchange(OpFactories& pred_operators);

    // SubClass can init request in this method
    virtual void _prepare_request() {}

    // lambda used to init _pipelines
    std::function<void(RuntimeState*)> _pipeline_builder;
    Pipelines _pipelines;

private:
    // Prepare execution context of pipeline
    void _prepare();

    // execute pipeline
    void _execute();

    size_t _next_operator_id = 0;
    size_t _next_plan_node_id = 0;
    uint32_t _next_pipeline_id = 0;
};
}; // namespace starrocks::pipeline
