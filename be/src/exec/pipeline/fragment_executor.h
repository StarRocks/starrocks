// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "common/status.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/workgroup/work_group_fwd.h"
#include "gen_cpp/InternalService_types.h"

namespace starrocks {
class DataSink;
class ExecEnv;
class RuntimeProfile;
class TPlanFragmentExecParams;
class RuntimeState;

namespace pipeline {
class FragmentContext;
class PipelineBuilderContext;
class QueryContext;

class FragmentExecutor {
public:
    Status prepare(ExecEnv* exec_env, const TExecPlanFragmentParams& request);
    Status execute(ExecEnv* exec_env);

private:
    void _fail_cleanup();
    int32_t _calc_dop(ExecEnv* exec_env, const TExecPlanFragmentParams& request) const;
    int _calc_delivery_expired_seconds(const TExecPlanFragmentParams& request) const;
    int _calc_query_expired_seconds(const TExecPlanFragmentParams& request) const;

    // Several steps of prepare a fragment
    // 1. query context
    // 2. fragment context
    // 3. workgroup
    // 4. runtime state
    // 5. exec plan
    // 6. pipeline driver
    Status _prepare_query_ctx(ExecEnv* exec_env, const TExecPlanFragmentParams& request);
    Status _prepare_fragment_ctx(const TExecPlanFragmentParams& request);
    Status _prepare_workgroup(const TExecPlanFragmentParams& request);
    Status _prepare_runtime_state(ExecEnv* exec_env, const TExecPlanFragmentParams& request);
    Status _prepare_exec_plan(ExecEnv* exec_env, const TExecPlanFragmentParams& request);
    Status _prepare_global_dict(const TExecPlanFragmentParams& request);
    Status _prepare_pipeline_driver(ExecEnv* exec_env, const TExecPlanFragmentParams& request);

    Status _decompose_data_sink_to_operator(RuntimeState* runtime_state, PipelineBuilderContext* context,
                                            const TExecPlanFragmentParams& params,
                                            std::unique_ptr<starrocks::DataSink>& datasink);

    QueryContext* _query_ctx = nullptr;
    FragmentContextPtr _fragment_ctx = nullptr;
    workgroup::WorkGroupPtr _wg = nullptr;
};
} // namespace pipeline
} // namespace starrocks
