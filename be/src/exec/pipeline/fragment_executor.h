// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "common/status.h"
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

    int _calc_delivery_expired_seconds(const TExecPlanFragmentParams& request) const;
    int _calc_query_expired_seconds(const TExecPlanFragmentParams& request) const;

    void _decompose_data_sink_to_operator(RuntimeState* state, PipelineBuilderContext* context,
                                          const TDataSink& t_datasink, DataSink* datasink);
    QueryContext* _query_ctx = nullptr;
    FragmentContext* _fragment_ctx = nullptr;
    workgroup::WorkGroup* _wg = nullptr;
};
} // namespace pipeline
} // namespace starrocks
