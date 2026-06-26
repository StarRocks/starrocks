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

#pragma once

#include <unordered_map>
#include <vector>

#include "common/global_types.h"
#include "common/status.h"
#include "compute_env/workgroup/work_group_fwd.h"
#include "exec/pipeline/fragment_execution_params.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/runtime/group_execution/execution_group_fwd.h"
#include "gen_cpp/InternalService_types.h"
#include "runtime/exec_env_fwd.h"

namespace starrocks {
class RuntimeState;

namespace query_orchestration {

class FragmentExecutor {
public:
    FragmentExecutor();

    Status prepare(ExecEnv* exec_env, const TExecPlanFragmentParams& common_request,
                   const TExecPlanFragmentParams& unique_request);
    Status execute(ExecEnv* exec_env);

    static Status append_incremental_scan_ranges(ExecEnv* exec_env, const TExecPlanFragmentParams& request,
                                                 TExecPlanFragmentResult* response);

    // Register the partition_value of each scan range into its HiveTableDescriptor's
    // _partition_id_to_desc_map. The HdfsPartitionDescriptor is allocated from the
    // query-level ObjectPool (RuntimeStateHelper::global_obj_pool) so that the map's
    // entries outlive any single fragment instance — see the function body for the
    // UAF this prevents.
    //
    // Exposed here so unit tests can pin the contract; production callers go through
    // _prepare_exec_plan / append_incremental_scan_ranges, not this entry point.
    static Status add_scan_ranges_partition_values(RuntimeState* runtime_state,
                                                   const std::vector<TScanRangeParams>& scan_ranges);

    Status prepare_global_state(ExecEnv* exec_env, const TExecPlanFragmentParams& common_request);
    void _fail_cleanup(bool fragment_has_registed);

private:
    uint32_t _calc_dop(ExecEnv* exec_env, const pipeline::UnifiedExecPlanFragmentParams& request) const;
    uint32_t _calc_sink_dop(ExecEnv* exec_env, const pipeline::UnifiedExecPlanFragmentParams& request) const;
    int _calc_delivery_expired_seconds(const pipeline::UnifiedExecPlanFragmentParams& request) const;
    int _calc_query_expired_seconds(const pipeline::UnifiedExecPlanFragmentParams& request) const;

    // Several steps of prepare a fragment
    // 1. query context
    // 2. fragment context
    // 3. workgroup
    // 4. runtime state
    // 5. exec plan
    // 6. pipeline driver
    Status _prepare_query_ctx(ExecEnv* exec_env, const pipeline::UnifiedExecPlanFragmentParams& request);
    Status _prepare_fragment_ctx(const pipeline::UnifiedExecPlanFragmentParams& request);
    Status _prepare_workgroup(const pipeline::UnifiedExecPlanFragmentParams& request);
    Status _prepare_runtime_state(ExecEnv* exec_env, const pipeline::UnifiedExecPlanFragmentParams& request);
    Status _prepare_exec_plan(ExecEnv* exec_env, const pipeline::UnifiedExecPlanFragmentParams& request);
    Status _prepare_global_dict(const pipeline::UnifiedExecPlanFragmentParams& request);
    Status _prepare_pipeline_driver(ExecEnv* exec_env, const pipeline::UnifiedExecPlanFragmentParams& request);
    Status _prepare_stream_load_pipe(ExecEnv* exec_env, const pipeline::UnifiedExecPlanFragmentParams& request);

    std::unordered_map<int32_t, pipeline::ExecutionGroupPtr> _colocate_exec_groups;
    bool _is_in_colocate_exec_group(PlanNodeId plan_node_id);

    int64_t _fragment_start_time = 0;
    pipeline::QueryContextManager* _query_ctx_mgr = nullptr;
    pipeline::QueryContext* _query_ctx = nullptr;
    // Pin the QueryContext alive for at least as long as `_fragment_ctx`.
    // Fragment operators cache raw pointers into query-scoped state (e.g.
    // QueryContext::spill_manager()); if the QueryContext is reclaimed (its active
    // fragment count reaches 0) while this executor still holds the last reference
    // to `_fragment_ctx`, tearing the fragment down would deref freed memory.
    // Declared before `_fragment_ctx` so it is destroyed AFTER it.
    pipeline::QueryContextPtr _query_ctx_hold = nullptr;
    pipeline::FragmentContextPtr _fragment_ctx = nullptr;
    workgroup::WorkGroupPtr _wg = nullptr;
};

} // namespace query_orchestration
} // namespace starrocks
