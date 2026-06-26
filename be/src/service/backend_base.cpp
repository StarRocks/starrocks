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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/service/backend_service.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "service/backend_base.h"

#include <thrift/protocol/TDebugProtocol.h>

#include "common/logging.h"
#include "common/status.h"
#include "compute_env/load/stream_context_mgr.h"
#include "compute_env/result/result_buffer_mgr.h"
#include "orchestration/external_scan_orchestrator.h"
#include "orchestration/orchestration_env.h"
#include "orchestration/routine_load_task_executor.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"

namespace starrocks {

BackendServiceBase::BackendServiceBase(ExecEnv* exec_env, orchestration::OrchestrationEnv* orchestration_env)
        : _exec_env(exec_env), _orchestration_env(orchestration_env) {}

void BackendServiceBase::exec_plan_fragment(TExecPlanFragmentResult& return_val,
                                            const TExecPlanFragmentParams& params) {
    LOG(INFO) << "exec_plan_fragment() instance_id=" << params.params.fragment_instance_id << " coord=" << params.coord
              << " backend#=" << params.backend_num;
    VLOG_ROW << "exec_plan_fragment params is " << apache::thrift::ThriftDebugString(params).c_str();
    start_plan_fragment_execution(params).set_t_status(&return_val);
}

Status BackendServiceBase::start_plan_fragment_execution(const TExecPlanFragmentParams& exec_params) {
    if (!exec_params.fragment.__isset.output_sink) {
        return Status::InternalError("missing sink in plan fragment");
    }
    return _exec_env->fragment_mgr()->exec_plan_fragment(exec_params);
}

void BackendServiceBase::cancel_plan_fragment(TCancelPlanFragmentResult& return_val,
                                              const TCancelPlanFragmentParams& params) {
    LOG(INFO) << "cancel_plan_fragment(): instance_id=" << params.fragment_instance_id;
    _exec_env->fragment_mgr()->cancel(params.fragment_instance_id).set_t_status(&return_val);
}

void BackendServiceBase::transmit_data(TTransmitDataResult& return_val, const TTransmitDataParams& params) {
    VLOG_ROW << "transmit_data(): instance_id=" << params.dest_fragment_instance_id
             << " node_id=" << params.dest_node_id << " #rows=" << params.row_batch.num_rows
             << " eos=" << (params.eos ? "true" : "false");

    if (params.__isset.packet_seq) {
        return_val.__set_packet_seq(params.packet_seq);
        return_val.__set_dest_fragment_instance_id(params.dest_fragment_instance_id);
        return_val.__set_dest_node_id(params.dest_node_id);
    }
}

void BackendServiceBase::fetch_data(TFetchDataResult& return_val, const TFetchDataParams& params) {
    // maybe hang in this function
    Status status = _exec_env->result_mgr()->fetch_data(params.fragment_instance_id, &return_val);
    status.set_t_status(&return_val);
}

void BackendServiceBase::submit_routine_load_task(TStatus& t_status, const std::vector<TRoutineLoadTask>& tasks) {
#ifdef __APPLE__
    Status::NotSupported("submit_routine_load_task is not supported on MacOS").to_thrift(&t_status);
#else
    DCHECK(_orchestration_env != nullptr);
    auto* routine_load_task_executor = _orchestration_env->routine_load_task_executor();
    DCHECK(routine_load_task_executor != nullptr);
    for (auto& task : tasks) {
        Status st = routine_load_task_executor->submit_task(task);
        if (!st.ok()) {
            LOG(WARNING) << "failed to submit routine load task. job id: " << task.job_id << " task id: " << task.id;
            return st.to_thrift(&t_status);
        }
    }

    return Status::OK().to_thrift(&t_status);
#endif
}

void BackendServiceBase::finish_stream_load_channel(TStatus& t_status, const TStreamLoadChannel& stream_load_channel) {
    Status st = _exec_env->stream_context_mgr()->finish_body_sink(
            stream_load_channel.label, stream_load_channel.table_name, stream_load_channel.channel_id);
    if (!st.ok()) {
        LOG(WARNING) << "failed to finish stream load channel. label: " << stream_load_channel.label
                     << " table name: " << stream_load_channel.table_name
                     << " channel id: " << stream_load_channel.channel_id;
        return st.to_thrift(&t_status);
    }
    return Status::OK().to_thrift(&t_status);
}

void BackendServiceBase::open_scanner(TScanOpenResult& result_, const TScanOpenParams& params) {
    DCHECK(_orchestration_env != nullptr);
    DCHECK(_orchestration_env->external_scan_orchestrator() != nullptr);
    _orchestration_env->external_scan_orchestrator()->open_scanner(result_, params);
}

void BackendServiceBase::get_next(TScanBatchResult& result_, const TScanNextBatchParams& params) {
    DCHECK(_orchestration_env != nullptr);
    DCHECK(_orchestration_env->external_scan_orchestrator() != nullptr);
    _orchestration_env->external_scan_orchestrator()->get_next(result_, params);
}

void BackendServiceBase::close_scanner(TScanCloseResult& result_, const TScanCloseParams& params) {
    DCHECK(_orchestration_env != nullptr);
    DCHECK(_orchestration_env->external_scan_orchestrator() != nullptr);
    _orchestration_env->external_scan_orchestrator()->close_scanner(result_, params);
}

} // namespace starrocks
