// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/fragment_mgr.h

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

#ifndef STARROCKS_BE_RUNTIME_FRAGMENT_MGR_H
#define STARROCKS_BE_RUNTIME_FRAGMENT_MGR_H

#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "gen_cpp/StarrocksExternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "http/rest_monitor_iface.h"
#include "runtime/mem_tracker.h"
#include "util/hash_util.hpp"

namespace starrocks {

class ExecEnv;
class FragmentExecState;
class TExecPlanFragmentParams;
class TUniqueId;
class PlanFragmentExecutor;
class ThreadPool;

namespace vectorized {
class JoinRuntimeFilter;
}
std::string to_load_error_http_path(const std::string& file_name);

// This class used to manage all the fragment execute in this instance
class FragmentMgr : public RestMonitorIface {
public:
    typedef std::function<void(PlanFragmentExecutor*)> FinishCallback;
    typedef std::function<void(PlanFragmentExecutor*)> StartSuccCallback;

    FragmentMgr(ExecEnv* exec_env);
    ~FragmentMgr() override;

    // execute one plan fragment
    Status exec_plan_fragment(const TExecPlanFragmentParams& params);

    Status exec_plan_fragment(const TExecPlanFragmentParams& params, const FinishCallback& cb);

    // TODO(zc): report this is over
    Status exec_plan_fragment(const TExecPlanFragmentParams& params, const StartSuccCallback& start_cb,
                              const FinishCallback& cb);

    Status cancel(const TUniqueId& fragment_id) {
        return cancel(fragment_id, PPlanFragmentCancelReason::INTERNAL_ERROR);
    }

    Status cancel(const TUniqueId& fragment_id, const PPlanFragmentCancelReason& reason);

    void receive_runtime_filter(const PTransmitRuntimeFilterParams& params,
                                const std::shared_ptr<const vectorized::JoinRuntimeFilter>& shared_rf);

    void cancel_worker();

    void debug(std::stringstream& ss) override;

    Status trigger_profile_report(const PTriggerProfileReportRequest* request);

    // input: TScanOpenParams fragment_instance_id
    // output: selected_columns
    // execute external query, all query info are packed in TScanOpenParams
    Status exec_external_plan_fragment(const TScanOpenParams& params, const TUniqueId& fragment_instance_id,
                                       std::vector<TScanColumnDesc>* selected_columns);

private:
    void exec_actual(std::shared_ptr<FragmentExecState>* exec_state, const FinishCallback& cb);

    // This is input params
    ExecEnv* _exec_env;

    std::mutex _lock;

    // Make sure that remove this before no data reference FragmentExecState
    std::unordered_map<TUniqueId, std::shared_ptr<FragmentExecState>> _fragment_map;

    // Cancel thread
    bool _stop;
    std::thread _cancel_thread;
    // every job is a pool
    std::unique_ptr<ThreadPool> _thread_pool;
};

} // namespace starrocks

#endif
