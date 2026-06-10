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

#include <atomic>
#include <utility>

#include "common/status.h"
#include "compute_env/workgroup/work_group_fwd.h"
#include "exec/pipeline/runtime_filter_hub.h"
#include "gen_cpp/Types_types.h" // for TNetworkAddress, TUniqueId
#include "storage/primitive/predicate_tree_params.h"

namespace starrocks::pipeline {

class FragmentContext;

class FragmentRuntimeState {
    friend class FragmentContext;

public:
    FragmentRuntimeState() = default;

    void set_fragment_instance_id(const TUniqueId& fragment_instance_id) {
        _fragment_instance_id = fragment_instance_id;
    }
    const TUniqueId& fragment_instance_id() const { return _fragment_instance_id; }

    RuntimeFilterHub* runtime_filter_hub() { return &_runtime_filter_hub; }
    const RuntimeFilterHub* runtime_filter_hub() const { return &_runtime_filter_hub; }

    void set_enable_cache(bool flag) { _enable_cache = flag; }
    bool enable_cache() const { return _enable_cache; }

    void set_fe_addr(const TNetworkAddress& fe_addr) { _fe_addr = fe_addr; }
    const TNetworkAddress& fe_addr() const { return _fe_addr; }

    void set_enable_adaptive_dop(bool val) { _enable_adaptive_dop = val; }
    bool enable_adaptive_dop() const { return _enable_adaptive_dop; }

    void set_pred_tree_params(const PredicateTreeParams& params) { _pred_tree_params = params; }
    const PredicateTreeParams& pred_tree_params() const { return _pred_tree_params; }

    void set_workgroup(workgroup::WorkGroupPtr wg) { _workgroup = std::move(wg); }
    const workgroup::WorkGroupPtr& workgroup() const { return _workgroup; }

    Status final_status() const {
        auto* status = _final_status.load();
        return status == nullptr ? Status::OK() : *status;
    }

private:
    TUniqueId _fragment_instance_id;
    RuntimeFilterHub _runtime_filter_hub;
    bool _enable_cache = false;
    TNetworkAddress _fe_addr;
    bool _enable_adaptive_dop = false;
    PredicateTreeParams _pred_tree_params;
    workgroup::WorkGroupPtr _workgroup = nullptr;
    std::atomic<Status*> _final_status = nullptr;
    Status _s_status;
};

} // namespace starrocks::pipeline
