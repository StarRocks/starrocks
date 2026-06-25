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

#include "data_workflows/data_workflows_env.h"

#include <memory>

#include "data_workflows/load/tablet_writer/load_channel_mgr.h"

namespace starrocks {

DataWorkflowsEnv::DataWorkflowsEnv() = default;

DataWorkflowsEnv::~DataWorkflowsEnv() {
    destroy();
}

Status DataWorkflowsEnv::init(const DataWorkflowsEnvOptions& options) {
    DCHECK(options.diagnose_daemon != nullptr);
    DCHECK(options.brpc_stub_cache != nullptr);
    DCHECK(options.load_mem_tracker != nullptr);

    _load_channel_mgr =
            std::make_unique<LoadChannelMgr>(options.lake_tablet_manager, options.diagnose_daemon,
                                             options.brpc_stub_cache, options.metrics, options.table_metrics_mgr);
    RETURN_IF_ERROR(_load_channel_mgr->init(options.load_mem_tracker));
    _load_channel_mgr_started = true;
    return Status::OK();
}

void DataWorkflowsEnv::stop() {
    if (_load_channel_mgr != nullptr && _load_channel_mgr_started) {
        _load_channel_mgr->close();
        _load_channel_mgr_started = false;
    }
}

void DataWorkflowsEnv::destroy() {
    stop();
    _load_channel_mgr.reset();
}

} // namespace starrocks
