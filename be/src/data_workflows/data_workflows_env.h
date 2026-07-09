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

#include <memory>

#include "common/status.h"

namespace starrocks {

class BrpcStubCache;
class DiagnoseDaemon;
class ExecEnv;
class LoadChannelMgr;
class MemTracker;
class MetricRegistry;
class RejectedRecordSyncDaemon;
class TableMetricsManager;

namespace lake {
class TabletManager;
}

struct DataWorkflowsEnvOptions {
    ExecEnv* exec_env = nullptr;
    lake::TabletManager* lake_tablet_manager = nullptr;
    DiagnoseDaemon* diagnose_daemon = nullptr;
    BrpcStubCache* brpc_stub_cache = nullptr;
    MetricRegistry* metrics = nullptr;
    TableMetricsManager* table_metrics_mgr = nullptr;
    MemTracker* load_mem_tracker = nullptr;
};

class DataWorkflowsEnv {
public:
    DataWorkflowsEnv();
    ~DataWorkflowsEnv();

    Status init(const DataWorkflowsEnvOptions& options);
    void stop();
    void destroy();

    LoadChannelMgr* load_channel_mgr() { return _load_channel_mgr.get(); }
    const LoadChannelMgr* load_channel_mgr() const { return _load_channel_mgr.get(); }

private:
    std::unique_ptr<LoadChannelMgr> _load_channel_mgr;
    std::unique_ptr<RejectedRecordSyncDaemon> _rejected_record_sync_daemon;
    bool _load_channel_mgr_started = false;
    bool _rejected_record_sync_daemon_started = false;
};

} // namespace starrocks
