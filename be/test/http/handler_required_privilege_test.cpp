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

// Pins the priv-level overrides on BE HTTP handlers that were chosen with the
// http_auth feature. Each override is a one-line virtual return, so the failure
// mode is silent demotion/promotion in a future refactor (e.g. demoting NODE to
// OPERATE for a node-mgmt endpoint, or promoting OPERATE to ADMIN for a
// query-debug endpoint). Locking the values here keeps the SQL-aligned policy
// from drifting.

#include <gtest/gtest.h>

#include "http/core/ev_http_server.h"
#include "http/core/http_handler.h"
#include "http/service/action/checksum_action.h"
#include "http/service/action/compact_rocksdb_meta_action.h"
#include "http/service/action/compaction_action.h"
#include "http/service/action/datacache_action.h"
#include "http/service/action/greplog_action.h"
#include "http/service/action/health_action.h"
#include "http/service/action/lake/dump_tablet_metadata_action.h"
#include "http/service/action/memory_metrics_action.h"
#include "http/service/action/meta_action.h"
#include "http/service/action/metrics_action.h"
#include "http/service/action/pipeline_blocking_drivers_action.h"
#include "http/service/action/pprof_actions.h"
#include "http/service/action/proc_profile_action.h"
#include "http/service/action/proc_profile_file_action.h"
#include "http/service/action/query_cache_action.h"
#include "http/service/action/reload_tablet_action.h"
#include "http/service/action/restore_tablet_action.h"
#include "http/service/action/runtime_filter_cache_action.h"
#include "http/service/action/snapshot_action.h"
#include "http/service/action/stop_be_action.h"
#include "http/service/action/stream_load.h"
#include "http/service/action/transaction_stream_load.h"
#include "http/service/action/update_config_action.h"
#include "http/service/download_action.h"
#include "http/service/web_page_handler.h"
#include "orchestration/stream_load_orchestrator.h"
#include "runtime/env/global_env.h"
#include "runtime/exec_env.h"

#ifdef STARROCKS_JIT_ENABLE
#include "http/service/action/jit_cache_action.h"
#endif

namespace starrocks {

// Tests only call required_privilege() / need_auth() — const lookups that don't
// touch the stored GlobalEnv&. The process-wide singleton is the cheapest valid
// reference that won't crash if a future override accidentally dereferences it.
static const GlobalEnv& fake_global_env() {
    return *GlobalEnv::GetInstance();
}

using Priv = HttpHandler::RequiredPrivilege;

// --------- NODE (cluster_admin domain) ---------
// Matches SQL `DROP BACKEND` policy. db_admin / security_admin must NOT be
// able to stop BEs through this endpoint.
TEST(BeHandlerPrivilegeTest, stop_be_requires_NODE) {
    StopBeAction h(nullptr);
    EXPECT_EQ(Priv::NODE, h.required_privilege());
}

// --------- OPERATE (DB / data / execution operate) ---------
// Matches SQL `ADMIN SET BACKEND CONFIG` / `ADMIN COMPACT` policy.
TEST(BeHandlerPrivilegeTest, update_config_requires_OPERATE) {
    UpdateConfigAction h;
    EXPECT_EQ(Priv::OPERATE, h.required_privilege());
}

TEST(BeHandlerPrivilegeTest, compaction_requires_OPERATE_all_subtypes) {
    EXPECT_EQ(Priv::OPERATE, CompactionAction(CompactionActionType::SHOW_INFO).required_privilege());
    EXPECT_EQ(Priv::OPERATE, CompactionAction(CompactionActionType::RUN_COMPACTION).required_privilege());
    EXPECT_EQ(Priv::OPERATE, CompactionAction(CompactionActionType::SHOW_REPAIR).required_privilege());
    EXPECT_EQ(Priv::OPERATE, CompactionAction(CompactionActionType::SUBMIT_REPAIR).required_privilege());
    EXPECT_EQ(Priv::OPERATE, CompactionAction(CompactionActionType::SHOW_RUNNING_TASK).required_privilege());
}

TEST(BeHandlerPrivilegeTest, checksum_requires_OPERATE) {
    ChecksumAction h(fake_global_env());
    EXPECT_EQ(Priv::OPERATE, h.required_privilege());
}

TEST(BeHandlerPrivilegeTest, reload_tablet_requires_OPERATE) {
    ReloadTabletAction h(nullptr);
    EXPECT_EQ(Priv::OPERATE, h.required_privilege());
}

TEST(BeHandlerPrivilegeTest, restore_tablet_requires_OPERATE) {
    RestoreTabletAction h(nullptr);
    EXPECT_EQ(Priv::OPERATE, h.required_privilege());
}

TEST(BeHandlerPrivilegeTest, snapshot_requires_OPERATE) {
    SnapshotAction h(nullptr);
    EXPECT_EQ(Priv::OPERATE, h.required_privilege());
}

TEST(BeHandlerPrivilegeTest, meta_requires_OPERATE) {
    MetaAction h(META_TYPE::HEADER);
    EXPECT_EQ(Priv::OPERATE, h.required_privilege());
}

TEST(BeHandlerPrivilegeTest, dump_tablet_metadata_requires_OPERATE) {
    lake::DumpTabletMetadataAction h(nullptr);
    EXPECT_EQ(Priv::OPERATE, h.required_privilege());
}

TEST(BeHandlerPrivilegeTest, compact_rocksdb_meta_requires_OPERATE) {
    CompactRocksDbMetaAction h(nullptr);
    EXPECT_EQ(Priv::OPERATE, h.required_privilege());
}

TEST(BeHandlerPrivilegeTest, datacache_requires_OPERATE) {
    DataCacheAction h(nullptr, nullptr);
    EXPECT_EQ(Priv::OPERATE, h.required_privilege());
}

TEST(BeHandlerPrivilegeTest, query_cache_requires_OPERATE) {
    QueryCacheAction h(nullptr);
    EXPECT_EQ(Priv::OPERATE, h.required_privilege());
}

TEST(BeHandlerPrivilegeTest, runtime_filter_cache_requires_OPERATE) {
    RuntimeFilterCacheAction h(nullptr);
    EXPECT_EQ(Priv::OPERATE, h.required_privilege());
}

#ifdef STARROCKS_JIT_ENABLE
TEST(BeHandlerPrivilegeTest, jit_cache_requires_OPERATE) {
    JITCacheAction h;
    EXPECT_EQ(Priv::OPERATE, h.required_privilege());
}
#endif

TEST(BeHandlerPrivilegeTest, pipeline_blocking_drivers_requires_OPERATE) {
    PipelineBlockingDriversAction h(nullptr);
    EXPECT_EQ(Priv::OPERATE, h.required_privilege());
}

// --------- OPERATE (process profiling / log diagnostics) ---------
// DB operators (db_admin → OPERATE) inspect profiles to diagnose hot queries
// running on the BE. cluster_admin holders can pick up OPERATE explicitly when
// they need pprof access.
TEST(BeHandlerPrivilegeTest, pprof_handlers_require_OPERATE) {
    EXPECT_EQ(Priv::OPERATE, HeapAction().required_privilege());
    EXPECT_EQ(Priv::OPERATE, GrowthAction().required_privilege());
    EXPECT_EQ(Priv::OPERATE, ProfileAction().required_privilege());
    EXPECT_EQ(Priv::OPERATE, IOProfileAction().required_privilege());
    EXPECT_EQ(Priv::OPERATE, PmuProfileAction().required_privilege());
    EXPECT_EQ(Priv::OPERATE, ContentionAction().required_privilege());
    EXPECT_EQ(Priv::OPERATE, CmdlineAction().required_privilege());
    EXPECT_EQ(Priv::OPERATE, SymbolAction().required_privilege());
}

TEST(BeHandlerPrivilegeTest, proc_profile_requires_OPERATE) {
    ProcProfileAction h(nullptr);
    EXPECT_EQ(Priv::OPERATE, h.required_privilege());
}

TEST(BeHandlerPrivilegeTest, proc_profile_file_requires_OPERATE) {
    ProcProfileFileAction h(nullptr);
    EXPECT_EQ(Priv::OPERATE, h.required_privilege());
}

TEST(BeHandlerPrivilegeTest, greplog_requires_OPERATE) {
    GrepLogAction h;
    EXPECT_EQ(Priv::OPERATE, h.required_privilege());
}

// --------- DownloadAction: dynamic need_auth() based on download type ---------
// NORMAL (BE-to-BE tablet clone, internal load file download): token-gated,
// framework Basic is skipped (`need_auth() == false`).
// ERROR_LOG (user-facing load-failure log): framework Basic required.
TEST(BeHandlerNeedAuthTest, download_action_normal_skips_framework_auth) {
    DownloadAction normal(std::vector<std::string>{});
    EXPECT_FALSE(normal.need_auth());
}

TEST(BeHandlerNeedAuthTest, download_action_error_log_requires_framework_auth) {
    DownloadAction error_log(std::string{});
    EXPECT_TRUE(error_log.need_auth());
}

// Probe / Prometheus endpoints (/api/health, /metrics, /metrics/memory) are AuthN-only:
// historically anonymous, now gated by `config::enable_http_auth` -- need_auth() == true
// (the injected verifier short-circuits when the flag is off) with no extra privilege.
TEST(BeHandlerNeedAuthTest, probe_and_prometheus_endpoints_are_authn_only) {
    EXPECT_TRUE(HealthAction(nullptr).need_auth());
    EXPECT_EQ(Priv::NONE, HealthAction(nullptr).required_privilege());
    EXPECT_TRUE(MetricsAction(nullptr).need_auth());
    EXPECT_EQ(Priv::NONE, MetricsAction(nullptr).required_privilege());
    EXPECT_TRUE(MemoryMetricsAction(fake_global_env()).need_auth());
    EXPECT_EQ(Priv::NONE, MemoryMetricsAction(fake_global_env()).required_privilege());
}

TEST(BeHandlerNeedAuthTest, stream_load_uses_builtin_fe_auth_flow) {
    ExecEnv env;
    orchestration::StreamLoadOrchestrator stream_load_orchestrator(&env, nullptr);
    StreamLoadAction action(&env, &stream_load_orchestrator, nullptr);
    EXPECT_FALSE(action.need_auth());
}

TEST(BeHandlerNeedAuthTest, transaction_endpoints_skip_framework_auth) {
    // Both transaction-management endpoints opt out of the framework Basic-Auth
    // gate — they use a label-bound session model (begin parses Basic, later ops
    // look up the StreamLoadContext by label).
    TransactionManagerAction txn_mgr(nullptr);
    EXPECT_FALSE(txn_mgr.need_auth());
    ExecEnv env;
    orchestration::StreamLoadOrchestrator stream_load_orchestrator(&env, nullptr);
    TransactionStreamLoadAction txn_load(&env, &stream_load_orchestrator);
    EXPECT_FALSE(txn_load.need_auth());
}

TEST(BeHandlerPrivilegeTest, web_page_handler_requires_OPERATE) {
    // BE Web UI is operator-debug surface — same policy as pprof / ioprofile.
    EvHttpServer server(/*port=*/0, /*num_workers=*/1);
    WebPageHandler handler(&server);
    EXPECT_EQ(Priv::OPERATE, handler.required_privilege());
}

} // namespace starrocks
