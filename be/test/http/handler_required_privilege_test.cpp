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

#include "http/action/checksum_action.h"
#include "http/action/compact_rocksdb_meta_action.h"
#include "http/action/compaction_action.h"
#include "http/action/datacache_action.h"
#include "http/action/greplog_action.h"
#include "http/action/health_action.h"
#include "http/action/lake/dump_tablet_metadata_action.h"
#include "http/action/memory_metrics_action.h"
#include "http/action/meta_action.h"
#include "http/action/metrics_action.h"
#include "http/action/pipeline_blocking_drivers_action.h"
#include "http/action/pprof_actions.h"
#include "http/action/query_cache_action.h"
#include "http/action/reload_tablet_action.h"
#include "http/action/restore_tablet_action.h"
#include "http/action/runtime_filter_cache_action.h"
#include "http/action/snapshot_action.h"
#include "http/action/stop_be_action.h"
#include "http/action/stream_load.h"
#include "http/action/transaction_stream_load.h"
#include "http/action/update_config_action.h"
#include "http/download_action.h"
#include "http/ev_http_server.h"
#include "http/http_handler.h"
#include "http/web_page_handler.h"

#ifdef STARROCKS_JIT_ENABLE
#include "http/action/jit_cache_action.h"
#endif

namespace starrocks {

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
    UpdateConfigAction h(nullptr);
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
    ChecksumAction h;
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
    DataCacheAction h(nullptr);
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
    EXPECT_EQ(Priv::OPERATE, SymbolAction(nullptr).required_privilege());
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
    DownloadAction normal(nullptr, std::vector<std::string>{});
    EXPECT_FALSE(normal.need_auth());
}

TEST(BeHandlerNeedAuthTest, download_action_error_log_requires_framework_auth) {
    DownloadAction error_log(nullptr, std::string{});
    EXPECT_TRUE(error_log.need_auth());
}

TEST(BeHandlerNeedAuthTest, probe_and_prometheus_endpoints_skip_framework_auth) {
    EXPECT_FALSE(HealthAction(nullptr).need_auth());
    EXPECT_FALSE(MetricsAction(nullptr).need_auth());
    EXPECT_FALSE(MemoryMetricsAction().need_auth());
}

TEST(BeHandlerNeedAuthTest, stream_load_uses_builtin_fe_auth_flow) {
    StreamLoadAction action(nullptr, nullptr);
    EXPECT_FALSE(action.need_auth());
}

TEST(BeHandlerNeedAuthTest, transaction_endpoints_skip_framework_auth) {
    // Both transaction-management endpoints opt out of the framework Basic-Auth
    // gate — they use a label-bound session model (begin parses Basic, later ops
    // look up the StreamLoadContext by label).
    TransactionManagerAction txn_mgr(nullptr);
    EXPECT_FALSE(txn_mgr.need_auth());
    TransactionStreamLoadAction txn_load(nullptr);
    EXPECT_FALSE(txn_load.need_auth());
}

TEST(BeHandlerPrivilegeTest, web_page_handler_requires_OPERATE) {
    // BE Web UI is operator-debug surface — same policy as pprof / ioprofile.
    EvHttpServer server(/*port=*/0, /*num_workers=*/1);
    WebPageHandler handler(&server);
    EXPECT_EQ(Priv::OPERATE, handler.required_privilege());
}

} // namespace starrocks
