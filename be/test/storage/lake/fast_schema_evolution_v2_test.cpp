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

#include <gtest/gtest.h>

#include <array>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "base/testutil/sync_point.h"
#include "gen_cpp/FrontendService_types.h"
#include "runtime/exec_env.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log_applier.h"

namespace starrocks::lake {

using namespace starrocks;

namespace {

class ScopedSyncPoint {
public:
    ScopedSyncPoint() { SyncPoint::GetInstance()->EnableProcessing(); }
    ~ScopedSyncPoint() {
        SyncPoint::GetInstance()->ClearAllCallBacks();
        SyncPoint::GetInstance()->DisableProcessing();
    }
};

} // namespace

class FastSchemaEvolutionV2Test : public ::testing::Test {
public:
    void SetUp() override {
        _sp = std::make_unique<ScopedSyncPoint>();
        ASSERT_NE(ExecEnv::GetInstance(), nullptr);
        ASSERT_NE(ExecEnv::GetInstance()->lake_tablet_manager(), nullptr);
    }

    void TearDown() override { _sp.reset(); }

protected:
    struct RpcTestHookArgs {
        const TGetTableSchemaRequest* request = nullptr;
        TBatchGetTableSchemaResponse* response = nullptr;
        Status* rpc_status = nullptr;
        bool* mock_thrift_rpc = nullptr;
    };

    static void set_status(TStatus* status, TStatusCode::type code, std::string msg = "") {
        status->__set_status_code(code);
        if (!msg.empty()) {
            status->__set_error_msgs(std::vector<std::string>{std::move(msg)});
        }
    }

    static TTabletSchema make_thrift_schema(TKeysType::type keys_type, int64_t schema_id, int32_t schema_version,
                                            int num_columns) {
        TTabletSchema t_schema;
        t_schema.__set_id(schema_id);
        t_schema.__set_schema_version(schema_version);
        t_schema.__set_short_key_column_count(1);
        t_schema.__set_keys_type(keys_type);

        TColumn c0;
        c0.__set_column_name("c0");
        c0.column_type.__set_type(TPrimitiveType::INT);
        c0.__set_col_unique_id(1);
        c0.__set_is_key(true);
        c0.__set_is_allow_null(false);
        t_schema.columns.push_back(c0);

        TColumn c1;
        c1.__set_column_name("c1");
        c1.column_type.__set_type(TPrimitiveType::INT);
        c1.__set_col_unique_id(2);
        c1.__set_is_key(false);
        c1.__set_is_allow_null(false);
        t_schema.columns.push_back(c1);

        for (int i = 2; i < num_columns; i++) {
            TColumn c;
            c.__set_column_name("c" + std::to_string(i));
            c.column_type.__set_type(TPrimitiveType::INT);
            c.__set_col_unique_id(i + 1);
            c.__set_is_key(false);
            c.__set_is_allow_null(false);
            t_schema.columns.push_back(c);
        }

        return t_schema;
    }

    static RpcTestHookArgs unpack_rpc_test_hook_args(void* arg) {
        auto* arr = static_cast<std::array<void*, 4>*>(arg);
        RpcTestHookArgs out;
        out.request = static_cast<const TGetTableSchemaRequest*>((*arr)[0]);
        out.response = static_cast<TBatchGetTableSchemaResponse*>((*arr)[1]);
        out.rpc_status = static_cast<Status*>((*arr)[2]);
        out.mock_thrift_rpc = static_cast<bool*>((*arr)[3]);
        return out;
    }

    static void install_rpc_hook(const std::function<void(const RpcTestHookArgs&)>& hook) {
        SyncPoint::GetInstance()->SetCallBack("TableSchemaService::_fetch_schema_via_rpc::test_hook",
                                              [hook](void* arg) { hook(unpack_rpc_test_hook_args(arg)); });
    }

    static std::unique_ptr<TxnLogApplier> new_applier(int64_t tablet_id, const MutableTabletMetadataPtr& meta) {
        Tablet tablet(ExecEnv::GetInstance()->lake_tablet_manager(), tablet_id);
        return new_txn_log_applier(tablet, meta, /*new_version=*/2, /*rebuild_pindex=*/false,
                                   /*skip_write_tablet_metadata=*/true);
    }

    static void mock_schema_rpc(TKeysType::type keys_type, int64_t schema_id, int32_t schema_version, int num_columns) {
        install_rpc_hook([keys_type, schema_id, schema_version, num_columns](const RpcTestHookArgs& ctx) {
            *ctx.mock_thrift_rpc = true;
            *ctx.rpc_status = Status::OK();
            set_status(&ctx.response->status, TStatusCode::OK);
            ctx.response->__set_responses(std::vector<TGetTableSchemaResponse>{});
            auto& resp = ctx.response->responses.emplace_back();
            set_status(&resp.status, TStatusCode::OK);
            resp.__set_schema(make_thrift_schema(keys_type, schema_id, schema_version, num_columns));
        });
    }

    static MutableTabletMetadataPtr make_meta(int64_t tablet_id, KeysType keys_type, int64_t schema_id,
                                              int32_t schema_version) {
        auto m = std::make_shared<TabletMetadata>();
        m->set_id(tablet_id);
        m->set_version(1);
        m->set_next_rowset_id(0);

        auto* schema = m->mutable_schema();
        schema->set_id(schema_id);
        schema->set_keys_type(keys_type);
        schema->set_schema_version(schema_version);
        schema->set_num_short_key_columns(1);

        // Two-column base schema: {c0(key), c1(value)}.
        schema->set_next_column_unique_id(3);
        auto* c0 = schema->add_column();
        c0->set_unique_id(1);
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
        c0->set_length(4);

        auto* c1 = schema->add_column();
        c1->set_unique_id(2);
        c1->set_name("c1");
        c1->set_type("INT");
        c1->set_is_key(false);
        c1->set_is_nullable(false);
        c1->set_length(4);
        return m;
    }

    static TxnLogPB make_write_log(int64_t txn_id, int64_t tablet_id, int64_t schema_id, int64_t db_id,
                                   int64_t table_id) {
        TxnLogPB log;
        log.set_txn_id(txn_id);
        log.set_tablet_id(tablet_id);
        auto* op_write = log.mutable_op_write();
        auto* schema_key = op_write->mutable_schema_key();
        schema_key->set_schema_id(schema_id);
        schema_key->set_db_id(db_id);
        schema_key->set_table_id(table_id);
        return log;
    }

    static std::shared_ptr<TxnLogPB> make_write_log_ptr(int64_t txn_id, int64_t tablet_id, int64_t schema_id,
                                                        int64_t db_id, int64_t table_id) {
        auto log = std::make_shared<TxnLogPB>();
        log->set_txn_id(txn_id);
        log->set_tablet_id(tablet_id);
        auto* op_write = log->mutable_op_write();
        auto* schema_key = op_write->mutable_schema_key();
        schema_key->set_schema_id(schema_id);
        schema_key->set_db_id(db_id);
        schema_key->set_table_id(table_id);
        return log;
    }

    std::unique_ptr<ScopedSyncPoint> _sp;
};

TEST_F(FastSchemaEvolutionV2Test, no_schema_update) {
    // Verify update is skipped when schema_key is missing or schema_version is not newer.
    const int64_t kOldSchemaId = next_id();
    const int32_t kOldSchemaVersion = 10;

    {
        // 1) Prepare metadata with an existing schema in TabletMetadataPB::schema.
        auto tablet_id = next_id();
        auto meta = make_meta(tablet_id, DUP_KEYS, kOldSchemaId, kOldSchemaVersion);
        auto applier = new_applier(tablet_id, meta);

        // 2) Install a hook to detect whether schema fetch is attempted.
        bool invoked = false;
        install_rpc_hook([&](const RpcTestHookArgs&) { invoked = true; });

        // 3) Apply an op_write without schema_key.
        TxnLogPB log;
        log.set_txn_id(next_id());
        log.set_tablet_id(tablet_id);
        (void)log.mutable_op_write();

        // 4) Assert: schema unchanged, and no remote schema fetch attempted.
        auto st = applier->apply(log);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_FALSE(invoked);
        ASSERT_EQ(meta->schema().id(), kOldSchemaId);
        ASSERT_EQ(meta->schema().schema_version(), kOldSchemaVersion);
    }

    {
        // 1) Prepare metadata with an existing schema in TabletMetadataPB::schema.
        auto tablet_id = next_id();
        auto meta = make_meta(tablet_id, DUP_KEYS, kOldSchemaId, kOldSchemaVersion);
        auto applier = new_applier(tablet_id, meta);

        // 2) Install a hook to detect whether schema fetch is attempted.
        bool invoked = false;
        install_rpc_hook([&](const RpcTestHookArgs&) { invoked = true; });

        // 3) Apply an op_write with schema_key.schema_id == current.
        auto log = make_write_log(next_id(), tablet_id, /*schema_id=*/kOldSchemaId, /*db_id=*/1, /*table_id=*/2);

        // 4) Assert: schema unchanged, and no remote schema fetch attempted.
        auto st = applier->apply(log);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_FALSE(invoked);
        ASSERT_EQ(meta->schema().id(), kOldSchemaId);
        ASSERT_EQ(meta->schema().schema_version(), kOldSchemaVersion);
    }

    {
        // 1) Prepare metadata with two existing schema in TabletMetadataPB::schema and TabletMetadataPB::historical_schemas.
        auto tablet_id = next_id();
        auto meta = make_meta(tablet_id, DUP_KEYS, kOldSchemaId, kOldSchemaVersion);
        const int64_t historical_schema_id = kOldSchemaId - 1;
        TabletSchemaPB historical;
        historical.set_id(historical_schema_id);
        historical.set_schema_version(kOldSchemaVersion - 1);
        meta->mutable_historical_schemas()->insert({historical_schema_id, historical});
        auto applier = new_applier(tablet_id, meta);

        // 2) Install a hook to detect whether schema fetch is attempted.
        bool invoked = false;
        install_rpc_hook([&](const RpcTestHookArgs&) { invoked = true; });

        // 3) Apply an op_write whose schema_id already exists in historical_schemas.
        auto log =
                make_write_log(next_id(), tablet_id, /*schema_id=*/historical_schema_id, /*db_id=*/1, /*table_id=*/2);

        // 4) Assert: schema unchanged, and no remote schema fetch attempted.
        auto st = applier->apply(log);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_FALSE(invoked);
        ASSERT_EQ(meta->schema().id(), kOldSchemaId);
        ASSERT_EQ(meta->schema().schema_version(), kOldSchemaVersion);
        ASSERT_EQ(meta->historical_schemas().size(), 1);
    }

    {
        // 1) Prepare metadata with an existing schema TabletMetadataPB::schema.
        auto tablet_id = next_id();
        auto meta = make_meta(tablet_id, DUP_KEYS, kOldSchemaId, kOldSchemaVersion);
        auto applier = new_applier(tablet_id, meta);

        // 2) Mock FE schema RPC to return an older schema (id/version controlled by the test).
        const int64_t new_schema_id = next_id();
        mock_schema_rpc(TKeysType::DUP_KEYS, new_schema_id, /*schema_version=*/5, /*num_columns=*/3);

        // 3) Apply an op_write with schema_version < current.
        auto log = make_write_log(next_id(), tablet_id, /*schema_id=*/new_schema_id, /*db_id=*/1, /*table_id=*/2);

        // 4) Assert: schema unchanged, and no remote schema fetch attempted.
        auto st = applier->apply(log);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(meta->schema().id(), kOldSchemaId);
        ASSERT_EQ(meta->schema().schema_version(), kOldSchemaVersion);
    }
}

TEST_F(FastSchemaEvolutionV2Test, schema_update) {
    // Verify metadata schema is updated when schema_key.schema_version is newer.
    for (auto keys_type : {DUP_KEYS, PRIMARY_KEYS}) {
        SCOPED_TRACE(keys_type == DUP_KEYS ? "dup_keys" : "primary_keys");
        auto tablet_id = next_id();
        auto meta = make_meta(tablet_id, keys_type, /*schema_id=*/next_id(), /*schema_version=*/10);
        auto applier = new_applier(tablet_id, meta);

        // 1) Mock FE schema RPC to return a newer schema (id/version controlled by the test).
        const int64_t new_schema_id = next_id();
        mock_schema_rpc(keys_type == DUP_KEYS ? TKeysType::DUP_KEYS : TKeysType::PRIMARY_KEYS, new_schema_id,
                        /*schema_version=*/20, /*num_columns=*/3);

        // 2) Apply an op_write with schema version newer than metadata.
        auto log = make_write_log(next_id(), tablet_id, new_schema_id, /*db_id=*/1, /*table_id=*/2);

        // 3) Assert: metadata schema replaced by fetched schema.
        auto st = applier->apply(log);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(meta->schema().id(), new_schema_id);
        ASSERT_EQ(meta->schema().schema_version(), 20);
        ASSERT_EQ(meta->schema().column_size(), 3);
        ASSERT_EQ(meta->schema().column(2).name(), "c2");
    }
}

TEST_F(FastSchemaEvolutionV2Test, archive_to_history) {
    // Verify rowset_to_schema backfill and old schema is archived into historical_schemas.
    auto tablet_id = next_id();
    const int64_t kOldSchemaId = next_id();
    auto meta = make_meta(tablet_id, DUP_KEYS, kOldSchemaId, /*schema_version=*/10);
    auto* rs1 = meta->add_rowsets();
    rs1->set_id(111);
    auto* rs2 = meta->add_rowsets();
    rs2->set_id(222);
    TabletSchemaPB old_schema = meta->schema();

    // 1) Prepare applier and mock schema fetch.
    auto applier = new_applier(tablet_id, meta);

    const int64_t new_schema_id = next_id();
    mock_schema_rpc(TKeysType::DUP_KEYS, new_schema_id, /*schema_version=*/20, /*num_columns=*/3);

    // 2) Apply an op_write with a newer schema.
    auto log = make_write_log(next_id(), tablet_id, new_schema_id, /*db_id=*/1, /*table_id=*/2);
    auto st = applier->apply(log);
    ASSERT_TRUE(st.ok()) << st;

    // 3) Assert: missing rowset_to_schema mappings are backfilled to old schema id.
    auto it1 = meta->rowset_to_schema().find(111);
    ASSERT_TRUE(it1 != meta->rowset_to_schema().end());
    ASSERT_EQ(it1->second, kOldSchemaId);
    auto it2 = meta->rowset_to_schema().find(222);
    ASSERT_TRUE(it2 != meta->rowset_to_schema().end());
    ASSERT_EQ(it2->second, kOldSchemaId);
    // 4) Assert: old schema archived.
    auto hs_it = meta->historical_schemas().find(kOldSchemaId);
    ASSERT_TRUE(hs_it != meta->historical_schemas().end());
    ASSERT_EQ(hs_it->second.DebugString(), old_schema.DebugString());
}

TEST_F(FastSchemaEvolutionV2Test, no_archive_to_history) {
    // Verify historical_schemas is not modified when no backfill is needed.
    auto tablet_id = next_id();
    const int64_t kOldSchemaId = next_id();
    auto meta = make_meta(tablet_id, DUP_KEYS, kOldSchemaId, /*schema_version=*/10);
    auto* rs1 = meta->add_rowsets();
    rs1->set_id(111);
    auto* rs2 = meta->add_rowsets();
    rs2->set_id(222);
    meta->mutable_rowset_to_schema()->insert({111, kOldSchemaId});
    meta->mutable_rowset_to_schema()->insert({222, kOldSchemaId});
    TabletSchemaPB existing;
    existing.set_id(kOldSchemaId);
    existing.set_schema_version(777);
    meta->mutable_historical_schemas()->insert({kOldSchemaId, existing});

    // 1) Prepare applier and mock schema fetch.
    auto applier = new_applier(tablet_id, meta);

    const int64_t new_schema_id = next_id();
    mock_schema_rpc(TKeysType::DUP_KEYS, new_schema_id, /*schema_version=*/20, /*num_columns=*/3);

    // 2) Apply an op_write with a newer schema.
    auto log = make_write_log(next_id(), tablet_id, new_schema_id, /*db_id=*/1, /*table_id=*/2);
    auto st = applier->apply(log);
    ASSERT_TRUE(st.ok()) << st;
    // 3) Assert: existing historical schema entry remains unchanged.
    ASSERT_EQ(meta->historical_schemas().size(), 1);
    auto hs_it = meta->historical_schemas().find(kOldSchemaId);
    ASSERT_TRUE(hs_it != meta->historical_schemas().end());
    ASSERT_EQ(hs_it->second.schema_version(), 777);
}

TEST_F(FastSchemaEvolutionV2Test, apply_log_vector_updates_schema) {
    // Verify batch apply updates schema to the latest (newest schema_version among logs).
    for (auto keys_type : {DUP_KEYS, PRIMARY_KEYS}) {
        SCOPED_TRACE(keys_type == DUP_KEYS ? "dup_keys" : "primary_keys");
        auto tablet_id = next_id();
        const int64_t kOldSchemaId = next_id();
        auto meta = make_meta(tablet_id, keys_type, kOldSchemaId, /*schema_version=*/50);
        auto applier = new_applier(tablet_id, meta);

        // 1) Mock schema fetch: each schema version has a different schema id, and newer schemas add new columns.
        std::array<int32_t, 3> returned_versions{100, 101, 102};
        std::array<int64_t, 3> schema_ids{next_id(), next_id(), next_id()};
        std::unordered_map<int64_t, int32_t> schema_id_to_version;
        for (size_t i = 0; i < schema_ids.size(); i++) {
            schema_id_to_version.emplace(schema_ids[i], returned_versions[i]);
        }
        install_rpc_hook([&](const RpcTestHookArgs& ctx) {
            const int64_t req_schema_id = ctx.request->schema_key.schema_id;
            auto it = schema_id_to_version.find(req_schema_id);
            ASSERT_TRUE(it != schema_id_to_version.end()) << "unexpected schema_id: " << req_schema_id;
            *ctx.mock_thrift_rpc = true;
            *ctx.rpc_status = Status::OK();
            set_status(&ctx.response->status, TStatusCode::OK);
            ctx.response->__set_responses(std::vector<TGetTableSchemaResponse>{});
            auto& resp = ctx.response->responses.emplace_back();
            set_status(&resp.status, TStatusCode::OK);
            // version 100 => 2 cols, 101 => 3 cols, 102 => 4 cols
            resp.__set_schema(make_thrift_schema(keys_type == DUP_KEYS ? TKeysType::DUP_KEYS : TKeysType::PRIMARY_KEYS,
                                                 req_schema_id, it->second, /*num_columns=*/it->second - 98));
        });

        // 2) Apply a log vector with increasing schema_id.
        TxnLogVector logs;
        logs.push_back(make_write_log_ptr(next_id(), tablet_id, schema_ids[0], /*db_id=*/1, /*table_id=*/2));
        logs.push_back(make_write_log_ptr(next_id(), tablet_id, schema_ids[1], /*db_id=*/1, /*table_id=*/2));
        logs.push_back(make_write_log_ptr(next_id(), tablet_id, schema_ids[2], /*db_id=*/1, /*table_id=*/2));

        // 3) Assert: schema updated to latest.
        auto st = applier->apply(logs);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(meta->schema().id(), schema_ids[2]);
        ASSERT_EQ(meta->schema().schema_version(), 102);
        ASSERT_EQ(meta->schema().column_size(), 4);
        ASSERT_EQ(meta->schema().column(2).name(), "c2");
        ASSERT_EQ(meta->schema().column(3).name(), "c3");
    }
}

} // namespace starrocks::lake
