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

#include "storage/lake/table_schema_service.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "base/testutil/sync_point.h"
#include "common/config_lake_fwd.h"
#include "fs/fs_util.h"
#include "runtime/mem_tracker.h"
#include "storage/lake/filenames.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/update_manager.h"
#include "storage/metadata_util.h"
#include "storage/tablet_schema.h"

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

class ScopedConfig {
public:
    explicit ScopedConfig(int32_t* target, int32_t new_value) : _target(target), _old(*target) { *_target = new_value; }
    ~ScopedConfig() { *_target = _old; }

private:
    int32_t* _target;
    int32_t _old;
};

// SingleFlightBarrier provides a deterministic way to reproduce "leader + follower join" in singleflight::Group
// without relying on timing (e.g. sleep_for).
//
// How it works:
// - It hooks singleflight::Group::Do:2 (leader path, right before executing the real function) and blocks there.
// - It waits until the follower path hits singleflight::Group::Do:1 (join path, finding an in-flight future).
//
// Typical usage in a test:
// 1) SingleFlightBarrier barrier(timeout); barrier.install();
// 2) Start leader thread (first request)
// 3) ASSERT_TRUE(barrier.wait_for_leader_reached_exec_point());
// 4) Start follower thread (second request, same key)
// 5) Join threads; ASSERT_FALSE(barrier.timed_out());
class SingleFlightBarrier {
public:
    explicit SingleFlightBarrier(std::chrono::milliseconds timeout) : _timeout(timeout) {}

    void install() {
        SyncPoint::GetInstance()->SetCallBack("singleflight::Group::Do:1", [&](void*) {
            std::lock_guard lk(_mu);
            _follower_seen++;
            _cv.notify_all();
        });
        SyncPoint::GetInstance()->SetCallBack("singleflight::Group::Do:2", [&](void*) {
            std::unique_lock lk(_mu);
            if (!_leader_blocked_once) {
                _leader_blocked_once = true;
                _leader_reached_exec_point = true;
                _cv.notify_all();
                const bool ok = _cv.wait_for(lk, _timeout, [&] { return _follower_seen >= 1; });
                if (!ok) {
                    _timed_out = true;
                    _cv.notify_all();
                }
            }
        });
    }

    // Wait until the leader thread reaches the hook point right before executing the real function.
    bool wait_for_leader_reached_exec_point() {
        std::unique_lock lk(_mu);
        return _cv.wait_for(lk, _timeout, [&] { return _leader_reached_exec_point; });
    }

    bool timed_out() const {
        std::lock_guard lk(_mu);
        return _timed_out;
    }

private:
    const std::chrono::milliseconds _timeout;
    mutable std::mutex _mu;
    mutable std::condition_variable _cv;
    int _follower_seen = 0;
    bool _leader_blocked_once = false;
    bool _leader_reached_exec_point = false;
    bool _timed_out = false;
};

} // namespace

class TableSchemaServiceTest : public testing::Test {
public:
    TableSchemaServiceTest() : _test_directory("test_table_schema_service") {
        _mem_tracker = std::make_unique<MemTracker>(1024 * 1024);
        _location_provider = std::make_shared<FixedLocationProvider>(_test_directory);
        _update_manager = std::make_unique<UpdateManager>(_location_provider, _mem_tracker.get());
        _tablet_manager = std::make_unique<TabletManager>(_location_provider, _update_manager.get(), 1024 * 1024);
        _schema_service = _tablet_manager->table_schema_service();
    }

    void SetUp() override { clear_and_init_test_dir(); }

    void TearDown() override { remove_test_dir_ignore_error(); }

protected:
    struct RpcHookArgs {
        const TGetTableSchemaRequest* request = nullptr;
        TBatchGetTableSchemaResponse* response = nullptr;
        Status* status = nullptr;
        bool* mock_thrift_rpc = nullptr;
    };

    static TTabletSchema make_thrift_schema(int64_t schema_id) {
        TTabletSchema schema;
        schema.__set_id(schema_id);
        schema.__set_short_key_column_count(1);
        schema.__set_keys_type(TKeysType::DUP_KEYS);

        TColumn c0;
        c0.__set_column_name("c0");
        c0.column_type.__set_type(TPrimitiveType::INT);
        c0.__set_is_key(true);
        c0.__set_is_allow_null(false);
        schema.columns.push_back(c0);

        TColumn c1;
        c1.__set_column_name("c1");
        c1.column_type.__set_type(TPrimitiveType::INT);
        c1.__set_is_key(false);
        c1.__set_is_allow_null(false);
        schema.columns.push_back(c1);

        return schema;
    }

    static void set_status(TStatus* status, TStatusCode::type code, std::string msg = "") {
        status->__set_status_code(code);
        if (!msg.empty()) {
            status->__set_error_msgs(std::vector<std::string>{std::move(msg)});
        }
    }

    static TabletSchemaPB make_schema_pb(int64_t schema_id, int32_t schema_version) {
        TabletSchemaPB schema_pb;
        schema_pb.set_id(schema_id);
        schema_pb.set_num_short_key_columns(1);
        schema_pb.set_keys_type(DUP_KEYS);
        schema_pb.set_num_rows_per_row_block(65535);
        schema_pb.set_schema_version(schema_version);

        auto c0 = schema_pb.add_column();
        c0->set_unique_id(next_id());
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);

        auto c1 = schema_pb.add_column();
        c1->set_unique_id(next_id());
        c1->set_name("c1");
        c1->set_type("INT");
        c1->set_is_key(false);
        c1->set_is_nullable(false);
        return schema_pb;
    }

    static RpcHookArgs unpack_rpc_hook_args(void* arg) {
        auto* arr = static_cast<std::array<void*, 4>*>(arg);
        RpcHookArgs out;
        out.request = static_cast<const TGetTableSchemaRequest*>((*arr)[0]);
        out.response = static_cast<TBatchGetTableSchemaResponse*>((*arr)[1]);
        out.status = static_cast<Status*>((*arr)[2]);
        out.mock_thrift_rpc = static_cast<bool*>((*arr)[3]);
        return out;
    }

    struct InitialMetadataRpcHookArgs {
        const TGetTabletMetadataRequest* request = nullptr;
        TBatchGetTabletMetadataResponse* response = nullptr;
        Status* status = nullptr;
        bool* mock_thrift_rpc = nullptr;
    };

    static InitialMetadataRpcHookArgs unpack_initial_metadata_hook_args(void* arg) {
        auto* arr = static_cast<std::array<void*, 4>*>(arg);
        InitialMetadataRpcHookArgs out;
        out.request = static_cast<const TGetTabletMetadataRequest*>((*arr)[0]);
        out.response = static_cast<TBatchGetTabletMetadataResponse*>((*arr)[1]);
        out.status = static_cast<Status*>((*arr)[2]);
        out.mock_thrift_rpc = static_cast<bool*>((*arr)[3]);
        return out;
    }
    void clear_and_init_test_dir() {
        (void)fs::remove_all(_test_directory);
        CHECK_OK(fs::create_directories(join_path(_test_directory, kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(join_path(_test_directory, kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(join_path(_test_directory, kTxnLogDirectoryName)));
    }

    void remove_test_dir_ignore_error() { (void)fs::remove_all(_test_directory); }

    TabletSchemaPtr create_test_schema(int64_t schema_id) { return TabletSchema::create(make_schema_pb(schema_id, 1)); }

    MutableTabletMetadataPtr create_tablet_metadata(int64_t tablet_id, int64_t schema_id) {
        auto metadata = std::make_shared<TabletMetadata>();
        metadata->set_id(tablet_id);
        metadata->set_version(1);

        metadata->mutable_schema()->CopyFrom(make_schema_pb(schema_id, 1));

        return metadata;
    }

    MutableTabletMetadataPtr create_tablet_metadata_with_historical_schema(int64_t tablet_id, int64_t current_schema_id,
                                                                           int64_t historical_schema_id) {
        auto metadata = create_tablet_metadata(tablet_id, current_schema_id);

        auto historical_schema = make_schema_pb(historical_schema_id, 0);
        metadata->mutable_historical_schemas()->insert({historical_schema_id, historical_schema});

        return metadata;
    }

    TableSchemaKeyPB create_schema_info(int64_t schema_id, int64_t db_id, int64_t table_id) {
        TableSchemaKeyPB schema_key;
        schema_key.set_schema_id(schema_id);
        schema_key.set_db_id(db_id);
        schema_key.set_table_id(table_id);
        return schema_key;
    }

    TUniqueId create_query_id() {
        TUniqueId query_id;
        query_id.hi = next_id();
        query_id.lo = next_id();
        return query_id;
    }

    TNetworkAddress create_fe_address() {
        TNetworkAddress fe;
        fe.hostname = "127.0.0.1";
        fe.port = 9020;
        return fe;
    }

    void setup_rpc_test_hook(std::function<void(const RpcHookArgs&)> hook) {
        SyncPoint::GetInstance()->SetCallBack(
                "TableSchemaService::_fetch_schema_via_rpc::test_hook",
                [hook](void* arg) { hook(TableSchemaServiceTest::unpack_rpc_hook_args(arg)); });
    }

    void setup_rpc_success_schema(int64_t return_schema_id, std::atomic<int>* rpc_calls = nullptr) {
        setup_rpc_test_hook([return_schema_id, rpc_calls](const RpcHookArgs& ctx) {
            if (rpc_calls != nullptr) {
                rpc_calls->fetch_add(1);
            }
            *ctx.mock_thrift_rpc = true;
            *ctx.status = Status::OK();
            TableSchemaServiceTest::set_status(&ctx.response->status, TStatusCode::OK);
            ctx.response->__set_responses(std::vector<TGetTableSchemaResponse>{});
            auto& resp = ctx.response->responses.emplace_back();
            TableSchemaServiceTest::set_status(&resp.status, TStatusCode::OK);
            resp.__set_schema(TableSchemaServiceTest::make_thrift_schema(return_schema_id));
        });
    }

    void setup_rpc_expect_request_and_return_schema(std::function<void(const TGetTableSchemaRequest&)> expect,
                                                    int64_t return_schema_id) {
        setup_rpc_test_hook([expect = std::move(expect), return_schema_id](const RpcHookArgs& ctx) {
            if (expect != nullptr) {
                expect(*ctx.request);
            }
            *ctx.mock_thrift_rpc = true;
            *ctx.status = Status::OK();
            TableSchemaServiceTest::set_status(&ctx.response->status, TStatusCode::OK);
            ctx.response->__set_responses(std::vector<TGetTableSchemaResponse>{});
            auto& resp = ctx.response->responses.emplace_back();
            TableSchemaServiceTest::set_status(&resp.status, TStatusCode::OK);
            resp.__set_schema(TableSchemaServiceTest::make_thrift_schema(return_schema_id));
        });
    }

    void mock_tablet_schema_by_id(StatusOr<std::shared_ptr<const TabletSchema>> schema_or) {
        SyncPoint::GetInstance()->SetCallBack("get_tablet_schema_by_id.1",
                                              [](void* arg) { ((std::shared_ptr<const TabletSchema>*)arg)->reset(); });
        SyncPoint::GetInstance()->SetCallBack("get_tablet_schema_by_id.2",
                                              [schema_or = std::move(schema_or)](void* arg) {
                                                  *((StatusOr<std::shared_ptr<const TabletSchema>>*)arg) = schema_or;
                                              });
    }

    void setup_rpc_batch_status_error(TStatusCode::type code) {
        setup_rpc_test_hook([code](const RpcHookArgs& ctx) {
            *ctx.mock_thrift_rpc = true;
            *ctx.status = Status::OK();
            TableSchemaServiceTest::set_status(&ctx.response->status, code, "mocked batch status error");
            ctx.response->__set_responses(std::vector<TGetTableSchemaResponse>{});
        });
    }

    void setup_rpc_empty_responses() {
        setup_rpc_test_hook([](const RpcHookArgs& ctx) {
            *ctx.mock_thrift_rpc = true;
            *ctx.status = Status::OK();
            TableSchemaServiceTest::set_status(&ctx.response->status, TStatusCode::OK);
            ctx.response->__set_responses(std::vector<TGetTableSchemaResponse>{});
        });
    }

    void setup_rpc_response_status_error(TStatusCode::type code) {
        setup_rpc_test_hook([code](const RpcHookArgs& ctx) {
            *ctx.mock_thrift_rpc = true;
            *ctx.status = Status::OK();
            TableSchemaServiceTest::set_status(&ctx.response->status, TStatusCode::OK);
            ctx.response->__set_responses(std::vector<TGetTableSchemaResponse>{});
            auto& resp = ctx.response->responses.emplace_back();
            TableSchemaServiceTest::set_status(&resp.status, code, "mocked response status error");
        });
    }

    void setup_rpc_method_not_found() {
        setup_rpc_test_hook([](const RpcHookArgs& ctx) {
            *ctx.mock_thrift_rpc = true;
            *ctx.status = Status::ThriftRpcError("Invalid method name 'getTableSchema'");
        });
    }

    void setup_skip_construct_initial_metadata() {
        SyncPoint::GetInstance()->SetCallBack("TabletManager::construct_initial_metadata", [](void* arg) {
            // Return NOT_FOUND to simulate tablet not existing, because construct_initial_metadata
            // requires g_worker (StarOS) which is unavailable in unit tests.
            *static_cast<Status*>(arg) = Status::NotFound("skipped by test");
        });
    }

    void setup_rpc_other_thrift_error() {
        setup_rpc_test_hook([](const RpcHookArgs& ctx) {
            *ctx.mock_thrift_rpc = true;
            *ctx.status = Status::ThriftRpcError("mocked thrift rpc error");
        });
    }

    void setup_rpc_table_not_exist() {
        setup_rpc_test_hook([](const RpcHookArgs& ctx) {
            *ctx.mock_thrift_rpc = true;
            *ctx.status = Status::OK();
            TableSchemaServiceTest::set_status(&ctx.response->status, TStatusCode::OK);
            ctx.response->__set_responses(std::vector<TGetTableSchemaResponse>{});
            auto& resp = ctx.response->responses.emplace_back();
            TableSchemaServiceTest::set_status(&resp.status, TStatusCode::TABLE_NOT_EXIST, "mocked table not exist");
        });
    }

    std::string _test_directory;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::shared_ptr<FixedLocationProvider> _location_provider;
    std::unique_ptr<UpdateManager> _update_manager;
    std::unique_ptr<TabletManager> _tablet_manager;
    TableSchemaService* _schema_service;
};

TEST_F(TableSchemaServiceTest, schema_cache_hit) {
    // 1) Put schema into global cache.
    // 2) Verify both LOAD and SCAN return from cache without RPC.
    int64_t schema_id = next_id();
    auto schema = create_test_schema(schema_id);
    _tablet_manager->cache_schema(schema);

    // LOAD path should hit global cache.
    auto load_result = _schema_service->get_schema_for_load(create_schema_info(schema_id, 100, 101), 1000, 1);
    ASSERT_OK(load_result);
    ASSERT_EQ(load_result.value()->id(), schema_id);

    // SCAN path should hit global cache.
    auto scan_result = _schema_service->get_schema_for_scan(create_schema_info(schema_id, 100, 101), 1000,
                                                            create_query_id(), create_fe_address());
    ASSERT_OK(scan_result);
    ASSERT_EQ(scan_result.value()->id(), schema_id);
}

TEST_F(TableSchemaServiceTest, tablet_metadata_hit) {
    // 1) Put tablet metadata with both current and historical schemas.
    // 2) Fetch schemas by id and verify they are cached into global cache.
    int64_t tablet_id = next_id();
    int64_t current_schema_id = next_id();
    int64_t historical_schema_id = next_id();
    auto metadata = create_tablet_metadata_with_historical_schema(tablet_id, current_schema_id, historical_schema_id);
    ASSERT_OK(_tablet_manager->put_tablet_metadata(*metadata));

    {
        SCOPED_TRACE("current_schema");
        auto result = _schema_service->get_schema_for_load(create_schema_info(current_schema_id, 100, 101), tablet_id,
                                                           1, metadata);
        ASSERT_OK(result);
        ASSERT_EQ(result.value()->id(), current_schema_id);
        auto cached_schema = _tablet_manager->get_cached_schema(current_schema_id);
        ASSERT_NE(cached_schema, nullptr);
        ASSERT_EQ(cached_schema->id(), current_schema_id);
    }

    {
        SCOPED_TRACE("historical_schema");
        auto result = _schema_service->get_schema_for_load(create_schema_info(historical_schema_id, 100, 101),
                                                           tablet_id, 1, metadata);
        ASSERT_OK(result);
        ASSERT_EQ(result.value()->id(), historical_schema_id);
        auto cached_schema = _tablet_manager->get_cached_schema(historical_schema_id);
        ASSERT_NE(cached_schema, nullptr);
        ASSERT_EQ(cached_schema->id(), historical_schema_id);
    }
}

TEST_F(TableSchemaServiceTest, remote_hit) {
    // 1) Force remote RPC success.
    // 2) Verify returned schema is cached into global cache.
    ScopedSyncPoint sp;
    int64_t schema_id = next_id();
    setup_rpc_success_schema(schema_id);

    auto result = _schema_service->get_schema_for_load(create_schema_info(schema_id, 100, 101), 1000, 1);
    ASSERT_OK(result);
    ASSERT_EQ(result.value()->id(), schema_id);

    auto cached_schema = _tablet_manager->get_cached_schema(schema_id);
    ASSERT_NE(cached_schema, nullptr);
    ASSERT_EQ(cached_schema->id(), schema_id);
}

TEST_F(TableSchemaServiceTest, rpc_request_fields_verify) {
    // Verify generated thrift request fields for both LOAD and SCAN paths.
    // 1) LOAD: trigger schema fetch; validate request fields; return OK schema.
    {
        ScopedSyncPoint sp;
        SCOPED_TRACE("load");
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        int64_t txn_id = next_id();
        std::atomic<bool> invoked{false};
        setup_rpc_expect_request_and_return_schema(
                [&](const TGetTableSchemaRequest& req) {
                    invoked.store(true);
                    ASSERT_EQ(req.source, TTableSchemaRequestSource::LOAD);
                    ASSERT_EQ(req.txn_id, txn_id);
                    ASSERT_EQ(req.tablet_id, tablet_id);
                    ASSERT_EQ(req.schema_key.schema_id, schema_id);
                    ASSERT_EQ(req.schema_key.db_id, 100);
                    ASSERT_EQ(req.schema_key.table_id, 101);
                },
                schema_id);

        auto result = _schema_service->get_schema_for_load(create_schema_info(schema_id, 100, 101), tablet_id, txn_id);
        ASSERT_TRUE(invoked.load());
        ASSERT_OK(result);
    }

    // 2) SCAN: trigger schema fetch; validate request fields; return OK schema.
    {
        ScopedSyncPoint sp;
        SCOPED_TRACE("scan");
        int64_t schema_id = next_id();
        int64_t tablet_id = next_id();
        auto query_id = create_query_id();
        std::atomic<bool> invoked{false};
        setup_rpc_expect_request_and_return_schema(
                [&](const TGetTableSchemaRequest& req) {
                    invoked.store(true);
                    ASSERT_EQ(req.source, TTableSchemaRequestSource::SCAN);
                    ASSERT_EQ(req.tablet_id, tablet_id);
                    ASSERT_EQ(req.query_id, query_id);
                    ASSERT_EQ(req.schema_key.schema_id, schema_id);
                    ASSERT_EQ(req.schema_key.db_id, 100);
                    ASSERT_EQ(req.schema_key.table_id, 101);
                },
                schema_id);

        auto result = _schema_service->get_schema_for_scan(create_schema_info(schema_id, 100, 101), tablet_id, query_id,
                                                           create_fe_address(), nullptr);
        ASSERT_TRUE(invoked.load());
        ASSERT_OK(result);
    }
}

TEST_F(TableSchemaServiceTest, remote_error_handling) {
    // Force different remote error responses and verify propagated Status.
    struct Case {
        const char* name;
        std::function<void(TableSchemaServiceTest*)> setup;
        std::function<void(const Status&)> check;
    };

    const std::vector<Case> cases{
            {"method_not_found_maps_to_not_supported",
             [](TableSchemaServiceTest* t) {
                 t->setup_rpc_method_not_found();
                 t->setup_skip_construct_initial_metadata();
             },
             [](const Status& st) { ASSERT_TRUE(st.is_not_found()) << st; }},
            {"other_thrift_rpc_error_fail_fast", [](TableSchemaServiceTest* t) { t->setup_rpc_other_thrift_error(); },
             [](const Status& st) { ASSERT_TRUE(st.is_thrift_rpc_error()) << st; }},
            {"table_not_exist_fail_fast", [](TableSchemaServiceTest* t) { t->setup_rpc_table_not_exist(); },
             [](const Status& st) { ASSERT_TRUE(st.is_table_not_exist()) << st; }},
            {"batch_status_not_ok_propagates",
             [](TableSchemaServiceTest* t) { t->setup_rpc_batch_status_error(TStatusCode::INTERNAL_ERROR); },
             [](const Status& st) { ASSERT_TRUE(st.is_internal_error()) << st; }},
            {"empty_responses_returns_internal_error",
             [](TableSchemaServiceTest* t) { t->setup_rpc_empty_responses(); },
             [](const Status& st) {
                 ASSERT_TRUE(st.is_internal_error()) << st;
                 ASSERT_TRUE(st.message().find("response is empty") != std::string::npos) << st;
             }},
            {"response_status_not_ok_propagates",
             [](TableSchemaServiceTest* t) { t->setup_rpc_response_status_error(TStatusCode::INTERNAL_ERROR); },
             [](const Status& st) { ASSERT_TRUE(st.is_internal_error()) << st; }},
    };

    for (const auto& c : cases) {
        // Each sub-case runs in its own SyncPoint scope to avoid cross-test interference.
        ScopedSyncPoint sp;
        SCOPED_TRACE(c.name);
        int64_t schema_id = next_id();
        c.setup(this);
        // Trigger LOAD path to exercise _get_remote_schema() error handling.
        auto result = _schema_service->get_schema_for_load(create_schema_info(schema_id, 100, 101), 1000, 1);
        c.check(result.status());
    }
}

TEST_F(TableSchemaServiceTest, load_fallback_to_schema_file) {
    // Verify LOAD falls back to schema-file path when FE does not support getTableSchema.
    // Case 1) schema-file lookup succeeds: LOAD should return schema from schema-file path.
    {
        ScopedSyncPoint sp;
        SCOPED_TRACE("schema_file_success");
        int64_t tablet_id = next_id();
        int64_t schema_id = next_id();
        auto schema = create_test_schema(schema_id);

        auto metadata = create_tablet_metadata(tablet_id, schema_id);
        ASSERT_OK(_tablet_manager->put_tablet_metadata(*metadata));
        ASSERT_OK(_tablet_manager->get_tablet(tablet_id));

        setup_rpc_method_not_found();

        // Force schema-file path to "hit" via TabletManager sync point.
        mock_tablet_schema_by_id(schema);

        auto result = _schema_service->get_schema_for_load(create_schema_info(schema_id, 100, 101), tablet_id, 1);
        ASSERT_OK(result);
        ASSERT_EQ(result.value()->id(), schema_id);
    }

    // Case 2) schema-file lookup returns non-NotFound error: the error should be propagated.
    {
        ScopedSyncPoint sp;
        SCOPED_TRACE("schema_file_error_propagated");
        int64_t tablet_id = next_id();
        int64_t schema_id = next_id();
        auto metadata = create_tablet_metadata(tablet_id, schema_id);
        ASSERT_OK(_tablet_manager->put_tablet_metadata(*metadata));

        setup_rpc_method_not_found();
        mock_tablet_schema_by_id(Status::InternalError("mocked"));

        auto result = _schema_service->get_schema_for_load(create_schema_info(schema_id, 100, 101), tablet_id, 1);
        ASSERT_TRUE(result.status().is_internal_error());
    }
}

TEST_F(TableSchemaServiceTest, load_fallback_to_tablet_schema) {
    // Verify LOAD falls back to tablet's current schema when schema-file path is missing (NotFound).
    // Case 1) tablet metadata is provided: LOAD should return tablet's current schema.
    {
        ScopedSyncPoint sp;
        SCOPED_TRACE("tablet_schema_fallback_success");
        int64_t tablet_id = next_id();
        int64_t schema_id = next_id();
        auto metadata = create_tablet_metadata(tablet_id, schema_id);
        ASSERT_OK(_tablet_manager->put_tablet_metadata(*metadata));
        ASSERT_OK(_tablet_manager->get_tablet(tablet_id));

        setup_rpc_method_not_found();
        mock_tablet_schema_by_id(Status::NotFound("mocked not found"));

        auto result =
                _schema_service->get_schema_for_load(create_schema_info(schema_id, 100, 101), tablet_id, 1, metadata);
        ASSERT_OK(result);
        ASSERT_EQ(result.value()->id(), schema_id);
    }

    // Case 2) tablet metadata/tablet object is missing: return NotFound.
    {
        ScopedSyncPoint sp;
        SCOPED_TRACE("tablet_missing_returns_not_found");
        int64_t tablet_id = next_id();
        int64_t schema_id = next_id();

        setup_rpc_method_not_found();
        mock_tablet_schema_by_id(Status::NotFound("mocked not found"));
        // Mock cn-free fallback RPC to return NOT_FOUND, otherwise it tries real RPC
        setup_skip_construct_initial_metadata();

        auto result = _schema_service->get_schema_for_load(create_schema_info(schema_id, 100, 101), tablet_id, 1);
        ASSERT_TRUE(result.status().is_not_found());
    }
}

TEST_F(TableSchemaServiceTest, rpc_request_deduplication) {
    // 1) Two concurrent LOAD requests for same schema should share one in-flight future.
    // 2) Verify only one RPC hook is executed.
    ScopedSyncPoint sp;
    ScopedConfig retry_guard(&config::table_schema_service_max_retries, 3);

    std::atomic<int> rpc_call_count{0};
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    SingleFlightBarrier barrier(std::chrono::minutes(1));
    barrier.install();

    setup_rpc_test_hook([&](const TableSchemaServiceTest::RpcHookArgs& ctx) {
        rpc_call_count.fetch_add(1);
        *ctx.mock_thrift_rpc = true;
        *ctx.status = Status::OK();
        TableSchemaServiceTest::set_status(&ctx.response->status, TStatusCode::OK);
        ctx.response->__set_responses(std::vector<TGetTableSchemaResponse>{});
        auto& resp = ctx.response->responses.emplace_back();
        TableSchemaServiceTest::set_status(&resp.status, TStatusCode::OK);
        resp.__set_schema(TableSchemaServiceTest::make_thrift_schema(schema_id));
    });

    auto worker = [&](int64_t txn_id, StatusOr<TabletSchemaPtr>* out) {
        *out = _schema_service->get_schema_for_load(create_schema_info(schema_id, 100, 101), tablet_id, txn_id);
    };

    StatusOr<TabletSchemaPtr> r1;
    StatusOr<TabletSchemaPtr> r2;
    std::thread t1(worker, next_id(), &r1);
    ASSERT_TRUE(barrier.wait_for_leader_reached_exec_point())
            << "Timed out waiting for leader to reach singleflight::Group::Do:2";
    std::thread t2(worker, next_id(), &r2);
    t1.join();
    t2.join();

    ASSERT_FALSE(barrier.timed_out()) << "Timed out waiting for follower to join singleflight group";
    ASSERT_OK(r1);
    ASSERT_OK(r2);
    ASSERT_EQ(rpc_call_count.load(), 1);
}

TEST_F(TableSchemaServiceTest, load_interference) {
    // 1) txn_a becomes leader and returns a retryable error.
    // 2) txn_b joins the in-flight future, sees failure from different txn, then retries and succeeds.
    ScopedSyncPoint sp;
    ScopedConfig retry_guard(&config::table_schema_service_max_retries, 3);

    std::atomic<int> rpc_call_count{0};
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();

    int64_t txn_a = next_id();
    int64_t txn_b = next_id();
    StatusOr<TabletSchemaPtr> ra;
    StatusOr<TabletSchemaPtr> rb;

    SingleFlightBarrier barrier(std::chrono::minutes(1));
    barrier.install();

    std::atomic<bool> leader_failed_once{false};
    setup_rpc_test_hook([&](const TableSchemaServiceTest::RpcHookArgs& ctx) {
        rpc_call_count.fetch_add(1);
        *ctx.mock_thrift_rpc = true;
        *ctx.status = Status::OK();
        TableSchemaServiceTest::set_status(&ctx.response->status, TStatusCode::OK);
        ctx.response->__set_responses(std::vector<TGetTableSchemaResponse>{});
        auto& resp = ctx.response->responses.emplace_back();

        if (ctx.request->txn_id == txn_a && !leader_failed_once.exchange(true)) {
            // Make the leader fail once with a retryable error; follower (txn_b) should retry and succeed.
            TableSchemaServiceTest::set_status(&resp.status, TStatusCode::INTERNAL_ERROR, "mocked retryable error");
            return;
        }
        TableSchemaServiceTest::set_status(&resp.status, TStatusCode::OK);
        resp.__set_schema(TableSchemaServiceTest::make_thrift_schema(schema_id));
    });

    auto call_load = [&](int64_t txn_id, StatusOr<TabletSchemaPtr>* out) {
        *out = _schema_service->get_schema_for_load(create_schema_info(schema_id, 100, 101), tablet_id, txn_id);
    };

    std::thread t1(call_load, txn_a, &ra);
    ASSERT_TRUE(barrier.wait_for_leader_reached_exec_point())
            << "Timed out waiting for leader to reach singleflight::Group::Do:2";
    std::thread t2(call_load, txn_b, &rb);
    t1.join();
    t2.join();

    ASSERT_FALSE(barrier.timed_out()) << "Timed out waiting for follower to join singleflight group";
    // One of them is expected to be the leader and return error without retry (same txn as exec ctx),
    // while the other should retry and succeed.
    ASSERT_TRUE((ra.ok() && !rb.ok()) || (!ra.ok() && rb.ok()));
    ASSERT_GE(rpc_call_count.load(), 2);
}

TEST_F(TableSchemaServiceTest, query_interference) {
    // 1) query q1 becomes leader and returns a retryable error.
    // 2) query q2 joins, sees failure from different query, then retries and succeeds.
    ScopedSyncPoint sp;
    ScopedConfig retry_guard(&config::table_schema_service_max_retries, 3);

    std::atomic<int> rpc_call_count{0};
    int64_t schema_id = next_id();
    int64_t tablet_id = next_id();
    auto fe = create_fe_address();

    auto q1 = create_query_id();
    auto q2 = create_query_id();
    StatusOr<TabletSchemaPtr> r1;
    StatusOr<TabletSchemaPtr> r2;

    SingleFlightBarrier barrier(std::chrono::minutes(1));
    barrier.install();

    std::atomic<bool> leader_failed_once{false};
    setup_rpc_test_hook([&](const TableSchemaServiceTest::RpcHookArgs& ctx) {
        rpc_call_count.fetch_add(1);
        *ctx.mock_thrift_rpc = true;
        *ctx.status = Status::OK();
        TableSchemaServiceTest::set_status(&ctx.response->status, TStatusCode::OK);
        ctx.response->__set_responses(std::vector<TGetTableSchemaResponse>{});
        auto& resp = ctx.response->responses.emplace_back();

        if (ctx.request->query_id == q1 && !leader_failed_once.exchange(true)) {
            TableSchemaServiceTest::set_status(&resp.status, TStatusCode::INTERNAL_ERROR, "mocked retryable error");
            return;
        }
        TableSchemaServiceTest::set_status(&resp.status, TStatusCode::OK);
        resp.__set_schema(TableSchemaServiceTest::make_thrift_schema(schema_id));
    });

    auto call_scan = [&](const TUniqueId& q, StatusOr<TabletSchemaPtr>* out) {
        *out = _schema_service->get_schema_for_scan(create_schema_info(schema_id, 100, 101), tablet_id, q, fe, nullptr);
    };

    std::thread t1(call_scan, q1, &r1);
    ASSERT_TRUE(barrier.wait_for_leader_reached_exec_point())
            << "Timed out waiting for leader to reach singleflight::Group::Do:2";
    std::thread t2(call_scan, q2, &r2);
    t1.join();
    t2.join();

    ASSERT_FALSE(barrier.timed_out()) << "Timed out waiting for follower to join singleflight group";
    ASSERT_TRUE((r1.ok() && !r2.ok()) || (!r1.ok() && r2.ok()));
    ASSERT_GE(rpc_call_count.load(), 2);
}

TEST_F(TableSchemaServiceTest, get_tablet_initial_metadata_success) {
    ScopedSyncPoint sync_point;
    auto schema_id = next_id();

    SyncPoint::GetInstance()->SetCallBack(
            "TableSchemaService::_fetch_initial_metadata_via_rpc::test_hook", [&](void* arg) {
                auto ctx = unpack_initial_metadata_hook_args(arg);
                *ctx.mock_thrift_rpc = true;
                *ctx.status = Status::OK();

                TableSchemaServiceTest::set_status(&ctx.response->status, TStatusCode::OK);
                ctx.response->__set_responses(std::vector<TGetTabletMetadataResponse>{});
                auto& resp = ctx.response->responses.emplace_back();
                TableSchemaServiceTest::set_status(&resp.status, TStatusCode::OK);
                TCloudTabletMeta meta;
                meta.__set_tablet_id(ctx.request->tablet_id);
                meta.__set_schema(TableSchemaServiceTest::make_thrift_schema(schema_id));
                meta.__set_enable_persistent_index(true);
                meta.__set_persistent_index_type(TPersistentIndexType::CLOUD_NATIVE);
                meta.__set_compaction_strategy(TCompactionStrategy::REAL_TIME);
                meta.__set_compression_type(TCompressionType::LZ4_FRAME);
                meta.__set_compression_level(3);
                meta.__set_gtid(12345);
                resp.__set_meta(meta);
            });

    auto tablet_id = next_id();
    auto table_id = next_id();
    auto partition_id = next_id();
    auto index_id = next_id();
    auto result = _schema_service->get_tablet_initial_metadata(tablet_id, table_id, partition_id, index_id);
    ASSERT_TRUE(result.ok());

    auto& resp = result.value();
    ASSERT_TRUE(resp.__isset.meta);
    ASSERT_TRUE(resp.meta.__isset.schema);
    ASSERT_EQ(schema_id, resp.meta.schema.id);
    ASSERT_TRUE(resp.meta.enable_persistent_index);
    ASSERT_EQ(TPersistentIndexType::CLOUD_NATIVE, resp.meta.persistent_index_type);
    ASSERT_EQ(TCompactionStrategy::REAL_TIME, resp.meta.compaction_strategy);
    ASSERT_EQ(TCompressionType::LZ4_FRAME, resp.meta.compression_type);
    ASSERT_EQ(3, resp.meta.compression_level);
    ASSERT_EQ(12345, resp.meta.gtid);
}

TEST_F(TableSchemaServiceTest, get_tablet_initial_metadata_not_found) {
    ScopedSyncPoint sync_point;

    SyncPoint::GetInstance()->SetCallBack(
            "TableSchemaService::_fetch_initial_metadata_via_rpc::test_hook", [&](void* arg) {
                auto ctx = unpack_initial_metadata_hook_args(arg);
                *ctx.mock_thrift_rpc = true;
                *ctx.status = Status::OK();

                TableSchemaServiceTest::set_status(&ctx.response->status, TStatusCode::OK);
                ctx.response->__set_responses(std::vector<TGetTabletMetadataResponse>{});
                auto& resp = ctx.response->responses.emplace_back();
                TableSchemaServiceTest::set_status(&resp.status, TStatusCode::NOT_FOUND, "tablet not found");
            });

    auto result = _schema_service->get_tablet_initial_metadata(next_id(), next_id(), next_id(), next_id());
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().is_not_found());
}

TEST_F(TableSchemaServiceTest, get_tablet_initial_metadata_rpc_error) {
    ScopedSyncPoint sync_point;

    SyncPoint::GetInstance()->SetCallBack("TableSchemaService::_fetch_initial_metadata_via_rpc::test_hook",
                                          [&](void* arg) {
                                              auto ctx = unpack_initial_metadata_hook_args(arg);
                                              *ctx.mock_thrift_rpc = true;
                                              *ctx.status = Status::ThriftRpcError("connection refused");
                                          });

    auto result = _schema_service->get_tablet_initial_metadata(next_id(), next_id(), next_id(), next_id());
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().is_thrift_rpc_error());
}

TEST_F(TableSchemaServiceTest, get_tablet_initial_metadata_batch_error) {
    ScopedSyncPoint sync_point;

    SyncPoint::GetInstance()->SetCallBack(
            "TableSchemaService::_fetch_initial_metadata_via_rpc::test_hook", [&](void* arg) {
                auto ctx = unpack_initial_metadata_hook_args(arg);
                *ctx.mock_thrift_rpc = true;
                *ctx.status = Status::OK();
                // Batch-level status is not OK
                TableSchemaServiceTest::set_status(&ctx.response->status, TStatusCode::INTERNAL_ERROR,
                                                   "mocked batch error");
            });

    auto result = _schema_service->get_tablet_initial_metadata(next_id(), next_id(), next_id(), next_id());
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().is_internal_error());
}

TEST_F(TableSchemaServiceTest, get_tablet_initial_metadata_empty_response) {
    ScopedSyncPoint sync_point;

    SyncPoint::GetInstance()->SetCallBack(
            "TableSchemaService::_fetch_initial_metadata_via_rpc::test_hook", [&](void* arg) {
                auto ctx = unpack_initial_metadata_hook_args(arg);
                *ctx.mock_thrift_rpc = true;
                *ctx.status = Status::OK();
                TableSchemaServiceTest::set_status(&ctx.response->status, TStatusCode::OK);
                // No responses set, empty batch response
            });

    auto result = _schema_service->get_tablet_initial_metadata(next_id(), next_id(), next_id(), next_id());
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().is_internal_error());
    ASSERT_TRUE(result.status().message().find("empty response") != std::string::npos);
}

TEST_F(TableSchemaServiceTest, construct_initial_metadata_fields_match_create_tablet) {
    auto tablet_id_a = next_id();
    auto tablet_id_b = next_id();
    auto schema_id = next_id();

    // 1. Build TCreateTabletReq and create tablet A via create_tablet()
    TCreateTabletReq create_req;
    create_req.tablet_id = tablet_id_a;
    create_req.__set_version(1);
    create_req.__set_enable_persistent_index(true);
    create_req.__set_persistent_index_type(TPersistentIndexType::CLOUD_NATIVE);
    create_req.__set_compaction_strategy(TCompactionStrategy::REAL_TIME);
    create_req.__set_compression_type(TCompressionType::LZ4_FRAME);
    create_req.__set_compression_level(5);
    create_req.__set_gtid(99999);
    create_req.__set_create_schema_file(false);

    auto thrift_schema = make_thrift_schema(schema_id);
    create_req.__set_tablet_schema(thrift_schema);

    ASSERT_OK(_tablet_manager->create_tablet(create_req));
    ASSIGN_OR_ABORT(auto metadata_a, _tablet_manager->get_tablet_metadata(tablet_id_a, 1));

    // 2. Build a mocked FE response with the same field values
    TGetTabletMetadataResponse resp;
    set_status(&resp.status, TStatusCode::OK);
    TCloudTabletMeta meta;
    meta.__set_tablet_id(tablet_id_b);
    meta.__set_schema(thrift_schema);
    meta.__set_enable_persistent_index(true);
    meta.__set_persistent_index_type(TPersistentIndexType::CLOUD_NATIVE);
    meta.__set_compaction_strategy(TCompactionStrategy::REAL_TIME);
    meta.__set_compression_type(TCompressionType::LZ4_FRAME);
    meta.__set_compression_level(5);
    meta.__set_gtid(99999);
    resp.__set_meta(meta);

    // 3. Directly call build_initial_metadata to test field assembly
    ASSIGN_OR_ABORT(auto metadata_b, _tablet_manager->build_initial_metadata(tablet_id_b, resp));

    // 4. Compare fields
    ASSERT_EQ(tablet_id_b, metadata_b->id());
    ASSERT_EQ(metadata_a->version(), metadata_b->version());
    ASSERT_EQ(metadata_a->next_rowset_id(), metadata_b->next_rowset_id());
    ASSERT_EQ(metadata_a->cumulative_point(), metadata_b->cumulative_point());
    ASSERT_EQ(metadata_a->gtid(), metadata_b->gtid());
    ASSERT_EQ(metadata_a->enable_persistent_index(), metadata_b->enable_persistent_index());
    ASSERT_EQ(metadata_a->persistent_index_type(), metadata_b->persistent_index_type());
    ASSERT_EQ(metadata_a->compaction_strategy(), metadata_b->compaction_strategy());
    ASSERT_EQ(metadata_a->schema().id(), metadata_b->schema().id());
    ASSERT_EQ(metadata_a->schema().compression_level(), metadata_b->schema().compression_level());
}

TEST_F(TableSchemaServiceTest, build_initial_metadata) {
    auto schema_id = next_id();

    // Case 1: defaults (only schema set)
    {
        auto tablet_id = next_id();
        TGetTabletMetadataResponse resp;
        set_status(&resp.status, TStatusCode::OK);
        TCloudTabletMeta meta;
        meta.__set_tablet_id(tablet_id);
        meta.__set_schema(make_thrift_schema(schema_id));
        resp.__set_meta(meta);

        ASSIGN_OR_ABORT(auto metadata, _tablet_manager->build_initial_metadata(tablet_id, resp));
        ASSERT_EQ(tablet_id, metadata->id());
        ASSERT_EQ(1, metadata->version());
        ASSERT_EQ(1, metadata->next_rowset_id());
        ASSERT_EQ(0, metadata->cumulative_point());
        ASSERT_FALSE(metadata->has_range());
        ASSERT_FALSE(metadata->has_flat_json_config());
        // compaction_strategy defaults to DEFAULT when not set
        ASSERT_EQ(CompactionStrategyPB::DEFAULT, metadata->compaction_strategy());
        // compression_level defaults to -1
        ASSERT_EQ(-1, metadata->schema().compression_level());
    }

    // Case 2: LOCAL persistent index type
    {
        auto tablet_id = next_id();
        TGetTabletMetadataResponse resp;
        set_status(&resp.status, TStatusCode::OK);
        TCloudTabletMeta meta;
        meta.__set_tablet_id(tablet_id);
        meta.__set_schema(make_thrift_schema(schema_id));
        meta.__set_enable_persistent_index(true);
        meta.__set_persistent_index_type(TPersistentIndexType::LOCAL);
        resp.__set_meta(meta);

        ASSIGN_OR_ABORT(auto metadata, _tablet_manager->build_initial_metadata(tablet_id, resp));
        ASSERT_TRUE(metadata->enable_persistent_index());
        ASSERT_EQ(PersistentIndexTypePB::LOCAL, metadata->persistent_index_type());
    }

    // Case 3: tablet_ranges contains this tablet's range
    {
        auto tablet_id = next_id();
        auto other_tablet_id = next_id();
        TGetTabletMetadataResponse resp;
        set_status(&resp.status, TStatusCode::OK);
        TCloudTabletMeta meta;
        meta.__set_tablet_id(tablet_id);
        meta.__set_schema(make_thrift_schema(schema_id));

        std::map<int64_t, TTabletRange> tablet_ranges;
        TTabletRange range;
        range.__set_lower_bound_included(true);
        range.__set_upper_bound_included(false);
        tablet_ranges[tablet_id] = range;
        tablet_ranges[other_tablet_id] = TTabletRange();
        meta.__set_tablet_ranges(tablet_ranges);
        resp.__set_meta(meta);

        ASSIGN_OR_ABORT(auto metadata, _tablet_manager->build_initial_metadata(tablet_id, resp));
        ASSERT_TRUE(metadata->has_range());
        ASSERT_TRUE(metadata->range().lower_bound_included());
        ASSERT_FALSE(metadata->range().upper_bound_included());
    }

    // Case 4: tablet_ranges set but does not contain this tablet
    {
        auto tablet_id = next_id();
        TGetTabletMetadataResponse resp;
        set_status(&resp.status, TStatusCode::OK);
        TCloudTabletMeta meta;
        meta.__set_tablet_id(tablet_id);
        meta.__set_schema(make_thrift_schema(schema_id));
        std::map<int64_t, TTabletRange> tablet_ranges;
        tablet_ranges[next_id()] = TTabletRange();
        meta.__set_tablet_ranges(tablet_ranges);
        resp.__set_meta(meta);

        ASSIGN_OR_ABORT(auto metadata, _tablet_manager->build_initial_metadata(tablet_id, resp));
        ASSERT_FALSE(metadata->has_range());
    }

    // Case 5: flat_json_config is set
    {
        auto tablet_id = next_id();
        TGetTabletMetadataResponse resp;
        set_status(&resp.status, TStatusCode::OK);
        TCloudTabletMeta meta;
        meta.__set_tablet_id(tablet_id);
        meta.__set_schema(make_thrift_schema(schema_id));
        TFlatJsonConfig flat_json;
        flat_json.__set_flat_json_enable(true);
        flat_json.__set_flat_json_null_factor(0.3);
        flat_json.__set_flat_json_sparsity_factor(0.9);
        flat_json.__set_flat_json_column_max(100);
        meta.__set_flat_json_config(flat_json);
        resp.__set_meta(meta);

        ASSIGN_OR_ABORT(auto metadata, _tablet_manager->build_initial_metadata(tablet_id, resp));
        ASSERT_TRUE(metadata->has_flat_json_config());
        ASSERT_TRUE(metadata->flat_json_config().flat_json_enable());
    }

    // Case 6: missing meta should fail
    {
        auto tablet_id = next_id();
        TGetTabletMetadataResponse resp;
        set_status(&resp.status, TStatusCode::OK);

        auto result = _tablet_manager->build_initial_metadata(tablet_id, resp);
        ASSERT_FALSE(result.ok());
    }
}

TEST_F(TableSchemaServiceTest, parse_shard_properties) {
    int64_t tablet_id = next_id();

    // Case 1: all fields present and valid
    {
        std::unordered_map<std::string, std::string> properties;
        properties["tableId"] = "100";
        properties["partitionId"] = "200";
        properties["indexId"] = "300";
        int64_t table_id = 0, partition_id = 0, index_id = 0;
        ASSERT_OK(TabletManager::parse_shard_properties(tablet_id, properties, &table_id, &partition_id, &index_id));
        ASSERT_EQ(100, table_id);
        ASSERT_EQ(200, partition_id);
        ASSERT_EQ(300, index_id);
    }

    // Case 2: missing required property
    {
        std::unordered_map<std::string, std::string> properties;
        properties["tableId"] = "100";
        properties["partitionId"] = "200";
        // missing indexId
        int64_t table_id = 0, partition_id = 0, index_id = 0;
        auto st = TabletManager::parse_shard_properties(tablet_id, properties, &table_id, &partition_id, &index_id);
        ASSERT_TRUE(st.is_internal_error());
    }
}

TEST_F(TableSchemaServiceTest, cn_free_fallback_not_triggered_for_version_gt_1) {
    ScopedSyncPoint sync_point;
    std::atomic<int> rpc_calls{0};

    SyncPoint::GetInstance()->SetCallBack(
            "TableSchemaService::_fetch_initial_metadata_via_rpc::test_hook", [&](void* arg) {
                rpc_calls.fetch_add(1);
                auto ctx = unpack_initial_metadata_hook_args(arg);
                *ctx.mock_thrift_rpc = true;
                *ctx.status = Status::OK();
                TableSchemaServiceTest::set_status(&ctx.response->status, TStatusCode::OK);
                ctx.response->__set_responses(std::vector<TGetTabletMetadataResponse>{});
                auto& resp = ctx.response->responses.emplace_back();
                TableSchemaServiceTest::set_status(&resp.status, TStatusCode::OK);
                TCloudTabletMeta meta;
                meta.__set_tablet_id(ctx.request->tablet_id);
                meta.__set_schema(make_thrift_schema(next_id()));
                resp.__set_meta(meta);
            });

    // version 2 not found should NOT trigger cn-free fallback
    auto result = _tablet_manager->get_tablet_metadata(next_id(), 2);
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().is_not_found());
    ASSERT_EQ(0, rpc_calls.load());
}

TEST_F(TableSchemaServiceTest, cn_free_fallback_not_triggered_with_external_fs) {
    ScopedSyncPoint sync_point;
    std::atomic<int> rpc_calls{0};

    SyncPoint::GetInstance()->SetCallBack(
            "TableSchemaService::_fetch_initial_metadata_via_rpc::test_hook", [&](void* arg) {
                rpc_calls.fetch_add(1);
                auto ctx = unpack_initial_metadata_hook_args(arg);
                *ctx.mock_thrift_rpc = true;
                *ctx.status = Status::OK();
                TableSchemaServiceTest::set_status(&ctx.response->status, TStatusCode::OK);
                ctx.response->__set_responses(std::vector<TGetTabletMetadataResponse>{});
                auto& resp = ctx.response->responses.emplace_back();
                TableSchemaServiceTest::set_status(&resp.status, TStatusCode::OK);
                TCloudTabletMeta meta;
                meta.__set_tablet_id(ctx.request->tablet_id);
                meta.__set_schema(make_thrift_schema(next_id()));
                resp.__set_meta(meta);
            });

    // version 1 not found with non-null fs should NOT trigger cn-free fallback
    auto external_fs = std::shared_ptr<FileSystem>(FileSystem::Default(), [](FileSystem*) {});
    auto tablet_id = next_id();
    auto result = _tablet_manager->get_tablet_metadata(tablet_id, 1, true, 0, external_fs);
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().is_not_found());
    ASSERT_EQ(0, rpc_calls.load());
}

} // namespace starrocks::lake
