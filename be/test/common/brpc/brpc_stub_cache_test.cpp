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

#include "common/brpc/brpc_stub_cache.h"

#include <base/testutil/assert.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <unordered_set>

#include "base/concurrency/countdown_latch.h"
#include "base/failpoint/fail_point.h"
#include "base/utility/defer_op.h"
#include "common/config_exec_flow_fwd.h"
#include "common/config_network_fwd.h"
#include "common/config_rpc_client_fwd.h"

namespace starrocks {

class BrpcStubCacheTest : public testing::Test {
public:
    BrpcStubCacheTest() = default;
    ~BrpcStubCacheTest() override = default;
    void SetUp() override {
        _saved_brpc_max_connections_per_server = config::brpc_max_connections_per_server;
        _saved_brpc_stub_expire_s = config::brpc_stub_expire_s;
        _saved_brpc_connection_type = config::brpc_connection_type;
        config::brpc_max_connections_per_server = 1;
        config::brpc_stub_expire_s = 3600;
        config::brpc_connection_type = "single";
        _timer = std::make_unique<BthreadTimer>();
        ASSERT_OK(_timer->start());
    }
    void TearDown() override {
        _timer.reset();
        config::brpc_max_connections_per_server = _saved_brpc_max_connections_per_server;
        config::brpc_stub_expire_s = _saved_brpc_stub_expire_s;
        config::brpc_connection_type = _saved_brpc_connection_type;
    }

private:
    std::unique_ptr<BthreadTimer> _timer;
    int32_t _saved_brpc_max_connections_per_server = 0;
    int32_t _saved_brpc_stub_expire_s = 0;
    std::string _saved_brpc_connection_type;
};

TEST_F(BrpcStubCacheTest, normal) {
    BrpcStubCache cache(_timer.get());
    TNetworkAddress address;
    address.hostname = "127.0.0.1";
    address.port = 123;
    auto stub1 = cache.get_stub(address);
    ASSERT_NE(nullptr, stub1);
    address.port = 124;
    auto stub2 = cache.get_stub(address);
    ASSERT_NE(nullptr, stub2);
    ASSERT_NE(stub1, stub2);
    address.port = 123;
    auto stub3 = cache.get_stub(address);
    ASSERT_EQ(stub1, stub3);
}

TEST_F(BrpcStubCacheTest, invalid) {
    BrpcStubCache cache(_timer.get());
    TNetworkAddress address;
    address.hostname = "invalid.cm.invalid";
    address.port = 123;
    auto stub1 = cache.get_stub(address);
    ASSERT_EQ(nullptr, stub1);
}

TEST_F(BrpcStubCacheTest, reset) {
    BrpcStubCache cache(_timer.get());
    TNetworkAddress address;
    address.hostname = "127.0.0.1";
    address.port = 123;
    auto stub1 = cache.get_stub(address);
    ASSERT_NE(nullptr, stub1);
    auto istub1 = stub1->stub();

    stub1->reset_channel();
    auto istub2 = stub1->stub();

    ASSERT_NE(istub1, istub2);
}

#ifndef __APPLE__
TEST_F(BrpcStubCacheTest, lake_service_stub_normal) {
    LakeServiceBrpcStubCache cache(_timer.get());
    TNetworkAddress address;
    std::string hostname = "127.0.0.1";
    int32_t port1 = 123;
    auto stub1 = cache.get_stub(hostname, port1);
    ASSERT_TRUE(stub1.ok());
    int32_t port2 = 124;
    auto stub2 = cache.get_stub(hostname, port2);
    ASSERT_TRUE(stub2.ok());
    ASSERT_NE(*stub1, *stub2);
    auto stub3 = cache.get_stub(hostname, port1);
    ASSERT_TRUE(stub3.ok());
    ASSERT_EQ(*stub1, *stub3);
    auto stub4 = cache.get_stub("invalid.cm.invalid", 123);
    ASSERT_FALSE(stub4.ok());
}
#endif

TEST_F(BrpcStubCacheTest, test_http_stub) {
    HttpBrpcStubCache cache(_timer.get());
    TNetworkAddress address;
    address.hostname = "127.0.0.1";
    address.port = 123;
    auto stub1 = cache.get_http_stub(address);
    ASSERT_NE(nullptr, *stub1);
    address.port = 124;
    auto stub2 = cache.get_http_stub(address);
    ASSERT_NE(nullptr, *stub2);
    ASSERT_NE(*stub1, *stub2);
    address.port = 123;
    auto stub3 = cache.get_http_stub(address);
    ASSERT_NE(nullptr, *stub3);
    ASSERT_EQ(*stub1, *stub3);

    address.hostname = "invalid.cm.invalid";
    auto stub4 = cache.get_http_stub(address);
    ASSERT_EQ(nullptr, *stub4);
}

TEST_F(BrpcStubCacheTest, test_cleanup) {
    config::brpc_stub_expire_s = 1;
    BrpcStubCache cache(_timer.get());
    TNetworkAddress address;
    address.hostname = "127.0.0.1";
    address.port = 123;
    auto stub1 = cache.get_stub(address);
    ASSERT_NE(nullptr, stub1);
    auto stub2 = cache.get_stub(address);
    ASSERT_EQ(stub2, stub1);

    sleep(2);
    auto stub3 = cache.get_stub(address);
    ASSERT_NE(stub3, stub1);
}

#ifndef __APPLE__
TEST_F(BrpcStubCacheTest, test_lake_cleanup) {
    config::brpc_stub_expire_s = 1;
    LakeServiceBrpcStubCache cache(_timer.get());
    std::string hostname = "127.0.0.1";
    int32_t port = 123;
    auto stub1 = cache.get_stub(hostname, port);
    ASSERT_TRUE(stub1.ok());
    ASSERT_NE(nullptr, *stub1);
    auto stub2 = cache.get_stub(hostname, port);
    ASSERT_TRUE(stub1.ok());
    ASSERT_EQ(*stub2, *stub1);

    sleep(2);
    auto stub3 = cache.get_stub(hostname, port);
    ASSERT_NE(*stub3, *stub1);
}
#endif

TEST_F(BrpcStubCacheTest, test_http_cleanup) {
    config::brpc_stub_expire_s = 1;
    HttpBrpcStubCache cache(_timer.get());
    TNetworkAddress address;
    address.hostname = "127.0.0.1";
    address.port = 123;
    auto stub1 = cache.get_http_stub(address);
    ASSERT_NE(nullptr, *stub1);
    auto stub2 = cache.get_http_stub(address);
    ASSERT_EQ(*stub2, *stub1);

    sleep(2);
    auto stub3 = cache.get_http_stub(address);
    ASSERT_NE(*stub3, *stub1);
}

// Regression test: destroying BrpcStubCache while a cleanup task is scheduled
// (and possibly firing) must join the task before the cache state is torn down.
TEST_F(BrpcStubCacheTest, test_destructor_joins_inflight_cleanup_tasks) {
    config::brpc_stub_expire_s = 1;
    auto cache = std::make_unique<BrpcStubCache>(_timer.get());
    TNetworkAddress address;
    address.hostname = "127.0.0.1";
    address.port = 123;
    auto stub = cache->get_stub(address);
    ASSERT_NE(nullptr, stub);

    // Trigger ~BrpcStubCache() while the cleanup task is (or will be) in flight.
    // Drain the cache and assert the unique_ptr release returns cleanly.
    cache.reset();

    // Reacquire the endpoint through a fresh cache; the slot must have been
    // cleanly torn down without leaking the previous task.
    auto cache2 = std::make_unique<BrpcStubCache>(_timer.get());
    auto fresh_stub = cache2->get_stub(address);
    ASSERT_NE(nullptr, fresh_stub);
    cache2.reset();
}

// Regression test for the lazy-reschedule fix: while an endpoint is being
// accessed within the expire window, the timer fires, sees the stub is still
// active (idle < brpc_stub_expire_s), and reschedules instead of evicting, so the
// stub must survive across multiple timer periods.
TEST_F(BrpcStubCacheTest, test_active_access_keeps_stub_alive_across_expire_window) {
    config::brpc_stub_expire_s = 2;
    BrpcStubCache cache(_timer.get());
    TNetworkAddress address;
    address.hostname = "127.0.0.1";
    address.port = 123;
    auto stub1 = cache.get_stub(address);
    ASSERT_NE(nullptr, stub1);

    // Repeatedly access within the window; each access refreshes the last-access
    // time, and the single scheduled timer reschedules instead of evicting.
    for (int i = 0; i < 3; ++i) {
        sleep(1);
        auto stub = cache.get_stub(address);
        ASSERT_EQ(stub1, stub) << "stub must not be evicted while being accessed";
    }
}

TEST_F(BrpcStubCacheTest, test_http_active_access_keeps_stub_alive_across_expire_window) {
    config::brpc_stub_expire_s = 2;
    HttpBrpcStubCache cache(_timer.get());
    TNetworkAddress address;
    address.hostname = "127.0.0.1";
    address.port = 123;
    auto stub1 = cache.get_http_stub(address);
    ASSERT_NE(nullptr, *stub1);

    for (int i = 0; i < 3; ++i) {
        sleep(1);
        auto stub = cache.get_http_stub(address);
        ASSERT_NE(nullptr, *stub);
        ASSERT_EQ(*stub1, *stub) << "http stub must not be evicted while being accessed";
    }
}

#ifndef __APPLE__
TEST_F(BrpcStubCacheTest, test_lake_active_access_keeps_stub_alive_across_expire_window) {
    config::brpc_stub_expire_s = 2;
    LakeServiceBrpcStubCache cache(_timer.get());
    std::string hostname = "127.0.0.1";
    int32_t port = 123;
    auto stub1 = cache.get_stub(hostname, port);
    ASSERT_TRUE(stub1.ok());
    ASSERT_NE(nullptr, *stub1);

    for (int i = 0; i < 3; ++i) {
        sleep(1);
        auto stub = cache.get_stub(hostname, port);
        ASSERT_TRUE(stub.ok());
        ASSERT_NE(nullptr, *stub);
        ASSERT_EQ(*stub1, *stub) << "lake stub must not be evicted while being accessed";
    }
}
#endif

TEST_F(BrpcStubCacheTest, http_singleton_reinitialize_rebinds_pipeline_timer) {
    auto timer2 = std::make_unique<BthreadTimer>();
    ASSERT_OK(timer2->start());

    HttpBrpcStubCache::initialize(_timer.get());
    auto* cache = HttpBrpcStubCache::getInstance();
    ASSERT_NE(nullptr, cache);

    TNetworkAddress address;
    address.hostname = "127.0.0.1";
    address.port = 123;
    auto stub = cache->get_http_stub(address);
    ASSERT_TRUE(stub.ok());
    ASSERT_NE(nullptr, *stub);

    cache->shutdown();
    ASSERT_FALSE(cache->get_http_stub(address).ok());

    HttpBrpcStubCache::initialize(timer2.get());

    auto rebound_stub = cache->get_http_stub(address);
    ASSERT_TRUE(rebound_stub.ok());
    ASSERT_NE(nullptr, *rebound_stub);

    cache->shutdown();
}

#ifndef __APPLE__
namespace {

class HangingLakeService : public LakeService {
public:
    void publish_version(google::protobuf::RpcController* /*controller*/, const PublishVersionRequest* /*request*/,
                         PublishVersionResponse* /*response*/, google::protobuf::Closure* done) override {
        received.count_down();
        release.wait();
        done->Run();
    }

    CountDownLatch received{1};
    CountDownLatch release{1};
};

class SignalClosure : public google::protobuf::Closure {
public:
    explicit SignalClosure(CountDownLatch* done) : _done(done) {}
    void Run() override { _done->count_down(); }

private:
    CountDownLatch* _done;
};

} // namespace

TEST_F(BrpcStubCacheTest, lake_singleton_reinitialize_rebinds_pipeline_timer) {
    auto timer2 = std::make_unique<BthreadTimer>();
    ASSERT_OK(timer2->start());

    LakeServiceBrpcStubCache::initialize(_timer.get());
    auto* cache = LakeServiceBrpcStubCache::getInstance();
    ASSERT_NE(nullptr, cache);

    auto stub = cache->get_stub("127.0.0.1", 123);
    ASSERT_TRUE(stub.ok());
    ASSERT_NE(nullptr, *stub);

    cache->shutdown();
    ASSERT_FALSE(cache->get_stub("127.0.0.1", 123).ok());

    LakeServiceBrpcStubCache::initialize(timer2.get());

    auto rebound_stub = cache->get_stub("127.0.0.1", 123);
    ASSERT_TRUE(rebound_stub.ok());
    ASSERT_NE(nullptr, *rebound_stub);

    cache->shutdown();
}

TEST_F(BrpcStubCacheTest, lake_traffic_contributes_to_internal_service_connection_load) {
    config::brpc_max_connections_per_server = 2;
    PublishVersionRequest request;
    request.add_tablet_ids(1);
    PublishVersionResponse response;
    brpc::Controller controller;
    controller.request_attachment().append("lake-attachment");
    const int64_t payload_bytes = request.ByteSizeLong() + controller.request_attachment().size();
    CountDownLatch rpc_done(1);
    SignalClosure done(&rpc_done);

    brpc::Server server;
    HangingLakeService service;
    brpc::ServerOptions options;
    ASSERT_EQ(0, server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE));
    ASSERT_EQ(0, server.Start(0, &options));
    DeferOp stop_server([&] {
        service.release.count_down();
        server.Stop(0);
        server.Join();
    });

    BrpcStubCache cache(_timer.get());
    const butil::EndPoint endpoint = server.listen_address();

    auto internal_stub = cache.get_stub(endpoint);
    ASSERT_NE(nullptr, internal_stub);
    auto lake_stub = std::make_shared<LakeService_RecoverableStub>(endpoint);
    ASSERT_OK(lake_stub->reset_channel());
    auto sibling_stub = std::make_shared<PInternalService_RecoverableStub>(endpoint, "", 1);
    auto sibling_reservation = sibling_stub->reserve_rpc(512);
    ASSERT_EQ(0, internal_stub->num_in_flight_rpcs());
    ASSERT_EQ(0, internal_stub->num_in_flight_payload_bytes());
    ASSERT_EQ(1, sibling_stub->num_in_flight_rpcs());
    ASSERT_EQ(512, sibling_stub->num_in_flight_payload_bytes());
    sibling_reservation.reset();

    lake_stub->publish_version(&controller, &request, &response, &done);
    ASSERT_TRUE(service.received.wait_for(std::chrono::seconds(10)));

    ASSERT_EQ(1, internal_stub->num_in_flight_rpcs());
    ASSERT_EQ(payload_bytes, internal_stub->num_in_flight_payload_bytes());

    auto selected_or = cache.acquire_least_loaded_stub(endpoint);
    ASSERT_OK(selected_or.status());
    auto selected = std::move(selected_or).value();
    ASSERT_NE(internal_stub.get(), selected.reservation.stub());
    ASSERT_TRUE(selected.created_on_contention);

    service.release.count_down();
    ASSERT_TRUE(rpc_done.wait_for(std::chrono::seconds(10)));
    ASSERT_EQ(0, internal_stub->num_in_flight_rpcs());
    ASSERT_EQ(0, internal_stub->num_in_flight_payload_bytes());
}
#endif

TEST_F(BrpcStubCacheTest, acquire_least_loaded_stub) {
    config::brpc_max_connections_per_server = 3;
    BrpcStubCache cache(_timer.get());
    TNetworkAddress address;
    address.hostname = "127.0.0.1";
    address.port = 123;

    auto stub0 = cache.get_stub(address);
    ASSERT_NE(nullptr, stub0);

    auto first_or = cache.acquire_least_loaded_stub(stub0->endpoint());
    ASSERT_OK(first_or.status());
    auto first = std::move(first_or).value();
    ASSERT_EQ(stub0.get(), first.reservation.stub());
    ASSERT_EQ(0, first.reservation.in_flight_before());
    ASSERT_FALSE(first.created_on_contention);
    ASSERT_FALSE(first.selected_at_connection_limit);

    auto second_or = cache.acquire_least_loaded_stub(stub0->endpoint());
    ASSERT_OK(second_or.status());
    auto second = std::move(second_or).value();
    ASSERT_NE(stub0.get(), second.reservation.stub());
    ASSERT_EQ(0, second.reservation.in_flight_before());
    ASSERT_TRUE(second.created_on_contention);
    ASSERT_FALSE(second.selected_at_connection_limit);

    auto third_or = cache.acquire_least_loaded_stub(stub0->endpoint());
    ASSERT_OK(third_or.status());
    auto third = std::move(third_or).value();
    ASSERT_NE(stub0.get(), third.reservation.stub());
    ASSERT_NE(second.reservation.stub(), third.reservation.stub());
    ASSERT_EQ(0, third.reservation.in_flight_before());
    ASSERT_TRUE(third.created_on_contention);
    ASSERT_FALSE(third.selected_at_connection_limit);

    auto least_loaded_or = cache.acquire_least_loaded_stub(stub0->endpoint());
    ASSERT_OK(least_loaded_or.status());
    auto least_loaded = std::move(least_loaded_or).value();
    ASSERT_EQ(stub0.get(), least_loaded.reservation.stub());
    ASSERT_EQ(1, least_loaded.reservation.in_flight_before());
    ASSERT_FALSE(least_loaded.created_on_contention);
    ASSERT_TRUE(least_loaded.selected_at_connection_limit);
}

TEST_F(BrpcStubCacheTest, first_stub_is_not_created_on_contention) {
    BrpcStubCache cache(_timer.get());
    butil::EndPoint endpoint;
    ASSERT_EQ(0, butil::str2endpoint("127.0.0.1", 123, &endpoint));

    auto pool = cache.get_or_create_pool(endpoint);
    ASSERT_NE(nullptr, pool);
    ASSERT_TRUE(pool->_stubs.empty());

    auto selected_or = pool->acquire_least_loaded(endpoint, 0);
    ASSERT_OK(selected_or.status());
    auto selected = std::move(selected_or).value();
    ASSERT_NE(nullptr, selected.reservation.stub());
    ASSERT_FALSE(selected.created_on_contention);
    ASSERT_FALSE(selected.selected_at_connection_limit);
}

TEST_F(BrpcStubCacheTest, stub_creation_failure_is_not_connection_limit) {
#ifdef FIU_ENABLE
    config::brpc_max_connections_per_server = 2;
    BrpcStubCache cache(_timer.get());
    TNetworkAddress address;
    address.hostname = "127.0.0.1";
    address.port = 123;

    auto stub = cache.get_stub(address);
    ASSERT_NE(nullptr, stub);
    auto busy = stub->reserve_rpc();

    auto* failpoint = failpoint::FailPointRegistry::GetInstance()->get("brpc_stub_cache_create_stub_failed");
    ASSERT_NE(nullptr, failpoint);
    PFailPointTriggerMode mode;
    mode.set_mode(FailPointTriggerModeType::ENABLE);
    failpoint->setMode(mode);
    DeferOp disable_failpoint([&] {
        mode.set_mode(FailPointTriggerModeType::DISABLE);
        failpoint->setMode(mode);
    });

    auto selected_or = cache.acquire_least_loaded_stub(stub->endpoint());
    ASSERT_OK(selected_or.status());
    auto selected = std::move(selected_or).value();
    ASSERT_EQ(stub.get(), selected.reservation.stub());
    ASSERT_FALSE(selected.created_on_contention);
    ASSERT_FALSE(selected.selected_at_connection_limit);
#else
    GTEST_SKIP() << "requires FIU_ENABLE to inject stub creation failure";
#endif
}

TEST_F(BrpcStubCacheTest, acquire_least_loaded_stub_prefers_lower_payload_load) {
    config::brpc_max_connections_per_server = 2;
    BrpcStubCache cache(_timer.get());
    TNetworkAddress address;
    address.hostname = "127.0.0.1";
    address.port = 123;

    auto stub0 = cache.get_stub(address);
    ASSERT_NE(nullptr, stub0);
    const int64_t batch_bytes = std::max<int64_t>(config::max_transmit_batched_bytes, 1);

    auto first_or = cache.acquire_least_loaded_stub(stub0->endpoint(), 2 * batch_bytes);
    ASSERT_OK(first_or.status());
    auto first = std::move(first_or).value();
    ASSERT_EQ(stub0.get(), first.reservation.stub());

    auto second_or = cache.acquire_least_loaded_stub(stub0->endpoint(), batch_bytes);
    ASSERT_OK(second_or.status());
    auto second = std::move(second_or).value();
    ASSERT_NE(stub0.get(), second.reservation.stub());

    auto selected_or = cache.acquire_least_loaded_stub(stub0->endpoint());
    ASSERT_OK(selected_or.status());
    auto selected = std::move(selected_or).value();
    ASSERT_EQ(second.reservation.stub(), selected.reservation.stub());
    ASSERT_EQ(1, selected.reservation.in_flight_before());
    ASSERT_TRUE(selected.selected_at_connection_limit);
}

TEST_F(BrpcStubCacheTest, dynamic_selection_requires_single_connection_type) {
    BrpcStubCache cache(_timer.get());
    TNetworkAddress address;
    address.hostname = "127.0.0.1";
    address.port = 123;
    auto stub = cache.get_stub(address);
    ASSERT_NE(nullptr, stub);

    for (const auto* connection_type : {"pooled", "short"}) {
        config::brpc_connection_type = connection_type;
        auto selection = cache.acquire_least_loaded_stub(stub->endpoint());
        ASSERT_FALSE(selection.ok());
        ASSERT_TRUE(selection.status().is_not_supported());
    }
}

TEST_F(BrpcStubCacheTest, concurrent_acquire_does_not_exceed_limit) {
    config::brpc_max_connections_per_server = 4;
    BrpcStubCache cache(_timer.get());
    TNetworkAddress address;
    address.hostname = "127.0.0.1";
    address.port = 123;

    auto initial_stub = cache.get_stub(address);
    ASSERT_NE(nullptr, initial_stub);
    auto initial_reservation = initial_stub->reserve_rpc();

    std::atomic<bool> succeeded = true;
    std::mutex selections_mutex;
    std::vector<BrpcStubCache::StubSelection> selections;
    std::vector<std::thread> threads;
    for (int i = 0; i < 16; ++i) {
        threads.emplace_back([&]() {
            auto result = cache.acquire_least_loaded_stub(initial_stub->endpoint());
            if (!result.ok()) {
                succeeded = false;
                return;
            }
            std::lock_guard lock(selections_mutex);
            selections.emplace_back(std::move(result).value());
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    ASSERT_TRUE(succeeded);
    ASSERT_EQ(16, selections.size());
    size_t created_on_contention = 0;
    std::unordered_set<PInternalService_RecoverableStub*> selected_stubs{initial_stub.get()};
    for (const auto& selection : selections) {
        created_on_contention += selection.created_on_contention;
        selected_stubs.insert(selection.reservation.stub());
    }
    ASSERT_EQ(3, created_on_contention);
    ASSERT_EQ(config::brpc_max_connections_per_server, selected_stubs.size());
}

TEST_F(BrpcStubCacheTest, get_or_create_pool_returns_stable_pool) {
    config::brpc_max_connections_per_server = 2;
    BrpcStubCache cache(_timer.get());
    TNetworkAddress address;
    address.hostname = "127.0.0.1";
    address.port = 123;

    auto stub0 = cache.get_stub(address);
    ASSERT_NE(nullptr, stub0);

    auto pool = cache.get_or_create_pool(stub0->endpoint());
    ASSERT_NE(nullptr, pool);
    // Repeated lookups renew the deadline and return the same pool instance rather than recreating it.
    ASSERT_EQ(pool.get(), cache.get_or_create_pool(stub0->endpoint()).get());

    // Selecting through the cached pool yields the stub already created via get_stub.
    auto selected = pool->acquire_least_loaded(stub0->endpoint(), 0);
    ASSERT_OK(selected.status());
    ASSERT_EQ(stub0.get(), std::move(selected).value().reservation.stub());
}

TEST_F(BrpcStubCacheTest, cached_pool_stays_usable_after_map_expiry) {
    config::brpc_stub_expire_s = 1;
    config::brpc_max_connections_per_server = 2;
    BrpcStubCache cache(_timer.get());
    TNetworkAddress address;
    address.hostname = "127.0.0.1";
    address.port = 123;

    auto stub = cache.get_stub(address);
    ASSERT_NE(nullptr, stub);
    const auto endpoint = stub->endpoint();
    auto pool = cache.get_or_create_pool(endpoint);
    ASSERT_NE(nullptr, pool);

    // Let the cleanup task evict the pool from the map after the expiry window.
    sleep(2);

    // The retained shared_ptr keeps the pool and its stubs alive and selectable.
    auto selected = pool->acquire_least_loaded(endpoint, 0);
    ASSERT_OK(selected.status());
    auto selection = std::move(selected).value();
    ASSERT_EQ(stub.get(), selection.reservation.stub());
    selection.reservation.reset();

    // A fresh lookup re-registers a new, distinct pool for the endpoint.
    auto fresh = cache.get_or_create_pool(endpoint);
    ASSERT_NE(pool.get(), fresh.get());

    // Recreated wrappers for the same connection slot share accounting with retained wrappers from the old pool.
    auto old_reservation = stub->reserve_rpc(1024);
    auto fresh_stub = fresh->get_or_create(endpoint);
    ASSERT_NE(stub.get(), fresh_stub.get());
    ASSERT_EQ(1, fresh_stub->num_in_flight_rpcs());
    ASSERT_EQ(1024, fresh_stub->num_in_flight_payload_bytes());
}

} // namespace starrocks
