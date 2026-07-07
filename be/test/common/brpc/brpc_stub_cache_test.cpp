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

#include "base/failpoint/fail_point.h"
#include "common/config_network_fwd.h"

namespace starrocks {

class BrpcStubCacheTest : public testing::Test {
public:
    BrpcStubCacheTest() = default;
    ~BrpcStubCacheTest() override = default;
    void SetUp() override {
        _saved_brpc_max_connections_per_server = config::brpc_max_connections_per_server;
        _saved_brpc_stub_expire_s = config::brpc_stub_expire_s;
        config::brpc_max_connections_per_server = 1;
        config::brpc_stub_expire_s = 3600;
        _timer = std::make_unique<BthreadTimer>();
        ASSERT_OK(_timer->start());
    }
    void TearDown() override {
        _timer.reset();
        config::brpc_max_connections_per_server = _saved_brpc_max_connections_per_server;
        config::brpc_stub_expire_s = _saved_brpc_stub_expire_s;
    }

private:
    std::unique_ptr<BthreadTimer> _timer;
    int32_t _saved_brpc_max_connections_per_server = 0;
    int32_t _saved_brpc_stub_expire_s = 0;
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

// Reducing brpc_stub_expire_s between get_*_stub() calls must replace the cleanup task: a fresh
// task is scheduled with the new earlier deadline, while the previous task is left in the timer
// queue and will fire at its original (later) deadline. That late fire ends up calling
// _cache->_stub_map.erase() on the new entry -- safe, because the next get_*_stub() recreates it.
// The check below: the task pointer stored in the map must change after a TTL shrink.
TEST_F(BrpcStubCacheTest, test_ttl_shrink_replaces_cleanup_task) {
    config::brpc_stub_expire_s = 3600;
    BrpcStubCache cache(_timer.get());
    butil::EndPoint endpoint;
    ASSERT_EQ(0, butil::str2endpoint("127.0.0.1:123", &endpoint));
    ASSERT_NE(nullptr, cache.get_stub(endpoint));

    auto* pool_before = cache._stub_map.seek(endpoint);
    ASSERT_NE(nullptr, pool_before);
    auto* task_before = (*pool_before)->_cleanup_task.get();

    config::brpc_stub_expire_s = 1;
    ASSERT_NE(nullptr, cache.get_stub(endpoint));

    auto* pool_after = cache._stub_map.seek(endpoint);
    ASSERT_NE(nullptr, pool_after);
    auto* task_after = (*pool_after)->_cleanup_task.get();
    ASSERT_NE(task_before, task_after) << "shrinking TTL must install a new cleanup task";
}

TEST_F(BrpcStubCacheTest, test_http_ttl_shrink_replaces_cleanup_task) {
    config::brpc_stub_expire_s = 3600;
    HttpBrpcStubCache cache(_timer.get());
    TNetworkAddress address;
    address.hostname = "127.0.0.1";
    address.port = 123;
    auto stub = cache.get_http_stub(address);
    ASSERT_TRUE(stub.ok());

    butil::EndPoint endpoint;
    ASSERT_EQ(0, butil::str2endpoint("127.0.0.1:123", &endpoint));
    auto* entry_before = cache._stub_map.seek(endpoint);
    ASSERT_NE(nullptr, entry_before);
    auto* task_before = entry_before->cleanup_task.get();

    config::brpc_stub_expire_s = 1;
    auto stub2 = cache.get_http_stub(address);
    ASSERT_TRUE(stub2.ok());

    auto* entry_after = cache._stub_map.seek(endpoint);
    ASSERT_NE(nullptr, entry_after);
    auto* task_after = entry_after->cleanup_task.get();
    ASSERT_NE(task_before, task_after) << "shrinking TTL must install a new cleanup task";
}

#ifndef __APPLE__
TEST_F(BrpcStubCacheTest, test_lake_ttl_shrink_replaces_cleanup_task) {
    config::brpc_stub_expire_s = 3600;
    LakeServiceBrpcStubCache cache(_timer.get());
    auto stub = cache.get_stub("127.0.0.1", 123);
    ASSERT_TRUE(stub.ok());

    butil::EndPoint endpoint;
    ASSERT_EQ(0, butil::str2endpoint("127.0.0.1:123", &endpoint));
    auto* entry_before = cache._stub_map.seek(endpoint);
    ASSERT_NE(nullptr, entry_before);
    auto* task_before = entry_before->cleanup_task.get();

    config::brpc_stub_expire_s = 1;
    auto stub2 = cache.get_stub("127.0.0.1", 123);
    ASSERT_TRUE(stub2.ok());

    auto* entry_after = cache._stub_map.seek(endpoint);
    ASSERT_NE(nullptr, entry_after);
    auto* task_after = entry_after->cleanup_task.get();
    ASSERT_NE(task_before, task_after) << "shrinking TTL must install a new cleanup task";
}
#endif

TEST_F(BrpcStubCacheTest, http_singleton_reinitialize_rebinds_pipeline_timer) {
    auto timer2 = std::make_unique<BthreadTimer>();
    ASSERT_OK(timer2->start());

    HttpBrpcStubCache::initialize(_timer.get());
    auto* cache = HttpBrpcStubCache::getInstance();
    ASSERT_NE(nullptr, cache);
    ASSERT_EQ(_timer.get(), cache->_timer);

    TNetworkAddress address;
    address.hostname = "127.0.0.1";
    address.port = 123;
    auto stub = cache->get_http_stub(address);
    ASSERT_TRUE(stub.ok());
    ASSERT_NE(nullptr, *stub);

    cache->shutdown();
    ASSERT_EQ(nullptr, cache->_timer);

    HttpBrpcStubCache::initialize(timer2.get());
    ASSERT_EQ(timer2.get(), cache->_timer);

    auto rebound_stub = cache->get_http_stub(address);
    ASSERT_TRUE(rebound_stub.ok());
    ASSERT_NE(nullptr, *rebound_stub);

    cache->shutdown();
}

#ifndef __APPLE__
TEST_F(BrpcStubCacheTest, lake_singleton_reinitialize_rebinds_pipeline_timer) {
    auto timer2 = std::make_unique<BthreadTimer>();
    ASSERT_OK(timer2->start());

    LakeServiceBrpcStubCache::initialize(_timer.get());
    auto* cache = LakeServiceBrpcStubCache::getInstance();
    ASSERT_NE(nullptr, cache);
    ASSERT_EQ(_timer.get(), cache->_timer);

    auto stub = cache->get_stub("127.0.0.1", 123);
    ASSERT_TRUE(stub.ok());
    ASSERT_NE(nullptr, *stub);

    cache->shutdown();
    ASSERT_EQ(nullptr, cache->_timer);

    LakeServiceBrpcStubCache::initialize(timer2.get());
    ASSERT_EQ(timer2.get(), cache->_timer);

    auto rebound_stub = cache->get_stub("127.0.0.1", 123);
    ASSERT_TRUE(rebound_stub.ok());
    ASSERT_NE(nullptr, *rebound_stub);

    cache->shutdown();
}
#endif

} // namespace starrocks
