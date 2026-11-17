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

#include <arpa/inet.h>
#include <base/testutil/assert.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "base/failpoint/fail_point.h"
#include "common/config_network_fwd.h"

namespace starrocks {

namespace {

// Binds a local port without listening on it, so that connections to the port are refused.
class RefusingPort {
public:
    RefusingPort() {
        _fd = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        socklen_t len = sizeof(addr);
        if (_fd >= 0 && bind(_fd, reinterpret_cast<sockaddr*>(&addr), len) == 0 &&
            getsockname(_fd, reinterpret_cast<sockaddr*>(&addr), &len) == 0) {
            _port = ntohs(addr.sin_port);
        }
    }
    ~RefusingPort() {
        if (_fd >= 0) {
            close(_fd);
        }
    }

    int port() const { return _port; }

private:
    int _fd = -1;
    int _port = 0;
};

// Sends a synchronous RPC, which RecoverableClosure doesn't wrap, so a failure leaves the channel as it is.
template <typename StubT, typename RequestT, typename ResponseT>
bool send_rpc(StubT* stub, void (StubT::*method)(google::protobuf::RpcController*, const RequestT*, ResponseT*,
                                                 google::protobuf::Closure*)) {
    brpc::Controller cntl;
    cntl.set_timeout_ms(3000);
    RequestT request;
    ResponseT response;
    (stub->*method)(&cntl, &request, &response, nullptr);
    return !cntl.Failed();
}

template <typename RecoverableStubT>
bool connection_failed(const std::shared_ptr<RecoverableStubT>& stub) {
    return dynamic_cast<brpc::ChannelBase*>(stub->stub()->channel())->CheckHealth() != 0;
}

} // namespace

class BrpcStubCacheTest : public testing::Test {
public:
    BrpcStubCacheTest() = default;
    ~BrpcStubCacheTest() override = default;
    void SetUp() override {
        _saved_brpc_max_connections_per_server = config::brpc_max_connections_per_server;
        _saved_brpc_stub_expire_s = config::brpc_stub_expire_s;
        _saved_brpc_failed_channel_reset_interval_s = config::brpc_failed_channel_reset_interval_s;
        config::brpc_max_connections_per_server = 1;
        config::brpc_stub_expire_s = 3600;
        // Tests that need the periodic reset enable it, so that it doesn't race with the other tests.
        config::brpc_failed_channel_reset_interval_s = 0;
        _timer = std::make_unique<BthreadTimer>();
        ASSERT_OK(_timer->start());
    }
    void TearDown() override {
        _timer.reset();
        config::brpc_max_connections_per_server = _saved_brpc_max_connections_per_server;
        config::brpc_stub_expire_s = _saved_brpc_stub_expire_s;
        config::brpc_failed_channel_reset_interval_s = _saved_brpc_failed_channel_reset_interval_s;
    }

private:
    std::unique_ptr<BthreadTimer> _timer;
    int32_t _saved_brpc_max_connections_per_server = 0;
    int32_t _saved_brpc_stub_expire_s = 0;
    int32_t _saved_brpc_failed_channel_reset_interval_s = 0;
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

TEST_F(BrpcStubCacheTest, test_reset_failed_channels) {
    config::brpc_max_connections_per_server = 2;
    RefusingPort port;
    ASSERT_GT(port.port(), 0);
    BrpcStubCache cache(_timer.get());
    auto failed_stub = cache.get_stub("127.0.0.1", port.port());
    auto idle_stub = cache.get_stub("127.0.0.1", port.port());
    ASSERT_NE(nullptr, failed_stub);
    ASSERT_NE(nullptr, idle_stub);
    ASSERT_NE(failed_stub, idle_stub);

    ASSERT_FALSE(send_rpc(failed_stub->stub().get(), &PInternalService_Stub::execute_command));
    ASSERT_TRUE(connection_failed(failed_stub));
    ASSERT_FALSE(connection_failed(idle_stub));
    const int64_t failed_group = failed_stub->connection_group();
    const int64_t idle_group = idle_stub->connection_group();

    cache.reset_failed_channels();
    // The new channel doesn't connect until the next RPC.
    ASSERT_EQ(failed_group + 1, failed_stub->connection_group());
    ASSERT_FALSE(connection_failed(failed_stub));
    ASSERT_EQ(idle_group, idle_stub->connection_group());

    // Stubs that were reset are not reset again until they fail again.
    cache.reset_failed_channels();
    ASSERT_EQ(failed_group + 1, failed_stub->connection_group());
    ASSERT_FALSE(send_rpc(failed_stub->stub().get(), &PInternalService_Stub::execute_command));
    cache.reset_failed_channels();
    ASSERT_EQ(failed_group + 2, failed_stub->connection_group());
}

TEST_F(BrpcStubCacheTest, test_failed_channel_reset_task) {
    config::brpc_failed_channel_reset_interval_s = 1;
    RefusingPort port;
    ASSERT_GT(port.port(), 0);
    auto cache = std::make_unique<BrpcStubCache>(_timer.get());
    ASSERT_NE(nullptr, cache->_channel_reset_task);
    auto stub = cache->get_stub("127.0.0.1", port.port());
    ASSERT_NE(nullptr, stub);
    const int64_t group = stub->connection_group();
    ASSERT_FALSE(send_rpc(stub->stub().get(), &PInternalService_Stub::execute_command));

    for (int i = 0; i < 50 && stub->connection_group() == group; ++i) {
        usleep(100 * 1000);
    }
    ASSERT_EQ(group + 1, stub->connection_group());
    ASSERT_FALSE(connection_failed(stub));

    // The destructor waits for the task, which may be running.
    cache.reset();
}

#ifndef __APPLE__
TEST_F(BrpcStubCacheTest, test_lake_reset_failed_channels) {
    RefusingPort port;
    ASSERT_GT(port.port(), 0);
    LakeServiceBrpcStubCache cache(_timer.get());
    auto failed_stub = cache.get_stub("127.0.0.1", port.port());
    ASSERT_TRUE(failed_stub.ok());
    RefusingPort idle_port;
    ASSERT_GT(idle_port.port(), 0);
    auto idle_stub = cache.get_stub("127.0.0.1", idle_port.port());
    ASSERT_TRUE(idle_stub.ok());

    ASSERT_FALSE(send_rpc((*failed_stub)->stub().get(), &LakeService_Stub::publish_version));
    ASSERT_TRUE(connection_failed(*failed_stub));
    const int64_t failed_group = (*failed_stub)->connection_group();
    const int64_t idle_group = (*idle_stub)->connection_group();

    cache.reset_failed_channels();
    ASSERT_EQ(failed_group + 1, (*failed_stub)->connection_group());
    ASSERT_FALSE(connection_failed(*failed_stub));
    ASSERT_EQ(idle_group, (*idle_stub)->connection_group());
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
TEST_F(BrpcStubCacheTest, lake_singleton_reinitialize_rebinds_pipeline_timer) {
    auto timer2 = std::make_unique<BthreadTimer>();
    ASSERT_OK(timer2->start());

    LakeServiceBrpcStubCache::initialize(_timer.get());
    auto* cache = LakeServiceBrpcStubCache::getInstance();
    ASSERT_NE(nullptr, cache);

    auto stub = cache->get_stub("127.0.0.1", 123);
    ASSERT_TRUE(stub.ok());
    ASSERT_NE(nullptr, *stub);
    ASSERT_NE(nullptr, cache->_channel_reset_task);

    cache->shutdown();
    ASSERT_FALSE(cache->get_stub("127.0.0.1", 123).ok());
    ASSERT_EQ(nullptr, cache->_channel_reset_task);

    LakeServiceBrpcStubCache::initialize(timer2.get());
    ASSERT_NE(nullptr, cache->_channel_reset_task);

    auto rebound_stub = cache->get_stub("127.0.0.1", 123);
    ASSERT_TRUE(rebound_stub.ok());
    ASSERT_NE(nullptr, *rebound_stub);

    cache->shutdown();
    ASSERT_EQ(nullptr, cache->_channel_reset_task);
}
#endif

} // namespace starrocks
