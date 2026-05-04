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

#include "util/brpc_stub_cache.h"

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
        _timer = std::make_unique<BthreadTimer>();
        ASSERT_OK(_timer->start());
    }
    void TearDown() override {
        _timer.reset();
        config::brpc_stub_expire_s = 3600;
    }

private:
    std::unique_ptr<BthreadTimer> _timer;
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
