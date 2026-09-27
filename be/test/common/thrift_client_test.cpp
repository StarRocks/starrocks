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

#include "common/util/thrift_client.h"

#include <gtest/gtest.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <unistd.h>

#include <memory>
#include <thread>

#include "base/network/network_util.h"
#include "base/testutil/assert.h"
#include "common/util/thrift_client_cache.h"
#include "gen_cpp/FrontendService.h"

namespace starrocks {

class MockedThriftService : public FrontendServiceNull {
public:
    ~MockedThriftService() override = default;
};

class MockedFrontendService {
public:
    void init();

    ~MockedFrontendService() {
        _server->stop();
        _thr->join();
    }

    int get_port() const { return _port; }

private:
    std::unique_ptr<std::thread> _thr;
    std::shared_ptr<FrontendServiceProcessor> _processer;
    std::unique_ptr<apache::thrift::server::TSimpleServer> _server;
    std::shared_ptr<apache::thrift::transport::TServerSocket> _server_transport;
    int _port = 0;
};

void MockedFrontendService::init() {
    using namespace apache::thrift::transport;
    using namespace apache::thrift::protocol;
    using namespace apache::thrift::server;

    auto service = std::make_shared<MockedThriftService>();
    _processer = std::make_unique<FrontendServiceProcessor>(service);

    // Use port 0 to let the OS assign an available port
    _server_transport = std::make_shared<TServerSocket>(0);
    auto transportFactory = std::make_shared<TBufferedTransportFactory>();
    auto protocolFactory = std::make_shared<TBinaryProtocolFactory>();
    _server = std::make_unique<TSimpleServer>(_processer, _server_transport, transportFactory, protocolFactory);
    _thr = std::make_unique<std::thread>([this]() { _server->serve(); });
    // thrift server don't provide a start function
    // wait server ready and get the actual port that was assigned
    // The port is assigned when the server starts listening
    for (int i = 0; i < 30; ++i) {
        _port = _server_transport->getPort();
        if (_port > 0) {
            break;
        }
        usleep(100000); // sleep 100ms
    }
    // Additional wait to ensure server is fully ready
    sleep(1);
}

class ThriftClientTest : public testing::Test {
protected:
    static void SetUpTestSuite() {
        _service = std::make_unique<MockedFrontendService>();
        _service->init();
        _other_service = std::make_unique<MockedFrontendService>();
        _other_service->init();
    }

    static void TearDownTestSuite() {
        _other_service.reset();
        _service.reset();
    }

    static MockedFrontendService* service() { return _service.get(); }
    static MockedFrontendService* other_service() { return _other_service.get(); }

private:
    static std::unique_ptr<MockedFrontendService> _service;
    static std::unique_ptr<MockedFrontendService> _other_service;
};

std::unique_ptr<MockedFrontendService> ThriftClientTest::_service;
std::unique_ptr<MockedFrontendService> ThriftClientTest::_other_service;

TEST_F(ThriftClientTest, test_open_close_and_reopen) {
    TGetProfileResponse rep;
    TGetProfileRequest req;

    ThriftClient<FrontendServiceClient> client("127.0.0.1", service()->get_port());
    ASSERT_OK(client.open());
    client.iface()->getQueryProfile(rep, req);

    client.close();
    ASSERT_OK(client.open_with_retry(3, 100));
    client.iface()->getQueryProfile(rep, req);
}

TEST_F(ThriftClientTest, release_client_after_close_connections) {
    FrontendServiceClientCache client_cache(10);
    MetricRegistry metrics("test");
    client_cache.init_metrics(&metrics, "frontend");
    auto address = make_network_address("127.0.0.1", service()->get_port());
    auto labels = MetricLabels().add("name", "frontend");
    auto* opened_clients = dynamic_cast<IntGauge*>(metrics.get_metric("thrift_opened_clients", labels));
    auto* used_clients = dynamic_cast<IntGauge*>(metrics.get_metric("thrift_used_clients", labels));
    ASSERT_NE(nullptr, opened_clients);
    ASSERT_NE(nullptr, used_clients);

    {
        Status status;
        FrontendServiceConnection client(&client_cache, address, 1000, &status);
        ASSERT_OK(status);
        ASSERT_EQ(1, opened_clients->value());
        ASSERT_EQ(1, used_clients->value());

        client_cache.close_connections(address);
        ASSERT_EQ(1, client_cache._client_cache_helper._client_map.size());
        ASSERT_EQ(1, client_cache._client_cache_helper._clients_to_evict.size());
    }

    EXPECT_TRUE(client_cache._client_cache_helper._client_map.empty());
    EXPECT_TRUE(client_cache._client_cache_helper._clients_to_evict.empty());
    EXPECT_EQ(0, opened_clients->value());
    EXPECT_EQ(0, used_clients->value());
}

TEST_F(ThriftClientTest, invalidated_client_does_not_enter_recreated_cache) {
    FrontendServiceClientCache client_cache(10);
    MetricRegistry metrics("test");
    client_cache.init_metrics(&metrics, "frontend");
    auto address = make_network_address("127.0.0.1", service()->get_port());
    auto labels = MetricLabels().add("name", "frontend");
    auto* opened_clients = dynamic_cast<IntGauge*>(metrics.get_metric("thrift_opened_clients", labels));
    auto* used_clients = dynamic_cast<IntGauge*>(metrics.get_metric("thrift_used_clients", labels));
    ASSERT_NE(nullptr, opened_clients);
    ASSERT_NE(nullptr, used_clients);

    Status old_status;
    auto old_client = std::make_unique<FrontendServiceConnection>(&client_cache, address, 1000, &old_status);
    ASSERT_OK(old_status);
    client_cache.close_connections(address);

    {
        Status new_status;
        FrontendServiceConnection new_client(&client_cache, address, 1000, &new_status);
        ASSERT_OK(new_status);
        ASSERT_EQ(2, opened_clients->value());
        ASSERT_EQ(2, used_clients->value());

        old_client.reset();
        EXPECT_EQ(1, client_cache._client_cache_helper._client_map.size());
        EXPECT_TRUE(client_cache._client_cache_helper._clients_to_evict.empty());
        EXPECT_EQ(1, opened_clients->value());
        EXPECT_EQ(1, used_clients->value());
    }

    EXPECT_EQ(1, client_cache._client_cache_helper._client_map.size());
    EXPECT_EQ(1, opened_clients->value());
    EXPECT_EQ(0, used_clients->value());

    client_cache.close_connections(address);
    EXPECT_TRUE(client_cache._client_cache_helper._client_map.empty());
    EXPECT_EQ(0, opened_clients->value());
    EXPECT_EQ(0, used_clients->value());
}

TEST_F(ThriftClientTest, close_connections_only_invalidates_matching_host) {
    FrontendServiceClientCache client_cache(10);
    MetricRegistry metrics("test");
    client_cache.init_metrics(&metrics, "frontend");
    auto address = make_network_address("127.0.0.1", service()->get_port());
    auto other_address = make_network_address("127.0.0.1", other_service()->get_port());
    auto labels = MetricLabels().add("name", "frontend");
    auto* opened_clients = dynamic_cast<IntGauge*>(metrics.get_metric("thrift_opened_clients", labels));
    auto* used_clients = dynamic_cast<IntGauge*>(metrics.get_metric("thrift_used_clients", labels));
    ASSERT_NE(nullptr, opened_clients);
    ASSERT_NE(nullptr, used_clients);

    Status status;
    auto client = std::make_unique<FrontendServiceConnection>(&client_cache, address, 1000, &status);
    ASSERT_OK(status);
    Status other_status;
    auto other_client = std::make_unique<FrontendServiceConnection>(&client_cache, other_address, 1000, &other_status);
    ASSERT_OK(other_status);
    void* client_key = client->get();
    void* other_client_key = other_client->get();

    client_cache.close_connections(address);
    ASSERT_EQ(1, client_cache._client_cache_helper._clients_to_evict.size());
    EXPECT_NE(client_cache._client_cache_helper._clients_to_evict.end(),
              client_cache._client_cache_helper._clients_to_evict.find(client_key));
    EXPECT_EQ(client_cache._client_cache_helper._clients_to_evict.end(),
              client_cache._client_cache_helper._clients_to_evict.find(other_client_key));

    client.reset();
    EXPECT_EQ(1, client_cache._client_cache_helper._client_map.size());
    EXPECT_TRUE(client_cache._client_cache_helper._clients_to_evict.empty());

    other_client.reset();
    auto cache_entry = client_cache._client_cache_helper._client_cache.find(other_address);
    ASSERT_NE(client_cache._client_cache_helper._client_cache.end(), cache_entry);
    EXPECT_EQ(1, cache_entry->second.size());
    EXPECT_EQ(1, opened_clients->value());
    EXPECT_EQ(0, used_clients->value());

    client_cache.close_connections(other_address);
    EXPECT_TRUE(client_cache._client_cache_helper._client_map.empty());
    EXPECT_EQ(0, opened_clients->value());
}

} // namespace starrocks
