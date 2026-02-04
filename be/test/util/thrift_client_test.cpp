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
#include <thrift/concurrency/Thread.h>
#include <thrift/concurrency/ThreadFactory.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TSocket.h>
#include <unistd.h>

#include <memory>

#include "base/testutil/assert.h"
#include "gen_cpp/FrontendService.h"
#include "runtime/client_cache.h"
#include "util/network_util.h"
#include "util/thrift_rpc_helper.h"

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

TEST(ThriftRpcClientCacheTest, test_all) {
    MockedFrontendService service;
    service.init();
    TGetProfileResponse rep;
    TGetProfileRequest req;

    auto client_cache = std::make_unique<FrontendServiceClientCache>(config::max_client_cache_size_per_host);
    TNetworkAddress address = make_network_address("127.0.0.1", service.get_port());
    Status status;
    FrontendServiceConnection client(client_cache.get(), address, 1000, &status);
    ASSERT_OK(status);
    client->getQueryProfile(rep, req);
    ASSERT_OK(client.reopen(100));
    client->getQueryProfile(rep, req);
}

} // namespace starrocks
