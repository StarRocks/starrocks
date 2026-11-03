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

#include "exec/schema_scanner/schema_fe_metrics_scanner.h"

#include <gtest/gtest.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <unistd.h>

#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "common/object_pool.h"
#include "gen_cpp/FrontendService.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks {

// Mock FrontendService that returns a canned `/metrics?type=json` payload over the
// getFeMetrics RPC, so the scanner's new Thrift transport + JSON parsing can be exercised
// without a live FE and without HTTP auth.
class FeMetricsMockService : public FrontendServiceNull {
public:
    explicit FeMetricsMockService(std::string json, bool include_json = true)
            : _json(std::move(json)), _include_json(include_json) {}
    ~FeMetricsMockService() override = default;

    void getFeMetrics(TFeMetricsResult& result) override {
        TStatus status;
        status.__set_status_code(TStatusCode::OK);
        result.__set_status(status);
        // When `_include_json` is false the reply carries an OK status but no json_metrics,
        // mimicking a partial/older FE so the scanner's missing-field guard is exercised.
        if (_include_json) {
            result.__set_json_metrics(_json);
        }
    }

private:
    std::string _json;
    bool _include_json;
};

class SchemaFeMetricsScannerTest : public testing::Test {
public:
    void start_mock(const std::string& json, bool include_json = true) {
        using namespace apache::thrift::transport;
        using namespace apache::thrift::protocol;
        using namespace apache::thrift::server;

        auto service = std::make_shared<FeMetricsMockService>(json, include_json);
        _processor = std::make_shared<FrontendServiceProcessor>(service);
        // Port 0 lets the OS assign an available port.
        _server_transport = std::make_shared<TServerSocket>(0);
        _server = std::make_unique<TSimpleServer>(_processor, _server_transport,
                                                  std::make_shared<TBufferedTransportFactory>(),
                                                  std::make_shared<TBinaryProtocolFactory>());
        _thr = std::make_unique<std::thread>([this]() { _server->serve(); });
        // TServerSocket binds and listens before getPort() returns a non-zero port, so once
        // the port is assigned the server is ready to accept — poll for that instead of a
        // fixed sleep, and fail fast (rather than proceeding with port 0) if it never binds.
        for (int i = 0; i < 50 && _port <= 0; ++i) {
            _port = _server_transport->getPort();
            if (_port <= 0) {
                usleep(100000); // 100ms
            }
        }
        ASSERT_GT(_port, 0) << "mock FrontendService failed to bind an ephemeral port";

        // The scanner reaches the FE via ThriftRpcHelper, which resolves its client cache from the
        // ExecEnv installed by setup(). Point the process-wide ExecEnv's frontend client cache at a
        // local cache so the RPC works in the unit test, then restore it in TearDown(). The test build
        // is compiled with -fno-access-control, so the private member is reachable here.
        _exec_env = ExecEnv::GetInstance();
        _saved_fe_cache = _exec_env->_frontend_client_cache;
        _exec_env->_frontend_client_cache = &_fe_cache;
        ThriftRpcHelper::setup(_exec_env);
    }

    void TearDown() override {
        if (_exec_env != nullptr) {
            _exec_env->_frontend_client_cache = _saved_fe_cache;
        }
        if (_server != nullptr) {
            _server->stop();
            _thr->join();
        }
    }

protected:
    std::unique_ptr<std::thread> _thr;
    std::shared_ptr<FrontendServiceProcessor> _processor;
    std::unique_ptr<apache::thrift::server::TSimpleServer> _server;
    std::shared_ptr<apache::thrift::transport::TServerSocket> _server_transport;
    FrontendServiceClientCache _fe_cache;
    ExecEnv* _exec_env = nullptr;
    ClientCache<FrontendServiceClient>* _saved_fe_cache = nullptr;
    int _port = 0;
    ObjectPool _pool;
};

static SchemaScannerParam make_param(int rpc_port) {
    TFrontend frontend;
    frontend.__set_ip("127.0.0.1");
    frontend.__set_id("1");
    frontend.__set_rpc_port(rpc_port);

    SchemaScannerParam param;
    param.frontends.push_back(std::move(frontend));
    return param;
}

TEST_F(SchemaFeMetricsScannerTest, fetch_metrics_over_thrift_ok) {
    start_mock(R"([{"tags":{"metric":"jvm_young_gc","type":"count"},"unit":"nounit","value":741}])");

    RuntimeState state;
    SchemaScannerParam param = make_param(_port);
    SchemaFeMetricsScanner scanner;
    ASSERT_OK(scanner.init(&param, &_pool));
    ASSERT_OK(scanner.start(&state));
}

TEST_F(SchemaFeMetricsScannerTest, malformed_json_returns_error) {
    // Missing quote after "value" makes the payload invalid JSON.
    start_mock(R"([{"tags":{"metric":"jvm_young_gc","type":"count"},"unit":"nounit","value:741}])");

    RuntimeState state;
    SchemaScannerParam param = make_param(_port);
    SchemaFeMetricsScanner scanner;
    ASSERT_OK(scanner.init(&param, &_pool));
    auto st = scanner.start(&state);
    ASSERT_ERROR(st);
    ASSERT_TRUE(st.message().find("Parse the result of fe metrics failed") != std::string::npos);
}

TEST_F(SchemaFeMetricsScannerTest, skip_frontend_with_invalid_rpc_port) {
    // A frontend whose rpc_port is missing/invalid (e.g. FE/BE version skew) is skipped with
    // a warning instead of connecting to port 0 — the query succeeds and yields no rows for it.
    RuntimeState state;
    SchemaScannerParam param = make_param(0);
    SchemaFeMetricsScanner scanner;
    ASSERT_OK(scanner.init(&param, &_pool));
    ASSERT_OK(scanner.start(&state));
}

TEST_F(SchemaFeMetricsScannerTest, missing_json_metrics_returns_error) {
    // FE replies with an OK status but no json_metrics field (e.g. a partial response or an
    // older FE). The scanner must surface an InternalError instead of treating the missing
    // payload as empty metrics — covers the `!result.__isset.json_metrics` guard.
    start_mock(/*json=*/"", /*include_json=*/false);

    RuntimeState state;
    SchemaScannerParam param = make_param(_port);
    SchemaFeMetricsScanner scanner;
    ASSERT_OK(scanner.init(&param, &_pool));
    auto st = scanner.start(&state);
    ASSERT_ERROR(st);
    ASSERT_TRUE(st.message().find("missing json_metrics") != std::string::npos);
}

} // namespace starrocks
