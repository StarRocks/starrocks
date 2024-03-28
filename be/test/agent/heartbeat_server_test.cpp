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
#include "agent/heartbeat_server.h"

#include <random>

#include "common/config.h"
#include "gtest/gtest.h"
#include "http/action/update_config_action.h"
#include "service/service_be/http_service.h"

namespace starrocks {

class HeartbeatServerTest : public testing::Test {
public:
    HeartbeatServerTest() = default;
    ~HeartbeatServerTest() override = default;

    void SetUp() override {
        std::vector<StorePath> paths;
        StorePath path("/tmp/");
        paths.push_back(path);

        _exec_env = ExecEnv::GetInstance();

        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> distr(config::be_http_port, std::min(config::be_http_port + 100, 65536));
        auto http_server_port = distr(gen);

        _http_server = std::make_unique<HttpServiceBE>(_exec_env, http_server_port, config::be_http_num_workers);
        EXPECT_TRUE(_http_server->start().ok());
    }

    void TearDown() override {
        _exec_env->wait_for_finish();
        _exec_env->stop();
        _http_server->stop();
        _http_server->join();
        _http_server.reset();
        _exec_env->destroy();
    }

private:
    ExecEnv* _exec_env;
    std::unique_ptr<HttpServiceBE> _http_server;
};

#ifdef USE_STAROS
TEST_F(HeartbeatServerTest, update_flush_thread_number_test) {
    TMasterInfo info;
    TNetworkAddress addr;
    addr.__set_hostname("127.0.0.1");
    info.__set_network_address(addr);
    info.__set_http_port(8030);
    info.__set_run_mode(TRunMode::SHARED_DATA);

    THeartbeatResult result;
    HeartbeatServer server;
    server.heartbeat(result, info);

    static auto num_hardware_cores = static_cast<int32_t>(CpuInfo::num_cores());
    EXPECT_EQ(config::flush_thread_num_per_store, 2 * num_hardware_cores);
}
#endif

} // namespace starrocks
