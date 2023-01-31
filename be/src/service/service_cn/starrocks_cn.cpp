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

#include <gperftools/malloc_extension.h>
#include <unistd.h>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "compute_service.h"
#include "exec/pipeline/query_context.h"
#include "http_service.h"
#include "runtime/exec_env.h"
#include "service/brpc.h"
#include "service/service.h"
#include "service/service_cn/internal_service.h"
#include "storage/storage_engine.h"
#include "util/logging.h"
#include "util/thrift_server.h"

namespace brpc {

DECLARE_uint64(max_body_size);
DECLARE_int64(socket_max_unwritten_bytes);

} // namespace brpc

void start_cn() {
    using starrocks::ComputeService;
    auto* exec_env = starrocks::ExecEnv::GetInstance();

    // Begin to start services
    // 1. Start thrift server with 'thrift_port'.
    auto thrift_server = ComputeService::create<ComputeService>(exec_env, starrocks::config::thrift_port);
    if (auto status = thrift_server->start(); !status.ok()) {
        LOG(ERROR) << "Fail to start ComputeService thrift server on port " << starrocks::config::be_port << ": "
                   << status;
        starrocks::shutdown_logging();
        exit(1);
    }

    // 2. Start brpc service.
    brpc::FLAGS_max_body_size = starrocks::config::brpc_max_body_size;
    brpc::FLAGS_socket_max_unwritten_bytes = starrocks::config::brpc_socket_max_unwritten_bytes;
    brpc::Server brpc_server;

    starrocks::ComputeNodeInternalServiceImpl<starrocks::PInternalService> internal_service(exec_env);
    starrocks::ComputeNodeInternalServiceImpl<doris::PBackendService> compute_service(exec_env);

    brpc_server.AddService(&internal_service, brpc::SERVER_DOESNT_OWN_SERVICE);
    brpc_server.AddService(&compute_service, brpc::SERVER_DOESNT_OWN_SERVICE);

    brpc::ServerOptions options;
    if (starrocks::config::brpc_num_threads != -1) {
        options.num_threads = starrocks::config::brpc_num_threads;
    }
    if (brpc_server.Start(starrocks::config::brpc_port, &options) != 0) {
        LOG(ERROR) << "BRPC service did not start correctly, exiting";
        starrocks::shutdown_logging();
        exit(1);
    }

    // 3. Start http service.
    std::unique_ptr<starrocks::HttpServiceCN> http_service = std::make_unique<starrocks::HttpServiceCN>(
            exec_env, starrocks::config::be_http_port, starrocks::config::be_http_num_workers);
    if (auto status = http_service->start(); !status.ok()) {
        LOG(ERROR) << "Internal Error:" << status.message();
        LOG(ERROR) << "StarRocks CN http service did not start correctly, exiting";
        starrocks::shutdown_logging();
        exit(1);
    }

    while (!starrocks::k_starrocks_exit.load()) {
        sleep(10);
    }

    starrocks::wait_for_fragments_finish(exec_env, starrocks::config::loop_count_wait_fragments_finish);

    http_service.reset();

    brpc_server.Stop(0);
    brpc_server.Join();

    thrift_server->stop();
    thrift_server->join();
}
