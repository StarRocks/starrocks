// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include <aws/core/Aws.h>
#include <gperftools/malloc_extension.h>
#include <unistd.h>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "compute_service.h"
#include "exec/pipeline/query_context.h"
#include "http_service.h"
#include "internal_service.h"
#include "runtime/exec_env.h"
#include "service/brpc_service.h"
#include "service/service.h"
#include "storage/storage_engine.h"
#include "util/logging.h"
#include "util/thrift_server.h"

void start_cn() {
    using starrocks::Status;
    auto* exec_env = starrocks::ExecEnv::GetInstance();

    // Begin to start services
    // 1. Start thrift server with 'thrift_port'.
    starrocks::ThriftServer* cn_server = nullptr;
    EXIT_IF_ERROR(starrocks::ComputeService::create_service(exec_env, starrocks::config::thrift_port, &cn_server));
    Status status = cn_server->start();
    if (!status.ok()) {
        LOG(ERROR) << "StarRocks CN server did not start correctly, exiting";
        starrocks::shutdown_logging();
        exit(1);
    }

    // 2. Start brpc service.
    std::unique_ptr<starrocks::BRpcService> brpc_service = std::make_unique<starrocks::BRpcService>(exec_env);
    status = brpc_service->start(starrocks::config::brpc_port,
                                 new starrocks::ComputeNodeInternalServiceImpl<starrocks::PInternalService>(exec_env),
                                 new starrocks::ComputeNodeInternalServiceImpl<doris::PBackendService>(exec_env));
    if (!status.ok()) {
        LOG(ERROR) << "BRPC service did not start correctly, exiting";
        starrocks::shutdown_logging();
        exit(1);
    }

    // 3. Start http service.
    std::unique_ptr<starrocks::HttpServiceCN> http_service = std::make_unique<starrocks::HttpServiceCN>(
            exec_env, starrocks::config::webserver_port, starrocks::config::webserver_num_workers);
    status = http_service->start();
    if (!status.ok()) {
        LOG(ERROR) << "Internal Error:" << status.message();
        LOG(ERROR) << "StarRocks CN http service did not start correctly, exiting";
        starrocks::shutdown_logging();
        exit(1);
    }

    while (!starrocks::k_starrocks_exit) {
        sleep(10);
    }

    http_service.reset();
    brpc_service->join();
    brpc_service.reset();

    cn_server->stop();
    cn_server->join();
    delete cn_server;
}
