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
    brpc::FLAGS_max_body_size = starrocks::config::brpc_max_body_size;
    brpc::FLAGS_socket_max_unwritten_bytes = starrocks::config::brpc_socket_max_unwritten_bytes;
    brpc::Server brpc_server;

    starrocks::ComputeNodeInternalServiceImpl<starrocks::PInternalService> compute_service(exec_env);
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
            exec_env, starrocks::config::webserver_port, starrocks::config::webserver_num_workers);
    status = http_service->start();
    if (!status.ok()) {
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

    cn_server->stop();
    cn_server->join();
    delete cn_server;
}
