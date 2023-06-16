#include <gperftools/malloc_extension.h>
#include <unistd.h>

#if defined(LEAK_SANITIZER)
#include <sanitizer/lsan_interface.h>
#endif

#include "backend_service.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/pipeline/query_context.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "service/brpc.h"
#include "service/service.h"
#include "service/service_be/http_service.h"
#include "service/service_be/internal_service.h"
#include "service/service_be/lake_service.h"
#include "storage/storage_engine.h"
#include "util/logging.h"
#include "util/thrift_server.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/load_channel_mgr.h"
#include "runtime/stream_load/load_stream_mgr.h"

namespace brpc {

DECLARE_uint64(max_body_size);
DECLARE_int64(socket_max_unwritten_bytes);

} // namespace brpc

void start_be() {
    using starrocks::BackendService;

    auto* exec_env = starrocks::ExecEnv::GetInstance();

    // Begin to start services
    // 1. Start thrift server with 'be_port'.
    auto thrift_server = BackendService::create<BackendService>(exec_env, starrocks::config::be_port);
    if (auto status = thrift_server->start(); !status.ok()) {
        LOG(ERROR) << "Fail to start BackendService thrift server on port " << starrocks::config::be_port << ": "
                   << status;
        starrocks::shutdown_logging();
        exit(1);
    }

    // 2. Start brpc services.
    brpc::FLAGS_max_body_size = starrocks::config::brpc_max_body_size;
    brpc::FLAGS_socket_max_unwritten_bytes = starrocks::config::brpc_socket_max_unwritten_bytes;
    brpc::Server brpc_server;

    starrocks::BackendInternalServiceImpl<starrocks::PInternalService> internal_service(exec_env);
    starrocks::BackendInternalServiceImpl<doris::PBackendService> backend_service(exec_env);
    starrocks::LakeServiceImpl lake_service(exec_env);

    brpc_server.AddService(&internal_service, brpc::SERVER_DOESNT_OWN_SERVICE);
    brpc_server.AddService(&backend_service, brpc::SERVER_DOESNT_OWN_SERVICE);
    brpc_server.AddService(&lake_service, brpc::SERVER_DOESNT_OWN_SERVICE);

    brpc::ServerOptions options;
    if (starrocks::config::brpc_num_threads != -1) {
        options.num_threads = starrocks::config::brpc_num_threads;
    }
    if (brpc_server.Start(starrocks::config::brpc_port, &options) != 0) {
        LOG(ERROR) << "BRPC service did not start correctly, exiting";
        starrocks::shutdown_logging();
        exit(1);
    }

    // 3. Start HTTP service.
    std::unique_ptr<starrocks::HttpServiceBE> http_service = std::make_unique<starrocks::HttpServiceBE>(
            exec_env, starrocks::config::be_http_port, starrocks::config::be_http_num_workers);
    if (auto status = http_service->start(); !status.ok()) {
        LOG(ERROR) << "Internal Error:" << status.message();
        LOG(ERROR) << "StarRocks Be http service did not start correctly, exiting";
        starrocks::shutdown_logging();
        exit(1);
    }

    LOG(INFO) << "BE started successfully";

    while (!(starrocks::k_starrocks_exit.load()) && !(starrocks::k_starrocks_exit_quick.load())) {
        sleep(10);
    }

    starrocks::wait_for_fragments_finish(exec_env, starrocks::config::loop_count_wait_fragments_finish);

    http_service.reset();

    std::cout << "STEP 1" << std::endl;
    LOG(ERROR) << "STEP 1" << std::endl;
    brpc_server.Stop(0);
    std::cout << "STEP 2" << std::endl;
    LOG(ERROR) << "STEP 2" << std::endl;
    exec_env->stream_mgr()->cancel_all();
    exec_env->load_stream_mgr()->close_all();
    std::cout << "STEP 3" << std::endl;
    LOG(ERROR) << "STEP 3" << std::endl;
    brpc_server.Join();
    std::cout << "STEP 4" << std::endl;
    LOG(ERROR) << "STEP 4" << std::endl;

    thrift_server->stop();
    thrift_server->join();
}
