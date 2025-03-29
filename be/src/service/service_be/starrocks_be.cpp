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

#if defined(LEAK_SANITIZER)
#include <sanitizer/lsan_interface.h>
#endif

#include "agent/heartbeat_server.h"
#include "backend_service.h"
#include "cache/block_cache/block_cache.h"
#include "common/config.h"
#include "common/daemon.h"
#include "common/process_exit.h"
#include "common/status.h"
#include "exec/pipeline/query_context.h"
#include "fs/s3/poco_common.h"
#include "gutil/strings/join.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/global_variables.h"
#include "runtime/jdbc_driver_manager.h"
#include "service/brpc.h"
#include "service/service.h"
#include "service/service_be/arrow_flight_sql_service.h"
#include "service/service_be/http_service.h"
#include "service/service_be/internal_service.h"
#include "service/service_be/lake_service.h"
#include "service/staros_worker.h"
#include "storage/lake/tablet_manager.h"
#include "storage/storage_engine.h"
#include "util/logging.h"
#include "util/mem_info.h"
#include "util/thrift_rpc_helper.h"
#include "util/thrift_server.h"

namespace brpc {

DECLARE_uint64(max_body_size);
DECLARE_int64(socket_max_unwritten_bytes);
DECLARE_bool(socket_keepalive);

} // namespace brpc

namespace starrocks {

StorageEngine* init_storage_engine(GlobalEnv* global_env, std::vector<StorePath> paths, bool as_cn) {
    // Init and open storage engine.
    EngineOptions options;
    options.store_paths = std::move(paths);
    options.backend_uid = UniqueId::gen_uid();
    options.compaction_mem_tracker = global_env->compaction_mem_tracker();
    options.update_mem_tracker = global_env->update_mem_tracker();
    options.need_write_cluster_id = !as_cn;
    StorageEngine* engine = nullptr;

    EXIT_IF_ERROR(StorageEngine::open(options, &engine));

    return engine;
}

extern void shutdown_tracer();

void start_be(const std::vector<StorePath>& paths, bool as_cn) {
    std::string process_name = as_cn ? "CN" : "BE";

    int start_step = 1;

    auto daemon = std::make_unique<Daemon>();
    daemon->init(as_cn, paths);
    LOG(INFO) << process_name << " start step " << start_step++ << ": daemon threads start successfully";

    // init jdbc driver manager
    EXIT_IF_ERROR(JDBCDriverManager::getInstance()->init(std::string(getenv("STARROCKS_HOME")) + "/lib/jdbc_drivers"));
    LOG(INFO) << process_name << " start step " << start_step++ << ": jdbc driver manager init successfully";

    // init network option
    if (!BackendOptions::init(as_cn)) {
        exit(-1);
    }
    LOG(INFO) << process_name << " start step " << start_step++ << ": backend network options init successfully";

    // init global env
    auto* global_env = GlobalEnv::GetInstance();
    EXIT_IF_ERROR(global_env->init());
    LOG(INFO) << process_name << " start step " << start_step++ << ": global env init successfully";

    // make sure global variables are initialized
    auto* global_vars = GlobalVariables::GetInstance();
    CHECK(global_vars->is_init()) << "global variables not initialized";
    LOG(INFO) << process_name << " start step " << start_step++ << ": global variables init successfully";

    auto* storage_engine = init_storage_engine(global_env, paths, as_cn);
    LOG(INFO) << process_name << " start step " << start_step++ << ": storage engine init successfully";

    auto* cache_env = CacheEnv::GetInstance();
    EXIT_IF_ERROR(cache_env->init(paths));
    LOG(INFO) << process_name << " start step " << start_step++ << ": cache env init successfully";

    auto* exec_env = ExecEnv::GetInstance();
    EXIT_IF_ERROR(exec_env->init(paths, as_cn));
    LOG(INFO) << process_name << " start step " << start_step++ << ": exec engine init successfully";

    // Start all background threads of storage engine.
    // SHOULD be called after exec env is initialized.
    EXIT_IF_ERROR(storage_engine->start_bg_threads());
    LOG(INFO) << process_name << " start step " << start_step++ << ": storage engine start bg threads successfully";

#ifdef USE_STAROS
    auto* block_cache = cache_env->block_cache();
    if (config::datacache_unified_instance_enable && block_cache->is_initialized()) {
        init_staros_worker(block_cache->starcache_instance());
    } else {
        init_staros_worker(nullptr);
    }
    LOG(INFO) << process_name << " start step " << start_step++ << ": staros worker init successfully";
#endif

    // set up thrift client before providing any service to the external
    // because these services may use thrift client, for example, stream
    // load will send thrift rpc to FE after http server is started
    ThriftRpcHelper::setup(exec_env);

    // Start thrift server
    int thrift_port = config::be_port;
    if (as_cn && config::thrift_port != 0) {
        thrift_port = config::thrift_port;
        LOG(WARNING) << "'thrift_port' is deprecated, please update be.conf to use 'be_port' instead!";
    }
    auto thrift_server = BackendService::create<BackendService>(exec_env, thrift_port);

    if (auto status = thrift_server->start(); !status.ok()) {
        LOG(ERROR) << "Fail to start BackendService thrift server on port " << thrift_port << ": " << status;
        shutdown_logging();
        exit(1);
    }
    LOG(INFO) << process_name << " start step " << start_step++ << ": start thrift server successfully";

    // Start brpc server
    brpc::FLAGS_max_body_size = config::brpc_max_body_size;

    // Configure keepalive.
    brpc::FLAGS_socket_keepalive = config::brpc_socket_keepalive;

    brpc::FLAGS_socket_max_unwritten_bytes = config::brpc_socket_max_unwritten_bytes;
    auto brpc_server = std::make_unique<brpc::Server>();

    BackendInternalServiceImpl<PInternalService> internal_service(exec_env);
    LakeServiceImpl lake_service(exec_env, exec_env->lake_tablet_manager());

    brpc_server->AddService(&internal_service, brpc::SERVER_DOESNT_OWN_SERVICE);
    brpc_server->AddService(&lake_service, brpc::SERVER_DOESNT_OWN_SERVICE);

    brpc::ServerOptions options;
    if (config::brpc_num_threads != -1) {
        options.num_threads = config::brpc_num_threads;
    }
    const auto lake_service_max_concurrency = config::lake_service_max_concurrency;
    const auto service_name = "starrocks.LakeService";
    const auto methods = {
            "abort_txn",     "abort_compaction", "compact",         "drop_table",          "delete_data",
            "delete_tablet", "get_tablet_stats", "publish_version", "publish_log_version", "publish_log_version_batch",
            "vacuum",        "vacuum_full"};
    for (auto method : methods) {
        brpc_server->MaxConcurrencyOf(service_name, method) = lake_service_max_concurrency;
    }
    int brpc_port = config::brpc_port;
    butil::EndPoint point;
    if (butil::str2endpoint(BackendOptions::get_service_bind_address(), brpc_port, &point) < 0) {
        LOG(ERROR) << "Fail to convert address. Please check your backend config.";
        shutdown_logging();
        exit(1);
    }
    LOG(INFO) << "BRPC server bind to host: " << BackendOptions::get_service_bind_address() << ", port: " << brpc_port;
    if (auto ret = brpc_server->Start(point, &options); ret != 0) {
        LOG(ERROR) << "BRPC service did not start correctly, exiting errcoe: " << ret;
        shutdown_logging();
        exit(1);
    }
    LOG(INFO) << process_name << " start step " << start_step++ << ": start brpc server successfully";

    // Start HTTP server
    auto http_server =
            std::make_unique<HttpServiceBE>(cache_env, exec_env, config::be_http_port, config::be_http_num_workers);
    if (auto status = http_server->start(); !status.ok()) {
        LOG(ERROR) << process_name << " http server did not start correctly, exiting: " << status.message();
        shutdown_logging();
        exit(1);
    }
    LOG(INFO) << process_name << " start step " << start_step++ << ": start http server successfully";

    // Start Arrow Flight SQL server
    auto arrow_flight_sql_server = std::make_unique<ArrowFlightSqlServer>();
    if (auto status = arrow_flight_sql_server->start(config::arrow_flight_port); !status.ok()) {
        LOG(ERROR) << process_name << " Arrow Flight Sql Server did not start correctly, exiting: " << status.message()
                   << ". Its port might be occupied. You can modify `arrow_flight_port` in `be.conf` to an unused port "
                      "or set it to -1 to disable it.";
        shutdown_logging();
        exit(1);
    }
    LOG(INFO) << process_name << " start step " << start_step++ << ": start arrow flight sql server successfully";

    // Start heartbeat server
    std::unique_ptr<ThriftServer> heartbeat_server;
    if (auto ret = create_heartbeat_server(exec_env, config::heartbeat_service_port,
                                           config::heartbeat_service_thread_count);
        !ret.ok()) {
        LOG(ERROR) << process_name << " heartbeat server did not start correctly, exiting: " << ret.status().message();
        shutdown_logging();
        exit(1);
    } else {
        heartbeat_server = std::move(ret.value());
    }
    if (auto status = heartbeat_server->start(); !status.ok()) {
        LOG(ERROR) << process_name << " heartbeat server dint not start correctlr, exiting: " << status.message();
        shutdown_logging();
        exit(1);
    }
    LOG(INFO) << process_name << " start step " << start_step++ << ": start heartbeat server successfully";

    LOG(INFO) << process_name << " started successfully";

    while (!process_exit_in_progress()) {
        sleep(1);
    }

    int exit_step = 1;

    exec_env->wait_for_finish();
    LOG(INFO) << process_name << " exit step " << exit_step++ << ": wait exec engine tasks finish successfully";

    heartbeat_server->stop();
    heartbeat_server->join();
    heartbeat_server.reset();
    LOG(INFO) << process_name << " exit step " << exit_step++ << ": heartbeat server exit successfully";

    arrow_flight_sql_server->stop();
    arrow_flight_sql_server.reset();
    LOG(INFO) << process_name << " exit step " << exit_step++ << ": Arrow Flight SQL server exit successfully";

    http_server->stop();
    brpc_server->Stop(0);
    thrift_server->stop();

    daemon->stop();
    daemon.reset();
    LOG(INFO) << process_name << " exit step " << exit_step++ << ": daemon threads exit successfully";

    exec_env->stop();
    LOG(INFO) << process_name << " exit step " << exit_step++ << ": exec engine destroy successfully";

    storage_engine->stop();
    LOG(INFO) << process_name << " exit step " << exit_step++ << ": storage engine exit successfully";

#ifdef USE_STAROS
    if (exec_env->lake_tablet_manager() != nullptr) {
        exec_env->lake_tablet_manager()->stop();
    }
    shutdown_staros_worker();
    LOG(INFO) << process_name << " exit step " << exit_step++ << ": staros worker exit successfully";
#endif

    if (config::enable_poco_client_for_aws_sdk) {
        starrocks::poco::HTTPSessionPools::instance().shutdown();
        LOG(INFO) << process_name << " exit step " << exit_step++ << ": poco connection pool shutdown successfully";
    }

    http_server->join();
    http_server.reset();
    LOG(INFO) << process_name << " exit step " << exit_step++ << ": http server exit successfully";

    brpc_server->Join();
    brpc_server.reset();
    LOG(INFO) << process_name << " exit step " << exit_step++ << ": brpc server exit successfully";

    thrift_server->join();
    thrift_server.reset();
    LOG(INFO) << process_name << " exit step " << exit_step++ << ": thrift server exit successfully";

    exec_env->destroy();
    LOG(INFO) << process_name << " exit step " << exit_step++ << ": exec env destroy successfully";

    delete storage_engine;

    cache_env->destroy();
    LOG(ERROR) << process_name << " exit step " << exit_step++ << ": cache env destroy successfully";

    // Unbind with MemTracker
    tls_mem_tracker = nullptr;

    global_env->stop();
    LOG(INFO) << process_name << " exit step " << exit_step++ << ": global env stop successfully";

    shutdown_tracer();

    LOG(INFO) << process_name << " exited successfully";
}
} // namespace starrocks
