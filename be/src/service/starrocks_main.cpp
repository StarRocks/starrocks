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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/service/doris_main.cpp

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

#include <aws/core/Aws.h>
#include <gperftools/malloc_extension.h>
#include <sys/file.h>
#include <unistd.h>

#include "block_cache/block_cache.h"

#if defined(LEAK_SANITIZER)
#include <sanitizer/lsan_interface.h>
#endif

#include <curl/curl.h>
#include <thrift/TOutput.h>

#include <boost/algorithm/string.hpp>

#include "agent/agent_server.h"
#include "agent/heartbeat_server.h"
#include "agent/status.h"
#include "common/config.h"
#include "common/daemon.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/pipeline/query_context.h"
#include "runtime/exec_env.h"
#include "runtime/heartbeat_flags.h"
#include "runtime/jdbc_driver_manager.h"
#include "service/backend_options.h"
#include "service/service.h"
#include "service/staros_worker.h"
#include "storage/options.h"
#include "storage/storage_engine.h"
#include "util/debug_util.h"
#include "util/logging.h"
#include "util/mem_info.h"
#include "util/thrift_rpc_helper.h"
#include "util/thrift_server.h"
#include "util/uid_util.h"

#if !_GLIBCXX_USE_CXX11_ABI
#error _GLIBCXX_USE_CXX11_ABI must be non-zero
#endif

DECLARE_bool(s2debug);

static void help(const char*);

#include <dlfcn.h>

extern "C" {
void __lsan_do_leak_check();
}

namespace starrocks {
static void thrift_output(const char* x) {
    LOG(WARNING) << "thrift internal message: " << x;
}
} // namespace starrocks

static Aws::Utils::Logging::LogLevel parse_aws_sdk_log_level(const std::string& s) {
    Aws::Utils::Logging::LogLevel levels[] = {
            Aws::Utils::Logging::LogLevel::Off,   Aws::Utils::Logging::LogLevel::Fatal,
            Aws::Utils::Logging::LogLevel::Error, Aws::Utils::Logging::LogLevel::Warn,
            Aws::Utils::Logging::LogLevel::Info,  Aws::Utils::Logging::LogLevel::Debug,
            Aws::Utils::Logging::LogLevel::Trace,
    };
    std::string slevel = boost::algorithm::to_upper_copy(s);
    Aws::Utils::Logging::LogLevel level = Aws::Utils::Logging::LogLevel::Warn;
    for (int idx = 0; idx < sizeof(levels) / sizeof(levels[0]); idx++) {
        auto s = Aws::Utils::Logging::GetLogLevelName(levels[idx]);
        if (s == slevel) {
            level = levels[idx];
            break;
        }
    }
    return level;
}

extern int meta_tool_main(int argc, char** argv);

int main(int argc, char** argv) {
    if (argc > 1 && strcmp(argv[1], "meta_tool") == 0) {
        return meta_tool_main(argc - 1, argv + 1);
    }
    bool as_cn = false;
    // Check if print version or help or cn.
    if (argc > 1) {
        if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-v") == 0) {
            puts(starrocks::get_build_version(false).c_str());
            exit(0);
        } else if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0) {
            help(basename(argv[0]));
            exit(0);
        } else if (strcmp(argv[1], "--cn") == 0) {
            as_cn = true;
        }
    }

    if (getenv("STARROCKS_HOME") == nullptr) {
        fprintf(stderr, "you need set STARROCKS_HOME environment variable.\n");
        exit(-1);
    }

    if (getenv("TCMALLOC_HEAP_LIMIT_MB") == nullptr) {
        fprintf(stderr,
                "Environment variable TCMALLOC_HEAP_LIMIT_MB is not set,"
                " maybe you forgot to replace bin directory\n");
        exit(-1);
    }

    // S2 will crashes when deserialization fails and FLAGS_s2debug was true.
    FLAGS_s2debug = false;

    using starrocks::Status;
    using std::string;

    // Open pid file, obtain file lock and save pid.
    string pid_file = string(getenv("PID_DIR"));
    if (as_cn) {
        pid_file += "/cn.pid";
    } else {
        pid_file += "/be.pid";
    }
    int fd = open(pid_file.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    if (fd < 0) {
        fprintf(stderr, "fail to create pid file.");
        exit(-1);
    }

    string pid = std::to_string((long)getpid());
    pid += "\n";
    size_t length = write(fd, pid.c_str(), pid.size());
    if (length != pid.size()) {
        fprintf(stderr, "fail to save pid into pid file.");
        exit(-1);
    }

    // Descriptor will be leaked if failing to close fd.
    if (::close(fd) < 0) {
        fprintf(stderr, "failed to close fd of pidfile.");
        exit(-1);
    }

    string conffile = string(getenv("STARROCKS_HOME"));
    if (as_cn) {
        conffile += "/conf/cn.conf";
    } else {
        conffile += "/conf/be.conf";
    }
    if (!starrocks::config::init(conffile.c_str(), true)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }

#if defined(ENABLE_STATUS_FAILED)
    // read range of source code for inject errors.
    starrocks::Status::access_directory_of_inject();
#endif

#if !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && !defined(THREAD_SANITIZER) && !defined(USE_JEMALLOC)
    // Aggressive decommit is required so that unused pages in the TCMalloc page heap are
    // not backed by physical pages and do not contribute towards memory consumption.
    //
    //  2020-08-31: Disable aggressive decommit,  which will decrease the performance of
    //  memory allocation and deallocation.
    // MallocExtension::instance()->SetNumericProperty("tcmalloc.aggressive_memory_decommit", 1);

    // Change the total TCMalloc thread cache size if necessary.
    if (!MallocExtension::instance()->SetNumericProperty("tcmalloc.max_total_thread_cache_bytes",
                                                         starrocks::config::tc_max_total_thread_cache_bytes)) {
        fprintf(stderr, "Failed to change TCMalloc total thread cache size.\n");
        return -1;
    }
#endif

    Aws::SDKOptions aws_sdk_options;
    if (starrocks::config::aws_sdk_logging_trace_enabled) {
        auto level = parse_aws_sdk_log_level(starrocks::config::aws_sdk_logging_trace_level);
        std::cerr << "enable aws sdk logging trace. log level = " << Aws::Utils::Logging::GetLogLevelName(level)
                  << "\n";
        aws_sdk_options.loggingOptions.logLevel = level;
    }
    if (starrocks::config::aws_sdk_enable_compliant_rfc3986_encoding) {
        aws_sdk_options.httpOptions.compliantRfc3986Encoding = true;
    }
    Aws::InitAPI(aws_sdk_options);

    std::vector<starrocks::StorePath> paths;
    auto olap_res = starrocks::parse_conf_store_paths(starrocks::config::storage_root_path, &paths);
    if (!olap_res.ok() && !as_cn) {
        LOG(FATAL) << "parse config storage path failed, path=" << starrocks::config::storage_root_path;
        exit(-1);
    }

    auto it = paths.begin();
    for (; it != paths.end();) {
        if (!starrocks::check_datapath_rw(it->path)) {
            if (starrocks::config::ignore_broken_disk) {
                LOG(WARNING) << "read write test file failed, path=" << it->path;
                it = paths.erase(it);
            } else {
                LOG(FATAL) << "read write test file failed, path=" << it->path;
                exit(-1);
            }
        } else {
            ++it;
        }
    }

    if (paths.empty()) {
        if (as_cn) {
#ifdef USE_STAROS
            starrocks::config::starlet_cache_dir = "";
#endif
        } else {
            LOG(FATAL) << "All disks are broken, exit.";
            exit(-1);
        }
    }

    // Initilize libcurl here to avoid concurrent initialization.
    auto curl_ret = curl_global_init(CURL_GLOBAL_ALL);
    if (curl_ret != 0) {
        LOG(FATAL) << "fail to initialize libcurl, curl_ret=" << curl_ret;
        exit(-1);
    }
    // Add logger for thrift internal.
    apache::thrift::GlobalOutput.setOutputFunction(starrocks::thrift_output);

    std::unique_ptr<starrocks::Daemon> daemon(new starrocks::Daemon());
    daemon->init(argc, argv, paths);

    // init jdbc driver manager
    EXIT_IF_ERROR(starrocks::JDBCDriverManager::getInstance()->init(std::string(getenv("STARROCKS_HOME")) +
                                                                    "/lib/jdbc_drivers"));

    if (!starrocks::BackendOptions::init()) {
        exit(-1);
    }

    auto* exec_env = starrocks::ExecEnv::GetInstance();
    EXIT_IF_ERROR(exec_env->init_mem_tracker());

    // Init and open storage engine.
    starrocks::EngineOptions options;
    options.store_paths = paths;
    options.backend_uid = starrocks::UniqueId::gen_uid();
    options.compaction_mem_tracker = exec_env->compaction_mem_tracker();
    options.update_mem_tracker = exec_env->update_mem_tracker();
    options.need_write_cluster_id = !as_cn;
    starrocks::StorageEngine* engine = nullptr;

    auto st = starrocks::StorageEngine::open(options, &engine);
    if (!st.ok()) {
        LOG(FATAL) << "fail to open StorageEngine, res=" << st.get_error_msg();
        exit(-1);
    }

    // Init exec env.
    EXIT_IF_ERROR(starrocks::ExecEnv::init(exec_env, paths, as_cn));
    engine->set_heartbeat_flags(exec_env->heartbeat_flags());

    // Start all background threads of storage engine.
    // SHOULD be called after exec env is initialized.
    EXIT_IF_ERROR(engine->start_bg_threads());

    // Begin to start Heartbeat services
    starrocks::ThriftRpcHelper::setup(exec_env);
    auto res = starrocks::create_heartbeat_server(exec_env, starrocks::config::heartbeat_service_port,
                                                  starrocks::config::heartbeat_service_thread_count);
    CHECK(res.ok()) << res.status();
    auto heartbeat_thrift_server = std::move(res).value();

    starrocks::Status status = heartbeat_thrift_server->start();
    if (!status.ok()) {
        LOG(ERROR) << "StarRocks BE HeartBeat Service did not start correctly. Error=" << status.to_string();
        starrocks::shutdown_logging();
        exit(1);
    }

#ifdef USE_STAROS
    starrocks::init_staros_worker();
#endif

    if (!starrocks::config::datacache_enable && starrocks::config::block_cache_enable) {
        starrocks::config::datacache_enable = true;
        starrocks::config::datacache_mem_size = std::to_string(starrocks::config::block_cache_mem_size);
        starrocks::config::datacache_disk_size = std::to_string(starrocks::config::block_cache_disk_size);
        starrocks::config::datacache_disk_path = starrocks::config::block_cache_disk_path;
        starrocks::config::datacache_meta_path = starrocks::config::block_cache_meta_path;
        starrocks::config::datacache_block_size = starrocks::config::block_cache_block_size;
        starrocks::config::datacache_max_concurrent_inserts = starrocks::config::block_cache_max_concurrent_inserts;
        starrocks::config::datacache_checksum_enable = starrocks::config::block_cache_checksum_enable;
        starrocks::config::datacache_direct_io_enable = starrocks::config::block_cache_direct_io_enable;
        starrocks::config::datacache_engine = starrocks::config::block_cache_engine;
        LOG(WARNING) << "The configuration items prefixed with `block_cache_` will be deprecated soon"
                     << ", you'd better use the configuration items prefixed `datacache` instead!";
    }

#if !defined(WITH_CACHELIB) && !defined(WITH_STARCACHE)
    if (starrocks::config::datacache_enable) {
        starrocks::config::datacache_enable = false;
    }
#endif

    if (starrocks::config::datacache_enable) {
        starrocks::BlockCache* cache = starrocks::BlockCache::instance();

        starrocks::CacheOptions cache_options;
        int64_t mem_limit = starrocks::MemInfo::physical_mem();
        if (exec_env->process_mem_tracker()->has_limit()) {
            mem_limit = exec_env->process_mem_tracker()->limit();
        }
        cache_options.mem_space_size = starrocks::parse_mem_size(starrocks::config::datacache_mem_size, mem_limit);

        std::vector<std::string> paths;
        EXIT_IF_ERROR(starrocks::parse_conf_datacache_paths(starrocks::config::datacache_disk_path, &paths));
        for (auto& p : paths) {
            int64_t disk_size = starrocks::parse_disk_size(p, starrocks::config::datacache_disk_size);
            if (disk_size < 0) {
                LOG(FATAL) << "invalid disk size for datacache: " << disk_size;
                exit(-1);
            }
            cache_options.disk_spaces.push_back({.path = p, .size = static_cast<size_t>(disk_size)});
        }

        // Adjust the default engine based on build switches.
        if (starrocks::config::datacache_engine == "") {
#if defined(WITH_STARCACHE)
            starrocks::config::datacache_engine = "starcache";
#else
            starrocks::config::datacache_engine = "cachelib";
#endif
        }
        cache_options.meta_path = starrocks::config::datacache_meta_path;
        cache_options.block_size = starrocks::config::datacache_block_size;
        cache_options.max_flying_memory_mb = starrocks::config::datacache_max_flying_memory_mb;
        cache_options.max_concurrent_inserts = starrocks::config::datacache_max_concurrent_inserts;
        cache_options.enable_checksum = starrocks::config::datacache_checksum_enable;
        cache_options.enable_direct_io = starrocks::config::datacache_direct_io_enable;
        cache_options.enable_cache_adaptor = starrocks::config::datacache_adaptor_enable;
        cache_options.skip_read_factor = starrocks::config::datacache_skip_read_factor;
        cache_options.engine = starrocks::config::datacache_engine;
        EXIT_IF_ERROR(cache->init(cache_options));
    }

    // cn need to support all ops for cloudnative table, so just start_be
    start_be();

    if (starrocks::k_starrocks_exit_quick.load()) {
        LOG(INFO) << "BE is shutting down，will exit quickly";
        exit(0);
    }

    daemon->stop();
    daemon.reset();

#ifdef USE_STAROS
    starrocks::shutdown_staros_worker();
#endif

#if defined(WITH_CACHELIB) || defined(WITH_STARCACHE)
    if (starrocks::config::datacache_enable) {
        starrocks::BlockCache::instance()->shutdown();
    }
#endif

    Aws::ShutdownAPI(aws_sdk_options);

    heartbeat_thrift_server->stop();
    heartbeat_thrift_server->join();

    exec_env->agent_server()->stop();

    starrocks::ExecEnv::stop(exec_env);
    engine->stop();
    delete engine;
    starrocks::ExecEnv::destroy(exec_env);

    return 0;
}

static void help(const char* progname) {
    printf("%s is the StarRocks backend server.\n\n", progname);
    printf("Usage:\n  %s [OPTION]...\n\n", progname);
    printf("Options:\n");
    printf("      --cn           start as compute node\n");
    printf("  -v, --version      output version information, then exit\n");
    printf("  -?, --help         show this help, then exit\n");
}
