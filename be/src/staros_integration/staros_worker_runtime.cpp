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

#ifdef USE_STAROS
#include "staros_integration/staros_worker_runtime.h"

#include <fslib/fslib_all_initializer.h>
#include <starlet.h>

#include <utility>

#include "common/config_staros_worker_fwd.h"
#include "common/gflags_utils.h"
#include "common/logging.h"
#include "common/shutdown_hook.h"
#include "common/util/table_metrics.h"
#include "fslib/star_cache_configuration.h"
#include "fslib/star_cache_handler.h"
#include "gflags/gflags.h"
#include "staros_integration/staros_worker.h"

// cachemgr thread pool size
DECLARE_int32(cachemgr_threadpool_size);
// buffer size in starlet fs buffer stream, size <= 0 means not use buffer stream.
DECLARE_int32(fs_stream_buffer_size_bytes);
// domain allow list to force starlet using s3 virtual address style
DECLARE_string(fslib_s3_virtual_address_domainlist);
// s3client factory cache capacity
DECLARE_int32(fslib_s3client_max_items);
// s3client max connections
DECLARE_int32(fslib_s3client_max_connections);
// s3client max instances per cache item, allow using multiple client instances per cache
DECLARE_int32(fslib_s3client_max_instance_per_item);
DECLARE_int32(fslib_s3client_nonread_max_retries);
DECLARE_int32(fslib_s3client_nonread_retry_scale_factor);
DECLARE_int32(fslib_s3client_connect_timeout_ms);
DECLARE_int32(fslib_s3client_request_timeout_ms);
DECLARE_bool(fslib_s3client_use_list_objects_v1);
// threadpool size for buffer prefetch task
DECLARE_int32(fs_buffer_prefetch_threadpool_size);
// switch to turn on/off buffer prefetch when read
DECLARE_bool(fs_enable_buffer_prefetch);

namespace starrocks {

namespace {

std::shared_ptr<StarOSWorker> g_worker;
std::unique_ptr<staros::starlet::Starlet> g_starlet;

} // namespace

namespace fslib = staros::starlet::fslib;

std::shared_ptr<StarOSWorker> get_staros_worker() {
    return g_worker;
}

staros::starlet::Starlet* get_starlet() {
    return g_starlet.get();
}

void init_staros_worker(const std::shared_ptr<starcache::StarCache>& star_cache,
                        TableMetricsManager* table_metrics_mgr) {
    if (g_starlet.get() != nullptr) {
        return;
    }

    if (star_cache) {
        (void)fslib::set_star_cache(star_cache);
    }

    // skip staros reinit aws sdk
    staros::starlet::fslib::skip_aws_init_api = true;

    staros::starlet::common::GFlagsUtils::UpdateFlagValue("cachemgr_threadpool_size",
                                                          std::to_string(config::starlet_cache_thread_num));
    staros::starlet::common::GFlagsUtils::UpdateFlagValue("fs_stream_buffer_size_bytes",
                                                          std::to_string(config::starlet_fs_stream_buffer_size_bytes));
    staros::starlet::common::GFlagsUtils::UpdateFlagValue("fs_enable_buffer_prefetch",
                                                          std::to_string(config::starlet_fs_read_prefetch_enable));
    staros::starlet::common::GFlagsUtils::UpdateFlagValue(
            "fs_buffer_prefetch_threadpool_size", std::to_string(config::starlet_fs_read_prefetch_threadpool_size));

    FLAGS_fslib_s3_virtual_address_domainlist = config::starlet_s3_virtual_address_domainlist;
    // use the same configuration as the external query
    FLAGS_fslib_s3client_max_connections = config::object_storage_max_connection;
    FLAGS_fslib_s3client_max_items = config::starlet_s3_client_max_cache_capacity;
    FLAGS_fslib_s3client_max_instance_per_item = config::starlet_s3_client_num_instances_per_cache;
    FLAGS_fslib_s3client_nonread_max_retries = config::starlet_fslib_s3client_nonread_max_retries;
    FLAGS_fslib_s3client_nonread_retry_scale_factor = config::starlet_fslib_s3client_nonread_retry_scale_factor;
    FLAGS_fslib_s3client_connect_timeout_ms = config::starlet_fslib_s3client_connect_timeout_ms;
    FLAGS_fslib_s3client_use_list_objects_v1 = config::s3_use_list_objects_v1;
    if (config::object_storage_request_timeout_ms >= 0) {
        FLAGS_fslib_s3client_request_timeout_ms = static_cast<int32_t>(config::object_storage_request_timeout_ms);
    }
    fslib::FLAGS_delete_files_max_key_in_batch = config::starlet_delete_files_max_key_in_batch;

    fslib::FLAGS_use_star_cache = config::starlet_use_star_cache;
    fslib::FLAGS_star_cache_async_init = config::starlet_star_cache_async_init;
    fslib::FLAGS_star_cache_mem_size_percent = config::starlet_star_cache_mem_size_percent;
    fslib::FLAGS_star_cache_mem_size_bytes = config::starlet_star_cache_mem_size_bytes;
    fslib::FLAGS_star_cache_disk_size_percent = config::starlet_star_cache_disk_size_percent;
    fslib::FLAGS_star_cache_disk_size_bytes = config::starlet_star_cache_disk_size_bytes;
    fslib::FLAGS_star_cache_block_size_bytes = config::starlet_star_cache_block_size_bytes;

    staros::starlet::StarletConfig starlet_config;
    starlet_config.rpc_port = config::starlet_port;
    g_worker = std::make_shared<StarOSWorker>(table_metrics_mgr);
    g_starlet = std::make_unique<staros::starlet::Starlet>(g_worker);
    g_starlet->init(starlet_config);
    g_starlet->start();
}

void shutdown_staros_worker() {
    g_starlet->stop();
    g_starlet.reset();
    g_worker = nullptr;

    LOG(INFO) << "Executing starlet shutdown hooks ...";
    staros::starlet::common::ShutdownHook::shutdown();
}

void set_starlet_in_shutdown() {
    auto* starlet = get_starlet();
    if (starlet) {
        starlet->on_shutdown();
    }
}

#ifdef BE_TEST
void set_staros_worker_for_test(std::shared_ptr<StarOSWorker> worker) {
    g_worker = std::move(worker);
}

std::unique_ptr<staros::starlet::Starlet> swap_starlet_for_test(std::unique_ptr<staros::starlet::Starlet> starlet) {
    std::swap(g_starlet, starlet);
    return starlet;
}
#endif

} // namespace starrocks
#endif // USE_STAROS
