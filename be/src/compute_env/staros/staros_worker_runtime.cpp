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
#include "compute_env/staros/staros_worker_runtime.h"

#include <fslib/fslib_all_initializer.h>
#include <starlet.h>

#include <limits>
#include <utility>

#include "common/config_staros_worker_fwd.h"
#include "common/gflags_utils.h"
#include "common/logging.h"
#include "common/shutdown_hook.h"
#include "common/util/table_metrics.h"
#include "compute_env/staros/staros_status.h"
#include "compute_env/staros/staros_worker.h"
#include "fslib/star_cache_configuration.h"
#include "fslib/star_cache_handler.h"
#include "gflags/gflags.h"

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
// Object-store upload thresholds; see the starlet_fslib_* BE configs of the same names.
DECLARE_int64(fslib_s3_max_single_part_size);
DECLARE_int64(fslib_s3_min_upload_part_size);
DECLARE_int64(fslib_gs_max_single_part_size);
DECLARE_int64(fslib_azure_storage_max_single_part_size);
DECLARE_int64(fslib_azure_storage_min_upload_part_size);
// threadpool size for buffer prefetch task
DECLARE_int32(fs_buffer_prefetch_threadpool_size);
// switch to turn on/off buffer prefetch when read
DECLARE_bool(fs_enable_buffer_prefetch);

namespace starrocks {

namespace {

// Both globals are read by any thread that touches a starlet-backed filesystem and are released by
// `shutdown_staros_worker()` while such threads may still be running, so every access goes through
// `std::atomic_load`/`std::atomic_store`/`std::atomic_exchange`. A plain read racing with the reset
// is undefined behaviour on its own; going through the atomic accessors makes the reader observe
// either a strong reference that keeps the object alive for the whole operation, or a null pointer
// it must handle. `g_starlet` is a `shared_ptr` rather than a `unique_ptr` for exactly that reason:
// a raw pointer would dangle across the blocking starmgr RPCs its callers make.
std::shared_ptr<StarOSWorker> g_worker;
std::shared_ptr<staros::starlet::Starlet> g_starlet;

// starlet's validator only runs through SetCommandLineOption, so apply it explicitly here.
// Calling starlet's own predicate avoids duplicating its rule, while the typed FLAGS_ assignment
// keeps compile-time name checking and keeps the defining archive member linked.
void apply_positive_int64_starlet_flag(const char* be_config, const char* flag_name, int64_t& flag, int64_t value) {
    if (!staros::starlet::common::validate_positive_int64(flag_name, value)) {
        LOG(WARNING) << "invalid value for BE config " << be_config << " (starlet flag " << flag_name << "): " << value
                     << "; not applied, effective value remains " << flag;
        return;
    }
    flag = value;
}

} // namespace

void apply_starlet_upload_threshold_configs() {
#define APPLY_STARLET_UPLOAD_THRESHOLD(BE_CONFIG, STARLET_FLAG) \
    apply_positive_int64_starlet_flag(#BE_CONFIG, #STARLET_FLAG, FLAGS_##STARLET_FLAG, config::BE_CONFIG)

    APPLY_STARLET_UPLOAD_THRESHOLD(starlet_fslib_s3_max_single_part_size, fslib_s3_max_single_part_size);
    APPLY_STARLET_UPLOAD_THRESHOLD(starlet_fslib_s3_min_upload_part_size, fslib_s3_min_upload_part_size);
    APPLY_STARLET_UPLOAD_THRESHOLD(starlet_fslib_gcs_max_single_part_size, fslib_gs_max_single_part_size);
    APPLY_STARLET_UPLOAD_THRESHOLD(starlet_fslib_azure_storage_max_single_part_size,
                                   fslib_azure_storage_max_single_part_size);
    APPLY_STARLET_UPLOAD_THRESHOLD(starlet_fslib_azure_storage_min_upload_part_size,
                                   fslib_azure_storage_min_upload_part_size);
#undef APPLY_STARLET_UPLOAD_THRESHOLD
}

namespace fslib = staros::starlet::fslib;

std::optional<int32_t> starlet_request_timeout_ms(int64_t configured_timeout_ms, bool use_poco_client) {
    if (configured_timeout_ms > std::numeric_limits<int32_t>::max()) {
        return std::nullopt;
    }
    if (configured_timeout_ms < 0) {
        // Curl uses zero as its SDK default. Poco needs a negative sentinel to distinguish the
        // unset value from an explicit zero, which disables its send and receive timeouts.
        return use_poco_client ? -1 : 0;
    }
    return static_cast<int32_t>(configured_timeout_ms);
}

std::shared_ptr<StarOSWorker> get_staros_worker() {
    // May return nullptr once `shutdown_staros_worker()` has run. Callers must check it: an
    // in-flight load can still reach a starlet filesystem after worker teardown, and dereferencing
    // the null worker there crashes the process instead of failing the load.
    return std::atomic_load(&g_worker);
}

std::shared_ptr<staros::starlet::Starlet> get_starlet() {
    // May return nullptr once `shutdown_staros_worker()` has run. The returned strong reference
    // keeps the runtime alive for the whole operation, so callers must hold it rather than cache a
    // raw pointer across a call.
    return std::atomic_load(&g_starlet);
}

void init_staros_worker(const std::shared_ptr<starcache::StarCache>& star_cache,
                        TableMetricsManager* table_metrics_mgr) {
    if (std::atomic_load(&g_starlet) != nullptr) {
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
    if (auto timeout = starlet_request_timeout_ms(config::object_storage_request_timeout_ms,
                                                  config::enable_poco_client_for_aws_sdk)) {
        FLAGS_fslib_s3client_request_timeout_ms = *timeout;
    }
    fslib::FLAGS_delete_files_max_key_in_batch = config::starlet_delete_files_max_key_in_batch;
    fslib::FLAGS_write_cache_rpc_timeout_ms = config::starlet_cache_replication_timeout_ms;
    apply_starlet_upload_threshold_configs();

    // Goes through UpdateFlagValue rather than a typed FLAGS_ assignment: starlet registers an update
    // hook on this flag that resolves the string to the enum its heartbeat path actually reads, and
    // only SetCommandLineOption fires that hook. Assigning the flag directly would leave the
    // heartbeat on "none" whatever this is set to.
    if (staros::starlet::common::GFlagsUtils::UpdateFlagValue("starmgr_client_compression_type",
                                                              config::starlet_starmgr_client_compression_type.value())
                .empty()) {
        LOG(WARNING) << "Failed to apply BE config starlet_starmgr_client_compression_type="
                     << config::starlet_starmgr_client_compression_type << "; worker heartbeats stay uncompressed";
    }

    fslib::FLAGS_use_star_cache = config::starlet_use_star_cache;
    fslib::FLAGS_star_cache_async_init = config::starlet_star_cache_async_init;
    fslib::FLAGS_star_cache_skip_fresh_block_checksum_verification =
            config::starlet_star_cache_skip_fresh_block_checksum_verification;
    fslib::FLAGS_star_cache_mem_size_percent = config::starlet_star_cache_mem_size_percent;
    fslib::FLAGS_star_cache_mem_size_bytes = config::starlet_star_cache_mem_size_bytes;
    fslib::FLAGS_star_cache_disk_size_percent = config::starlet_star_cache_disk_size_percent;
    fslib::FLAGS_star_cache_disk_size_bytes = config::starlet_star_cache_disk_size_bytes;
    fslib::FLAGS_star_cache_block_size_bytes = config::starlet_star_cache_block_size_bytes;

    staros::starlet::StarletConfig starlet_config;
    starlet_config.rpc_port = config::starlet_port;
    auto worker = std::make_shared<StarOSWorker>(table_metrics_mgr);
    auto starlet = std::make_shared<staros::starlet::Starlet>(worker);
    // Publish the worker only after the starlet runtime exists, so a reader that observes a
    // non-null worker never finds a null starlet behind it.
    std::atomic_store(&g_starlet, starlet);
    std::atomic_store(&g_worker, std::move(worker));
    starlet->init(starlet_config);
    starlet->start();
}

void shutdown_staros_worker() {
    // Retire both globals before tearing anything down, the reverse of the publish order in
    // `init_staros_worker()`, so an operation that has not fetched them yet sees null and fails
    // with a status instead of entering a runtime that is going away.
    //
    // Neither object is necessarily destroyed here. An operation that fetched them first holds a
    // strong reference and keeps them alive until it finishes; this function only drops the
    // process-wide one. `Starlet::stop()` is idempotent and `~Starlet()` does nothing beyond it
    // once stopped, so a deferred destruction on the last in-flight thread is cheap and safe.
    LOG(INFO) << "Retiring the global StarOS worker and starlet runtime, later filesystem "
                 "operations will fail with a status ...";
    std::atomic_store(&g_worker, std::shared_ptr<StarOSWorker>());
    auto starlet = std::atomic_exchange(&g_starlet, std::shared_ptr<staros::starlet::Starlet>());
    if (starlet != nullptr) {
        starlet->stop();
    }

    LOG(INFO) << "Executing starlet shutdown hooks ...";
    staros::starlet::common::ShutdownHook::shutdown();
}

void set_starlet_in_shutdown() {
    auto starlet = get_starlet();
    if (starlet) {
        starlet->on_shutdown();
    }
}

Status batch_update_tablet_replica_info(const std::vector<uint64_t>& tablet_ids) {
    return to_status(StarOSWorker::batch_update_shard_replica_info(tablet_ids));
}

Status staros_need_warmup_tablet(int64_t tablet_id) {
    auto worker = get_staros_worker();
    if (worker == nullptr) {
        return Status::ServiceUnavailable("StarOS worker is not initialized");
    }
    return worker->need_warmup_shard(static_cast<uint64_t>(tablet_id));
}

staros::WarmupLevel staros_worker_warmup_level() {
    auto worker = get_staros_worker();
    if (worker == nullptr) {
        return staros::WarmupLevel::WARMUP_NOT_SET;
    }
    return worker->worker_group_property().warmup_level();
}

#ifdef BE_TEST
void set_staros_worker_for_test(std::shared_ptr<StarOSWorker> worker) {
    std::atomic_store(&g_worker, std::move(worker));
}

std::shared_ptr<staros::starlet::Starlet> swap_starlet_for_test(std::shared_ptr<staros::starlet::Starlet> starlet) {
    return std::atomic_exchange(&g_starlet, std::move(starlet));
}
#endif

} // namespace starrocks
#endif // USE_STAROS
