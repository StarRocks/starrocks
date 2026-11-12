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

#include <fmt/format.h>
#include <fslib/fslib_all_initializer.h>
#include <starlet.h>

#include <algorithm>
#include <limits>
#include <utility>

#include "common/config_staros_worker_fwd.h"
#include "common/gflags_utils.h"
#include "common/logging.h"
#include "common/shutdown_hook.h"
#include "common/system/cpu_info.h"
#include "common/util/table_metrics.h"
#include "compute_env/staros/staros_status.h"
#include "compute_env/staros/staros_worker.h"
#include "fs/remote_read_context.h"
#include "fslib/cache_fill.h"
#include "fslib/remote_read_scheduler.h"
#include "fslib/star_cache_configuration.h"
#include "fslib/star_cache_handler.h"
#include "gflags/gflags.h"
#include "runtime/current_thread.h"

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
// Cut S3 multipart uploads into parts of exactly fslib_s3_min_upload_part_size bytes.
DECLARE_bool(fslib_s3_multipart_equal_part_size);
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

    // Shared with the BE S3 filesystem, hence no `starlet_` prefix on the BE config.
    FLAGS_fslib_s3_multipart_equal_part_size = config::s3_multipart_equal_part_size;
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

namespace {

bool remote_io_scheduler_has_cache() {
    return config::datacache_enable && config::starlet_use_star_cache;
}

// A block fill's buffer is allocated on the scheduler's IO pool, which charges no query, and released by the last
// reader of the block, a scan thread that charges its query. Charged to the query, every block it waited for would
// credit its tracker with memory it never allocated. Both ends are charged to the process instead, as the writes to
// the cache are; the fills in flight are bounded by the scheduler.
char* allocate_fill_buffer(size_t size) {
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(nullptr);
    return new char[size];
}

void release_fill_buffer(char* buffer) {
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(nullptr);
    delete[] buffer;
}

bool update_starlet_flag(const char* name, const std::string& value) {
    return !staros::starlet::common::GFlagsUtils::UpdateFlagValue(name, value).empty();
}

} // namespace

Status validate_remote_io_scheduler_config() {
    if (!config::remote_io_scheduler_enable || !remote_io_scheduler_has_cache()) {
        return Status::OK();
    }
    if (config::remote_read_io_threads < 0) {
        return Status::InvalidArgument(
                fmt::format("remote_read_io_threads must not be negative (got {})", config::remote_read_io_threads));
    }
    if (config::remote_read_query_weight <= 0 || config::remote_read_load_weight <= 0 ||
        config::remote_read_compaction_weight <= 0 || config::remote_read_background_weight <= 0) {
        return Status::InvalidArgument(
                fmt::format("remote_read_query_weight, remote_read_load_weight, remote_read_compaction_weight and "
                            "remote_read_background_weight must be positive (got {}, {}, {} and {})",
                            config::remote_read_query_weight, config::remote_read_load_weight,
                            config::remote_read_compaction_weight, config::remote_read_background_weight));
    }
    if (config::remote_read_adaptive_min_inflight <= 0 || config::remote_read_weight_decay_ios < 0 ||
        !(config::remote_read_weight_decay_min > 0 && config::remote_read_weight_decay_min <= 1)) {
        return Status::InvalidArgument(fmt::format(
                "remote_read_adaptive_min_inflight must be positive, remote_read_weight_decay_ios not negative and "
                "remote_read_weight_decay_min in (0, 1] (got {}, {} and {})",
                config::remote_read_adaptive_min_inflight, config::remote_read_weight_decay_ios,
                config::remote_read_weight_decay_min));
    }
    return Status::OK();
}

fslib::RemoteReadSchedulerOptions remote_read_scheduler_options() {
    fslib::RemoteReadSchedulerOptions opts;
    // One IO is a GET of a cache block: what a scan reads ahead and what a cache miss fetches.
    opts.block_bytes = std::max<int64_t>(1, config::starlet_star_cache_block_size_bytes);
    // As many threads as the lake scan threads by default: without the scheduler, the scan threads blocked in their
    // reads are what bounds the GETs in flight.
    const auto scan_threads = std::max<int32_t>(
            1, static_cast<int32_t>(config::pipeline_connector_scan_thread_num_per_cpu * CpuInfo::num_cores()));
    opts.io_threads = config::remote_read_io_threads > 0 ? config::remote_read_io_threads : scan_threads;
    opts.adaptive_limit = config::remote_read_adaptive_limit;
    opts.adaptive_min_inflight = config::remote_read_adaptive_min_inflight;
    opts.weight_decay_ios = config::remote_read_weight_decay_ios;
    opts.weight_decay_min = config::remote_read_weight_decay_min;
    auto weight = [&](fslib::ReadWorkload workload) -> int32_t& {
        return opts.workload_weights[static_cast<size_t>(workload)];
    };
    weight(fslib::ReadWorkload::QUERY) = config::remote_read_query_weight;
    weight(fslib::ReadWorkload::LOAD) = config::remote_read_load_weight;
    weight(fslib::ReadWorkload::COMPACTION) = config::remote_read_compaction_weight;
    weight(fslib::ReadWorkload::OTHER_BACKGROUND) = config::remote_read_background_weight;
    return opts;
}

Status enable_remote_io_scheduler() {
    if (!config::remote_io_scheduler_enable) {
        LOG(INFO) << "remote I/O scheduler disabled (remote_io_scheduler_enable=false); remote reads run unscheduled";
        return Status::OK();
    }
    if (!remote_io_scheduler_has_cache()) {
        // The scheduler fills the cache blocks it fetches; without the cache the remote reads keep their old paths.
        LOG(WARNING) << "remote I/O scheduler not enabled: it needs datacache_enable=true and "
                        "starlet_use_star_cache=true (got datacache_enable="
                     << config::datacache_enable << ", starlet_use_star_cache=" << config::starlet_use_star_cache
                     << "); remote reads run unscheduled";
        return Status::OK();
    }
    RETURN_IF_ERROR(validate_remote_io_scheduler_config());
    const fslib::RemoteReadSchedulerOptions opts = remote_read_scheduler_options();
    // Installed before the scheduler exists, so that no fill buffer is ever allocated with another allocator.
    if (auto st = fslib::set_fill_buffer_allocator(
                fslib::FillBufferAllocator{&allocate_fill_buffer, &release_fill_buffer});
        !st.ok()) {
        LOG(WARNING) << "remote I/O scheduler not enabled: failed to install the fill buffer allocator: " << st
                     << "; remote reads run unscheduled";
        return Status::OK();
    }
    // The scheduler replaces the starlet buffer prefetch; running both would let two policies decide what to fetch.
    // The configured value is kept, so a restart without the scheduler brings it back.
    if (!update_starlet_flag("fs_enable_buffer_prefetch", "false")) {
        LOG(WARNING) << "remote I/O scheduler not enabled: failed to turn starlet fs_enable_buffer_prefetch off; "
                        "remote reads run unscheduled";
        return Status::OK();
    }
    if (auto st = fslib::RemoteReadScheduler::init(opts); !st.ok()) {
        (void)update_starlet_flag("fs_enable_buffer_prefetch", std::to_string(config::starlet_fs_read_prefetch_enable));
        LOG(WARNING) << "remote I/O scheduler not enabled: " << st << "; remote reads run unscheduled";
        return Status::OK();
    }
    // The scheduler also replaces the coalesced lake read. Turning the config itself off, before any lake read, keeps
    // every reader of it on one value; be.conf keeps its own, so a restart without the scheduler brings it back.
    const bool configured_io_coalesce_lake_read = config::io_coalesce_lake_read_enable;
    config::io_coalesce_lake_read_enable = false;
    g_remote_io_scheduler_enabled.store(true);
    LOG(INFO) << "remote I/O scheduler enabled: io_threads=" << opts.io_threads
              << " adaptive_limit=" << opts.adaptive_limit << " adaptive_min_inflight=" << opts.adaptive_min_inflight
              << " block_bytes=" << opts.block_bytes << " weight_decay_ios=" << opts.weight_decay_ios
              << " weight_decay_min=" << opts.weight_decay_min
              << " query:load:compaction:background=" << config::remote_read_query_weight << ":"
              << config::remote_read_load_weight << ":" << config::remote_read_compaction_weight << ":"
              << config::remote_read_background_weight
              << " remote_prefetch_window_blocks=" << config::remote_prefetch_window_blocks
              << "; effective starlet_fs_read_prefetch_enable=false (configured "
              << config::starlet_fs_read_prefetch_enable << "), io_coalesce_lake_read_enable set to false (configured "
              << configured_io_coalesce_lake_read << ")";
    return Status::OK();
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

    // Before the starlet runtime exists, so that no read stream is ever created unscheduled.
    EXIT_IF_ERROR(enable_remote_io_scheduler());

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
