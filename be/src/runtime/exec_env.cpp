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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/exec_env.cpp

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

#include "runtime/exec_env.h"

#include <cctype>
#include <memory>
#include <thread>

#include "agent/agent_server.h"
#include "agent/master_info.h"
#include "cache/block_cache/block_cache.h"
#include "cache/object_cache/lrucache_module.h"
#include "common/config.h"
#include "common/configbase.h"
#include "common/logging.h"
#include "exec/pipeline/driver_limiter.h"
#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/pipeline/query_context.h"
#include "exec/pipeline/schedule/pipeline_timer.h"
#include "exec/spill/dir_manager.h"
#include "exec/workgroup/pipeline_executor_set.h"
#include "exec/workgroup/scan_executor.h"
#include "exec/workgroup/scan_task_queue.h"
#include "exec/workgroup/work_group.h"
#include "fs/fs_s3.h"
#include "gen_cpp/BackendService.h"
#include "gen_cpp/TFileBrokerService.h"
#include "gutil/strings/join.h"
#include "gutil/strings/split.h"
#include "gutil/strings/strip.h"
#include "gutil/strings/substitute.h"
#include "runtime/batch_write/batch_write_mgr.h"
#include "runtime/broker_mgr.h"
#include "runtime/client_cache.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/diagnose_daemon.h"
#include "runtime/dummy_load_path_mgr.h"
#include "runtime/external_scan_context_mgr.h"
#include "runtime/fragment_mgr.h"
#include "runtime/heartbeat_flags.h"
#include "runtime/load_channel_mgr.h"
#include "runtime/load_path_mgr.h"
#include "runtime/mem_tracker.h"
#include "runtime/memory/mem_chunk_allocator.h"
#include "runtime/profile_report_worker.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/result_queue_mgr.h"
#include "runtime/routine_load/routine_load_task_executor.h"
#include "runtime/runtime_filter_cache.h"
#include "runtime/runtime_filter_worker.h"
#include "runtime/small_file_mgr.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/stream_load/stream_load_executor.h"
#include "runtime/stream_load/transaction_mgr.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/replication_txn_manager.h"
#include "storage/lake/starlet_location_provider.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/update_manager.h"
#include "storage/page_cache.h"
#include "storage/storage_engine.h"
#include "storage/tablet_schema_map.h"
#include "storage/update_manager.h"
#include "udf/python/env.h"
#include "util/bfd_parser.h"
#include "util/brpc_stub_cache.h"
#include "util/cpu_info.h"
#include "util/mem_info.h"
#include "util/parse_util.h"
#include "util/pretty_printer.h"
#include "util/priority_thread_pool.hpp"
#include "util/starrocks_metrics.h"

#ifdef STARROCKS_JIT_ENABLE
#include "exprs/jit/jit_engine.h"
#endif

#ifdef WITH_STARCACHE
#include "cache/object_cache/starcache_module.h"
#endif

namespace starrocks {

// Calculate the total memory limit of all load tasks on this BE
static int64_t calc_max_load_memory(int64_t process_mem_limit) {
    if (process_mem_limit == -1) {
        // no limit
        return -1;
    }
    int32_t max_load_memory_percent = config::load_process_max_memory_limit_percent;
    int64_t max_load_memory_bytes = process_mem_limit * max_load_memory_percent / 100;
    return std::min<int64_t>(max_load_memory_bytes, config::load_process_max_memory_limit_bytes);
}

static int64_t calc_max_compaction_memory(int64_t process_mem_limit) {
    int64_t limit = config::compaction_max_memory_limit;
    int64_t percent = config::compaction_max_memory_limit_percent;

    if (config::compaction_memory_limit_per_worker < 0) {
        config::compaction_memory_limit_per_worker = 2147483648; // 2G
    }

    if (process_mem_limit < 0) {
        return -1;
    }
    if (limit < 0) {
        limit = process_mem_limit;
    }
    if (percent < 0 || percent > 100) {
        percent = 100;
    }
    return std::min<int64_t>(limit, process_mem_limit * percent / 100);
}

static int64_t calc_max_consistency_memory(int64_t process_mem_limit) {
    int64_t limit = ParseUtil::parse_mem_spec(config::consistency_max_memory_limit, process_mem_limit);
    int64_t percent = config::consistency_max_memory_limit_percent;

    if (process_mem_limit < 0) {
        return -1;
    }
    if (limit < 0) {
        limit = process_mem_limit;
    }
    if (percent < 0 || percent > 100) {
        percent = 100;
    }
    return std::min<int64_t>(limit, process_mem_limit * percent / 100);
}

bool GlobalEnv::_is_init = false;

bool GlobalEnv::is_init() {
    return _is_init;
}

Status GlobalEnv::init() {
    RETURN_IF_ERROR(_init_mem_tracker());
    _is_init = true;
    return Status::OK();
}

Status GlobalEnv::_init_mem_tracker() {
    MemTracker::init_type_label_map();

    int64_t bytes_limit = 0;
    std::stringstream ss;
    // --mem_limit="" means no memory limit
    bytes_limit = ParseUtil::parse_mem_spec(config::mem_limit, MemInfo::physical_mem());
    // use 90% of mem_limit as the soft mem limit of BE
    bytes_limit = bytes_limit * 0.9;
    if (bytes_limit <= 0) {
        ss << "Failed to parse mem limit from '" + config::mem_limit + "'.";
        return Status::InternalError(ss.str());
    }

    if (bytes_limit > MemInfo::physical_mem()) {
        LOG(WARNING) << "Memory limit " << PrettyPrinter::print(bytes_limit, TUnit::BYTES)
                     << " exceeds physical memory of " << PrettyPrinter::print(MemInfo::physical_mem(), TUnit::BYTES)
                     << ". Using physical memory instead";
        bytes_limit = MemInfo::physical_mem();
    }

    if (bytes_limit <= 0) {
        ss << "Invalid mem limit: " << bytes_limit;
        return Status::InternalError(ss.str());
    }

    _process_mem_tracker = regist_tracker(MemTrackerType::PROCESS, bytes_limit, nullptr);
    _jemalloc_metadata_tracker = regist_tracker(MemTrackerType::JEMALLOC, -1, process_mem_tracker());
    int64_t query_pool_mem_limit =
            calc_max_query_memory(_process_mem_tracker->limit(), config::query_max_memory_limit_percent);
    _query_pool_mem_tracker = regist_tracker(MemTrackerType::QUERY_POOL, query_pool_mem_limit, process_mem_tracker());
    int64_t query_pool_spill_limit = query_pool_mem_limit * config::query_pool_spill_mem_limit_threshold;
    _query_pool_mem_tracker->set_reserve_limit(query_pool_spill_limit);
    _connector_scan_pool_mem_tracker = regist_tracker(MemTrackerType::CONNECTOR_SCAN, query_pool_mem_limit, nullptr);
    _connector_scan_pool_mem_tracker->set_level(2);

    int64_t load_mem_limit = calc_max_load_memory(_process_mem_tracker->limit());
    _load_mem_tracker = regist_tracker(MemTrackerType::LOAD, load_mem_limit, process_mem_tracker());

    // Metadata statistics memory statistics do not use new mem statistics framework with hook
    _metadata_mem_tracker = regist_tracker(MemTrackerType::METADATA, -1, nullptr);
    _metadata_mem_tracker->set_level(2);

    _tablet_metadata_mem_tracker = regist_tracker(MemTrackerType::TABLET_METADATA, -1, metadata_mem_tracker());
    _rowset_metadata_mem_tracker = regist_tracker(MemTrackerType::ROWSET_METADATA, -1, metadata_mem_tracker());
    _segment_metadata_mem_tracker = regist_tracker(MemTrackerType::SEGMENT_METADATA, -1, metadata_mem_tracker());
    _column_metadata_mem_tracker = regist_tracker(MemTrackerType::COLUMN_METADATA, -1, metadata_mem_tracker());

    _tablet_schema_mem_tracker = regist_tracker(MemTrackerType::TABLET_SCHEMA, -1, tablet_metadata_mem_tracker());
    _segment_zonemap_mem_tracker = regist_tracker(MemTrackerType::SEGMENT_METADATA, -1, segment_metadata_mem_tracker());
    _short_key_index_mem_tracker = regist_tracker(MemTrackerType::SHORT_KEY_INDEX, -1, segment_metadata_mem_tracker());
    _column_zonemap_index_mem_tracker =
            regist_tracker(MemTrackerType::COLUMN_ZONEMAP_INDEX, -1, column_metadata_mem_tracker());
    _ordinal_index_mem_tracker = regist_tracker(MemTrackerType::ORDINAL_INDEX, -1, column_metadata_mem_tracker());
    _bitmap_index_mem_tracker = regist_tracker(MemTrackerType::BITMAP_INDEX, -1, column_metadata_mem_tracker());
    _bloom_filter_index_mem_tracker =
            regist_tracker(MemTrackerType::BLOOM_FILTER_INDEX, -1, column_metadata_mem_tracker());

    int64_t compaction_mem_limit = calc_max_compaction_memory(_process_mem_tracker->limit());
    _compaction_mem_tracker = regist_tracker(MemTrackerType::COMPACTION, compaction_mem_limit, process_mem_tracker());
    _schema_change_mem_tracker = regist_tracker(MemTrackerType::SCHEMA_CHANGE, -1, process_mem_tracker());
    _page_cache_mem_tracker = regist_tracker(MemTrackerType::PAGE_CACHE, -1, process_mem_tracker());
    _jit_cache_mem_tracker = regist_tracker(MemTrackerType::JIT_CACHE, -1, process_mem_tracker());
    int32_t update_mem_percent = std::max(std::min(100, config::update_memory_limit_percent), 0);
    _update_mem_tracker = regist_tracker(MemTrackerType::UPDATE, bytes_limit * update_mem_percent / 100, nullptr);
    _update_mem_tracker->set_level(2);
    _chunk_allocator_mem_tracker = regist_tracker(MemTrackerType::CHUNK_ALLOCATOR, -1, process_mem_tracker());
    _passthrough_mem_tracker = regist_tracker(MemTrackerType::PASSTHROUGH, -1, nullptr);
    _passthrough_mem_tracker->set_level(2);
    _clone_mem_tracker = regist_tracker(MemTrackerType::CLONE, -1, process_mem_tracker());
    int64_t consistency_mem_limit = calc_max_consistency_memory(_process_mem_tracker->limit());
    _consistency_mem_tracker =
            regist_tracker(MemTrackerType::CONSISTENCY, consistency_mem_limit, process_mem_tracker());
    _datacache_mem_tracker = regist_tracker(MemTrackerType::DATACACHE, -1, process_mem_tracker());
    _poco_connection_pool_mem_tracker = regist_tracker(MemTrackerType::POCO_CONNECTION_POOL, -1, process_mem_tracker());
    _replication_mem_tracker = regist_tracker(MemTrackerType::REPLICATION, -1, process_mem_tracker());

    MemChunkAllocator::init_instance(_chunk_allocator_mem_tracker.get(), config::chunk_reserved_bytes_limit);

    return Status::OK();
}

std::vector<std::shared_ptr<MemTracker>> GlobalEnv::mem_trackers() const {
    std::vector<std::shared_ptr<MemTracker>> mem_trackers;
    for (auto& item : _mem_tracker_map) {
        mem_trackers.emplace_back(item.second);
    }
    return mem_trackers;
}

std::shared_ptr<MemTracker> GlobalEnv::get_mem_tracker_by_type(MemTrackerType type) {
    auto iter = _mem_tracker_map.find(type);
    if (iter != _mem_tracker_map.end()) {
        return iter->second;
    } else {
        return nullptr;
    }
}

void GlobalEnv::_reset_tracker() {
    for (auto& iter : _mem_tracker_map) {
        iter.second.reset();
    }
}

int64_t GlobalEnv::get_storage_page_cache_size() {
    int64_t mem_limit = MemInfo::physical_mem();
    if (process_mem_tracker()->has_limit()) {
        mem_limit = process_mem_tracker()->limit();
    }
    return ParseUtil::parse_mem_spec(config::storage_page_cache_limit.value(), mem_limit);
}

int64_t GlobalEnv::check_storage_page_cache_size(int64_t storage_cache_limit) {
    if (storage_cache_limit > MemInfo::physical_mem()) {
        LOG(WARNING) << "Config storage_page_cache_limit is greater than memory size, config="
                     << config::storage_page_cache_limit.value() << ", memory=" << MemInfo::physical_mem();
    }
    if (!config::disable_storage_page_cache) {
        if (storage_cache_limit < kcacheMinSize) {
            LOG(WARNING) << "Storage cache limit is too small, use default size.";
            storage_cache_limit = kcacheMinSize;
        }
        LOG(INFO) << "Set storage page cache size " << storage_cache_limit;
    }
    return storage_cache_limit;
}

std::shared_ptr<MemTracker> GlobalEnv::regist_tracker(MemTrackerType type, int64_t bytes_limit, MemTracker* parent) {
    auto mem_tracker = std::make_shared<MemTracker>(type, bytes_limit, MemTracker::type_to_label(type), parent);
    _mem_tracker_map[type] = mem_tracker;
    return mem_tracker;
}

int64_t GlobalEnv::calc_max_query_memory(int64_t process_mem_limit, int64_t percent) {
    if (process_mem_limit <= 0) {
        // -1 means no limit
        return -1;
    }
    if (percent < 0 || percent > 100) {
        percent = 90;
    }
    return process_mem_limit * percent / 100;
}

bool parse_resource_str(const string& str, string* value) {
    if (!str.empty()) {
        std::string tmp_str = str;
        StripLeadingWhiteSpace(&tmp_str);
        StripTrailingWhitespace(&tmp_str);
        if (tmp_str.empty()) {
            return false;
        } else {
            *value = tmp_str;
            std::transform(value->begin(), value->end(), value->begin(), [](char c) { return std::tolower(c); });
            return true;
        }
    } else {
        return false;
    }
}

CacheEnv* CacheEnv::GetInstance() {
    static CacheEnv s_cache_env;
    return &s_cache_env;
}

Status CacheEnv::init(const std::vector<StorePath>& store_paths) {
    _global_env = GlobalEnv::GetInstance();
    _store_paths = store_paths;

    RETURN_IF_ERROR(_init_datacache());
    RETURN_IF_ERROR(_init_starcache_based_object_cache());
    RETURN_IF_ERROR(_init_lru_base_object_cache());
    RETURN_IF_ERROR(_init_page_cache());

    return Status::OK();
}

void CacheEnv::destroy() {
    _page_cache.reset();
    LOG(INFO) << "pagecache shutdown successfully";

    _lru_based_object_cache.reset();
    LOG(INFO) << "lru based object cache shutdown successfully";

    _starcache_based_object_cache.reset();
    LOG(INFO) << "starcache based object cache shutdown successfully";

    _block_cache.reset();
    LOG(INFO) << "datacache shutdown successfully";
}

Status CacheEnv::_init_starcache_based_object_cache() {
#ifdef WITH_STARCACHE
    if (_block_cache != nullptr && _block_cache->is_initialized()) {
        _starcache_based_object_cache = std::make_shared<StarCacheModule>(_block_cache->starcache_instance());
    }
#endif
    return Status::OK();
}

Status CacheEnv::_init_lru_base_object_cache() {
    ObjectCacheOptions options;
    int64_t storage_cache_limit = _global_env->get_storage_page_cache_size();
    storage_cache_limit = _global_env->check_storage_page_cache_size(storage_cache_limit);
    options.capacity = storage_cache_limit;

    _lru_based_object_cache = std::make_shared<LRUCacheModule>(options);
    LOG(INFO) << "object cache init successfully";
    return Status::OK();
}

Status CacheEnv::_init_page_cache() {
    _page_cache = std::make_shared<StoragePageCache>(_lru_based_object_cache.get());
    _page_cache->init_metrics();
    LOG(INFO) << "storage page cache init successfully";
    return Status::OK();
}

Status CacheEnv::_init_datacache() {
    _block_cache = std::make_shared<BlockCache>();

    // When configured old `block_cache` configurations, use the old items for compatibility.
    if (config::block_cache_enable) {
        config::datacache_enable = true;
        config::datacache_mem_size = std::to_string(config::block_cache_mem_size);
        config::datacache_disk_size = std::to_string(config::block_cache_disk_size);
        config::datacache_block_size = config::block_cache_block_size;
        config::datacache_max_concurrent_inserts = config::block_cache_max_concurrent_inserts;
        config::datacache_checksum_enable = config::block_cache_checksum_enable;
        config::datacache_direct_io_enable = config::block_cache_direct_io_enable;
        config::datacache_engine = config::block_cache_engine;
        LOG(WARNING) << "The configuration items prefixed with `block_cache_` will be deprecated soon"
                     << ", you'd better use the configuration items prefixed `datacache` instead!";
    }

#if !defined(WITH_STARCACHE)
    if (config::datacache_enable) {
        LOG(WARNING) << "No valid engines supported, skip initializing datacache module";
        config::datacache_enable = false;
    }
#endif

    if (config::datacache_enable) {
        CacheOptions cache_options;
        int64_t mem_limit = MemInfo::physical_mem();
        if (_global_env->process_mem_tracker()->has_limit()) {
            mem_limit = _global_env->process_mem_tracker()->limit();
        }
        RETURN_IF_ERROR(DataCacheUtils::parse_conf_datacache_mem_size(config::datacache_mem_size, mem_limit,
                                                                      &cache_options.mem_space_size));

        size_t total_quota_bytes = 0;
        for (auto& root_path : _store_paths) {
            // Because we have unified the datacache between datalake and starlet, we also need to unify the
            // cache path and quota.
            // To reuse the old cache data in `starlet_cache` directory, we try to rename it to the new `datacache`
            // directory if it exists. To avoid the risk of cross disk renaming of a large amount of cached data,
            // we do not automatically rename it when the source and destination directories are on different disks.
            // In this case, users should manually remount the directories and restart them.
            std::string datacache_path = root_path.path + "/datacache";
            std::string starlet_cache_path = root_path.path + "/starlet_cache/star_cache";
#ifdef USE_STAROS
            if (config::datacache_unified_instance_enable) {
                RETURN_IF_ERROR(DataCacheUtils::change_disk_path(starlet_cache_path, datacache_path));
            }
#endif
            // Create it if not exist
            Status st = FileSystem::Default()->create_dir_if_missing(datacache_path);
            if (!st.ok()) {
                LOG(ERROR) << "Fail to create datacache directory: " << datacache_path << ", reason: " << st.message();
                return Status::InternalError("Fail to create datacache directory");
            }

            int64_t disk_size =
                    DataCacheUtils::parse_conf_datacache_disk_size(datacache_path, config::datacache_disk_size, -1);
#ifdef USE_STAROS
            // If the `datacache_disk_size` is manually set a positive value, we will use the maximum cache quota between
            // dataleke and starlet cache as the quota of the unified cache. Otherwise, the cache quota will remain zero
            // and then automatically adjusted based on the current avalible disk space.
            if (config::datacache_unified_instance_enable && (!config::datacache_auto_adjust_enable || disk_size > 0)) {
                int64_t starlet_cache_size = DataCacheUtils::parse_conf_datacache_disk_size(
                        datacache_path, fmt::format("{}%", config::starlet_star_cache_disk_size_percent), -1);
                disk_size = std::max(disk_size, starlet_cache_size);
            }
#endif
            cache_options.disk_spaces.push_back({.path = datacache_path, .size = static_cast<size_t>(disk_size)});
            total_quota_bytes += disk_size;
        }

        if (cache_options.disk_spaces.empty() || total_quota_bytes != 0) {
            config::datacache_auto_adjust_enable = false;
        }

        // Adjust the default engine based on build switches.
        if (config::datacache_engine == "") {
#if defined(WITH_STARCACHE)
            config::datacache_engine = "starcache";
#endif
        }
        cache_options.block_size = config::datacache_block_size;
        cache_options.max_flying_memory_mb = config::datacache_max_flying_memory_mb;
        cache_options.max_concurrent_inserts = config::datacache_max_concurrent_inserts;
        cache_options.enable_checksum = config::datacache_checksum_enable;
        cache_options.enable_direct_io = config::datacache_direct_io_enable;
        cache_options.enable_tiered_cache = config::datacache_tiered_cache_enable;
        cache_options.skip_read_factor = config::datacache_skip_read_factor;
        cache_options.scheduler_threads_per_cpu = config::datacache_scheduler_threads_per_cpu;
        cache_options.enable_datacache_persistence = config::datacache_persistence_enable;
        cache_options.inline_item_count_limit = config::datacache_inline_item_count_limit;
        cache_options.engine = config::datacache_engine;
        cache_options.eviction_policy = config::datacache_eviction_policy;
        RETURN_IF_ERROR(_block_cache->init(cache_options));
        LOG(INFO) << "datacache init successfully";
    } else {
        LOG(INFO) << "starts by skipping the datacache initialization";
    }
    return Status::OK();
}

void CacheEnv::try_release_resource_before_core_dump() {
    std::set<std::string> modules;
    bool release_all = false;
    if (config::try_release_resource_before_core_dump.value() == "*") {
        release_all = true;
    } else {
        SplitStringAndParseToContainer(StringPiece(config::try_release_resource_before_core_dump), ",",
                                       &parse_resource_str, &modules);
    }

    auto need_release = [&release_all, &modules](const std::string& name) {
        return release_all || modules.contains(name);
    };

    if (_page_cache != nullptr && need_release("data_cache")) {
        _page_cache->set_capacity(0);
    }
    if (_block_cache != nullptr && _block_cache->available() && need_release("data_cache")) {
        // TODO: Currently, block cache don't support shutdown now,
        //  so here will temporary use update_mem_quota instead to release memory.
        (void)_block_cache->update_mem_quota(0, false);
    }
}

ExecEnv* ExecEnv::GetInstance() {
    static ExecEnv s_exec_env;
    return &s_exec_env;
}

ExecEnv::ExecEnv() = default;
ExecEnv::~ExecEnv() = default;

Status ExecEnv::init(const std::vector<StorePath>& store_paths, bool as_cn) {
    _store_paths = store_paths;
    _external_scan_context_mgr = new ExternalScanContextMgr(this);
    _metrics = StarRocksMetrics::instance()->metrics();
    _stream_mgr = new DataStreamMgr();
    _result_mgr = new ResultBufferMgr();
    _result_queue_mgr = new ResultQueueMgr();
    _backend_client_cache = new BackendServiceClientCache(config::max_client_cache_size_per_host);
    _frontend_client_cache = new FrontendServiceClientCache(config::max_client_cache_size_per_host);
    _broker_client_cache = new BrokerServiceClientCache(config::max_client_cache_size_per_host);
    // query_context_mgr keeps slotted map with 64 slot to reduce contention
    _query_context_mgr = new pipeline::QueryContextManager(6);
    RETURN_IF_ERROR(_query_context_mgr->init());
    _thread_pool =
            new PriorityThreadPool("table_scan_io", // olap/external table scan thread pool
                                   config::scanner_thread_pool_thread_num, config::scanner_thread_pool_queue_size);

    // Thread pool used for streaming load to scan StreamLoadPipe. The maximum number of
    // threads and queue size are set INT32_MAX which indicate there is no limit for the
    // thread pool, and this can avoid deadlock for concurrent streaming loads. The thread
    // pool will not be full easily because fragment execution pool and http workers also
    // limit the streaming load concurrency which is controlled by fragment_pool_thread_num_max
    // and webserver_num_workers respectively. This pool will be used when
    // enable_streaming_load_thread_pool is true.
    std::unique_ptr<ThreadPool> streaming_load_pool;
    RETURN_IF_ERROR(
            ThreadPoolBuilder("stream_load_io")
                    .set_min_threads(config::streaming_load_thread_pool_num_min)
                    .set_max_threads(INT32_MAX)
                    .set_max_queue_size(INT32_MAX)
                    .set_idle_timeout(MonoDelta::FromMilliseconds(config::streaming_load_thread_pool_idle_time_ms))
                    .build(&streaming_load_pool));
    _streaming_load_thread_pool = streaming_load_pool.release();

    _udf_call_pool = new PriorityThreadPool("udf", config::udf_thread_pool_size, config::udf_thread_pool_size);
    _fragment_mgr = new FragmentMgr(this);

    RETURN_IF_ERROR(ThreadPoolBuilder("automatic_partition") // automatic partition pool
                            .set_min_threads(0)
                            .set_max_threads(1000)
                            .set_max_queue_size(1000)
                            .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
                            .build(&_automatic_partition_pool));

    int num_prepare_threads = config::pipeline_prepare_thread_pool_thread_num;
    if (num_prepare_threads == 0) {
        num_prepare_threads = CpuInfo::num_cores();
    } else if (num_prepare_threads < 0) {
        // -n: means n * num_cpu_cores
        num_prepare_threads = -num_prepare_threads * CpuInfo::num_cores();
    }
    _pipeline_prepare_pool =
            new PriorityThreadPool("pip_prepare", num_prepare_threads, config::pipeline_prepare_thread_pool_queue_size);
    // register the metrics to monitor the task queue len
    auto task_qlen_fun = [] {
        auto pool = ExecEnv::GetInstance()->pipeline_prepare_pool();
        return (pool == nullptr) ? 0U : pool->get_queue_size();
    };
    REGISTER_GAUGE_STARROCKS_METRIC(pipe_prepare_pool_queue_len, task_qlen_fun);

    int num_sink_io_threads = config::pipeline_sink_io_thread_pool_thread_num;
    if (num_sink_io_threads <= 0) {
        num_sink_io_threads = CpuInfo::num_cores();
    }
    if (config::pipeline_sink_io_thread_pool_queue_size <= 0) {
        return Status::InvalidArgument("pipeline_sink_io_thread_pool_queue_size shoule be greater than 0");
    }
    _pipeline_sink_io_pool =
            new PriorityThreadPool("pip_sink_io", num_sink_io_threads, config::pipeline_sink_io_thread_pool_queue_size);

    int query_rpc_threads = config::internal_service_query_rpc_thread_num;
    if (query_rpc_threads <= 0) {
        query_rpc_threads = CpuInfo::num_cores();
    }
    _query_rpc_pool = new PriorityThreadPool("query_rpc", query_rpc_threads, std::numeric_limits<uint32_t>::max());

    // The _load_rpc_pool now handles routine load RPC and table function RPC.
    RETURN_IF_ERROR(ThreadPoolBuilder("load_rpc") // thread pool for load rpc
                            .set_min_threads(10)
                            .set_max_threads(1000)
                            .set_max_queue_size(0)
                            .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
                            .build(&_load_rpc_pool));
    REGISTER_GAUGE_STARROCKS_METRIC(load_rpc_threadpool_size, _load_rpc_pool->num_threads)

    RETURN_IF_ERROR(ThreadPoolBuilder("dictionary_cache") // thread pool for dictionary cache Sink
                            .set_min_threads(1)
                            .set_max_threads(config::dictionary_cache_refresh_threadpool_size)
                            .set_max_queue_size(INT32_MAX) // unlimit queue size
                            .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
                            .build(&_dictionary_cache_pool));

    _max_executor_threads = CpuInfo::num_cores();
    if (config::pipeline_exec_thread_pool_thread_num > 0) {
        _max_executor_threads = config::pipeline_exec_thread_pool_thread_num;
    }
    _max_executor_threads = std::max<int64_t>(1, _max_executor_threads);
    LOG(INFO) << strings::Substitute("[PIPELINE] Exec thread pool: thread_num=$0", _max_executor_threads);

    _driver_limiter =
            new pipeline::DriverLimiter(_max_executor_threads * config::pipeline_max_num_drivers_per_exec_thread);
    REGISTER_GAUGE_STARROCKS_METRIC(pipe_drivers, [] {
        auto* driver_limiter = ExecEnv::GetInstance()->driver_limiter();
        return (driver_limiter == nullptr) ? 0 : driver_limiter->num_total_drivers();
    });

    _pipeline_timer = new pipeline::PipelineTimer();
    RETURN_IF_ERROR(_pipeline_timer->start());

    const int num_io_threads = config::pipeline_scan_thread_pool_thread_num <= 0
                                       ? CpuInfo::num_cores()
                                       : config::pipeline_scan_thread_pool_thread_num;

    const int connector_num_io_threads = int(config::pipeline_connector_scan_thread_num_per_cpu * CpuInfo::num_cores());
    CHECK_GT(connector_num_io_threads, 0) << "pipeline_connector_scan_thread_num_per_cpu should greater than 0";

    if (config::hdfs_client_enable_hedged_read) {
        // Set hdfs client hedged read pool size
        config::hdfs_client_hedged_read_threadpool_size =
                std::min(connector_num_io_threads * 2, config::hdfs_client_hedged_read_threadpool_size);
        CHECK_GT(config::hdfs_client_hedged_read_threadpool_size, 0)
                << "hdfs_client_hedged_read_threadpool_size should greater than 0";
    }

    // Disable bind cpus when cgroup has cpu quota but no cpuset.
    const bool enable_bind_cpus = config::enable_resource_group_bind_cpus &&
                                  (!CpuInfo::is_cgroup_with_cpu_quota() || CpuInfo::is_cgroup_with_cpuset());
    config::enable_resource_group_bind_cpus = enable_bind_cpus;
    workgroup::PipelineExecutorSetConfig executors_manager_opts(
            CpuInfo::num_cores(), _max_executor_threads, num_io_threads, connector_num_io_threads,
            CpuInfo::get_core_ids(), enable_bind_cpus, config::enable_resource_group_cpu_borrowing,
            StarRocksMetrics::instance()->get_pipeline_executor_metrics());
    _workgroup_manager = std::make_unique<workgroup::WorkGroupManager>(std::move(executors_manager_opts));
    RETURN_IF_ERROR(_workgroup_manager->start());
    workgroup::DefaultWorkGroupInitialization default_workgroup_init;

    if (store_paths.empty() && as_cn) {
        _load_path_mgr = new DummyLoadPathMgr();
    } else {
        _load_path_mgr = new LoadPathMgr(this);
    }

    std::unique_ptr<ThreadPool> load_rowset_pool;
    std::unique_ptr<ThreadPool> load_segment_pool;
    std::unique_ptr<ThreadPool> put_combined_txn_log_thread_pool;
    RETURN_IF_ERROR(
            ThreadPoolBuilder("load_rowset_pool")
                    .set_min_threads(0)
                    .set_max_threads(config::load_segment_thread_pool_num_max)
                    .set_max_queue_size(config::load_segment_thread_pool_queue_size)
                    .set_idle_timeout(MonoDelta::FromMilliseconds(config::streaming_load_thread_pool_idle_time_ms))
                    .build(&load_rowset_pool));
    _load_rowset_thread_pool = load_rowset_pool.release();

    RETURN_IF_ERROR(
            ThreadPoolBuilder("load_segment_pool")
                    .set_min_threads(0)
                    .set_max_threads(config::load_segment_thread_pool_num_max)
                    .set_max_queue_size(config::load_segment_thread_pool_queue_size)
                    .set_idle_timeout(MonoDelta::FromMilliseconds(config::streaming_load_thread_pool_idle_time_ms))
                    .build(&load_segment_pool));
    _load_segment_thread_pool = load_segment_pool.release();

    _broker_mgr = new BrokerMgr(this);

    RETURN_IF_ERROR(ThreadPoolBuilder("put_combined_txn_log_thread_pool")
                            .set_min_threads(0)
                            .set_max_threads(config::put_combined_txn_log_thread_pool_num_max)
                            .set_idle_timeout(MonoDelta::FromMilliseconds(500))
                            .build(&put_combined_txn_log_thread_pool));
    _put_combined_txn_log_thread_pool = put_combined_txn_log_thread_pool.release();

#ifndef BE_TEST
    _bfd_parser = BfdParser::create();
#endif
    _load_channel_mgr = new LoadChannelMgr();
    _load_stream_mgr = new LoadStreamMgr();
    _brpc_stub_cache = new BrpcStubCache();
    _stream_load_executor = new StreamLoadExecutor(this);
    _stream_context_mgr = new StreamContextMgr();
    _transaction_mgr = new TransactionMgr(this);

    std::unique_ptr<ThreadPool> batch_write_thread_pool;
    RETURN_IF_ERROR(ThreadPoolBuilder("batch_write")
                            .set_min_threads(config::merge_commit_thread_pool_num_min)
                            .set_max_threads(config::merge_commit_thread_pool_num_max)
                            .set_max_queue_size(config::merge_commit_thread_pool_queue_size)
                            .set_idle_timeout(MonoDelta::FromMilliseconds(10000))
                            .build(&batch_write_thread_pool));
    auto batch_write_executor =
            std::make_unique<bthreads::ThreadPoolExecutor>(batch_write_thread_pool.release(), kTakesOwnership);
    _batch_write_mgr = new BatchWriteMgr(std::move(batch_write_executor));
    RETURN_IF_ERROR(_batch_write_mgr->init());

    _routine_load_task_executor = new RoutineLoadTaskExecutor(this);
    RETURN_IF_ERROR(_routine_load_task_executor->init());

    _small_file_mgr = new SmallFileMgr(this, config::small_file_dir);
    _runtime_filter_worker = new RuntimeFilterWorker(this);
    _runtime_filter_cache = new RuntimeFilterCache(8);
    RETURN_IF_ERROR(_runtime_filter_cache->init());
    _profile_report_worker = new ProfileReportWorker(this);
    auto runtime_filter_event_func = [] {
        auto pool = ExecEnv::GetInstance()->runtime_filter_worker();
        return (pool == nullptr) ? 0U : pool->queue_size();
    };
    REGISTER_GAUGE_STARROCKS_METRIC(runtime_filter_event_queue_len, runtime_filter_event_func);

    _backend_client_cache->init_metrics(StarRocksMetrics::instance()->metrics(), "backend");
    _frontend_client_cache->init_metrics(StarRocksMetrics::instance()->metrics(), "frontend");
    _broker_client_cache->init_metrics(StarRocksMetrics::instance()->metrics(), "broker");
    RETURN_IF_ERROR(_result_mgr->init());

    // it means acting as compute node while store_path is empty. some threads are not needed for that case.
    Status status = _load_path_mgr->init();
    if (!status.ok()) {
        LOG(ERROR) << "load path mgr init failed." << status.message();
        exit(-1);
    }

#if defined(USE_STAROS) && !defined(BE_TEST) && !defined(BUILD_FORMAT_LIB)
    _lake_location_provider = std::make_shared<lake::StarletLocationProvider>();
    _lake_update_manager =
            new lake::UpdateManager(_lake_location_provider, GlobalEnv::GetInstance()->update_mem_tracker());
    _lake_tablet_manager =
            new lake::TabletManager(_lake_location_provider, _lake_update_manager, config::lake_metadata_cache_limit);
    _lake_replication_txn_manager = new lake::ReplicationTxnManager(_lake_tablet_manager);
    if (config::starlet_cache_dir.empty()) {
        std::vector<std::string> starlet_cache_paths;
        std::for_each(store_paths.begin(), store_paths.end(), [&](const StorePath& root_path) {
            std::string starlet_cache_path = root_path.path + "/starlet_cache";
            starlet_cache_paths.emplace_back(starlet_cache_path);
        });
        config::starlet_cache_dir = JoinStrings(starlet_cache_paths, ":");
    }

#elif defined(BE_TEST)
    _lake_location_provider = std::make_shared<lake::FixedLocationProvider>(_store_paths.front().path);
    _lake_update_manager =
            new lake::UpdateManager(_lake_location_provider, GlobalEnv::GetInstance()->update_mem_tracker());
    _lake_tablet_manager =
            new lake::TabletManager(_lake_location_provider, _lake_update_manager, config::lake_metadata_cache_limit);
    _lake_replication_txn_manager = new lake::ReplicationTxnManager(_lake_tablet_manager);
#endif

    _agent_server = new AgentServer(this, false);
    _agent_server->init_or_die();

    _broker_mgr->init();
    RETURN_IF_ERROR(_small_file_mgr->init());

    RETURN_IF_ERROR(_load_channel_mgr->init(GlobalEnv::GetInstance()->load_mem_tracker()));

    _heartbeat_flags = new HeartbeatFlags();
    auto capacity = std::max<size_t>(config::query_cache_capacity, 4L * 1024 * 1024);
    _cache_mgr = new query_cache::CacheManager(capacity);

    _spill_dir_mgr = std::make_shared<spill::DirManager>();
    RETURN_IF_ERROR(_spill_dir_mgr->init(config::spill_local_storage_dir));

    _diagnose_daemon = new DiagnoseDaemon();
    RETURN_IF_ERROR(_diagnose_daemon->init());
#ifdef STARROCKS_JIT_ENABLE
    auto jit_engine = JITEngine::get_instance();
    status = jit_engine->init();
    if (!status.ok()) {
        LOG(WARNING) << "Failed to init JIT engine: " << status.message();
    }
#endif

    RETURN_IF_ERROR(PythonEnvManager::getInstance().init(config::python_envs));
    PythonEnvManager::getInstance().start_background_cleanup_thread();

    return Status::OK();
}

std::string ExecEnv::token() const {
    return get_master_token();
}

void ExecEnv::add_rf_event(const RfTracePoint& pt) {
    std::string msg =
            strings::Substitute("$0($1)", pt.msg, pt.network.empty() ? BackendOptions::get_localhost() : pt.network);
    _runtime_filter_cache->add_rf_event(pt.query_id, pt.filter_id, std::move(msg));
}

void ExecEnv::stop() {
    if (_load_channel_mgr) {
        // Clear load channel should be executed before stopping the storage engine,
        // otherwise some writing tasks will still be in the MemTableFlushThreadPool of the storage engine,
        // so when the ThreadPool is destroyed, it will crash.
        _load_channel_mgr->close();
    }

    if (_load_stream_mgr) {
        _load_stream_mgr->close();
    }

    if (_fragment_mgr) {
        _fragment_mgr->close();
    }

    if (_stream_mgr != nullptr) {
        _stream_mgr->close();
    }

    if (_pipeline_sink_io_pool) {
        _pipeline_sink_io_pool->shutdown();
    }

    if (_agent_server) {
        _agent_server->stop();
    }

    if (_runtime_filter_worker) {
        _runtime_filter_worker->close();
    }

    if (_profile_report_worker) {
        _profile_report_worker->close();
    }

    if (_automatic_partition_pool) {
        _automatic_partition_pool->shutdown();
    }

    if (_query_rpc_pool) {
        _query_rpc_pool->shutdown();
    }

    if (_load_rpc_pool) {
        _load_rpc_pool->shutdown();
    }

    if (_workgroup_manager) {
        _workgroup_manager->close();
    }

    if (_thread_pool) {
        _thread_pool->shutdown();
    }

    if (_query_context_mgr) {
        _query_context_mgr->clear();
    }

    if (_result_mgr) {
        _result_mgr->stop();
    }

    if (_stream_mgr) {
        _stream_mgr->close();
    }

    if (_batch_write_mgr) {
        _batch_write_mgr->stop();
    }

    if (_routine_load_task_executor) {
        _routine_load_task_executor->stop();
    }

    if (_dictionary_cache_pool) {
        _dictionary_cache_pool->shutdown();
    }

    if (_diagnose_daemon) {
        _diagnose_daemon->stop();
    }

#ifndef BE_TEST
    close_s3_clients();
#endif

    PythonEnvManager::getInstance().close();
}

void ExecEnv::destroy() {
    SAFE_DELETE(_agent_server);
    SAFE_DELETE(_runtime_filter_worker);
    SAFE_DELETE(_profile_report_worker);
    SAFE_DELETE(_heartbeat_flags);
    SAFE_DELETE(_small_file_mgr);
    SAFE_DELETE(_transaction_mgr);
    SAFE_DELETE(_stream_context_mgr);
    SAFE_DELETE(_routine_load_task_executor);
    SAFE_DELETE(_stream_load_executor);
    SAFE_DELETE(_fragment_mgr);
    SAFE_DELETE(_load_stream_mgr);
    SAFE_DELETE(_load_channel_mgr);
    SAFE_DELETE(_broker_mgr);
    SAFE_DELETE(_bfd_parser);
    SAFE_DELETE(_load_path_mgr);
    SAFE_DELETE(_brpc_stub_cache);
    SAFE_DELETE(_udf_call_pool);
    SAFE_DELETE(_pipeline_prepare_pool);
    SAFE_DELETE(_pipeline_sink_io_pool);
    SAFE_DELETE(_query_rpc_pool);
    _load_rpc_pool.reset();
    _workgroup_manager->destroy();
    _workgroup_manager.reset();
    SAFE_DELETE(_thread_pool);
    SAFE_DELETE(_streaming_load_thread_pool);
    SAFE_DELETE(_load_segment_thread_pool);

    if (_lake_tablet_manager != nullptr) {
        _lake_tablet_manager->prune_metacache();
    }

    SAFE_DELETE(_query_context_mgr);
    // WorkGroupManager should release MemTracker of WorkGroups belongs to itself before deallocate
    // _query_pool_mem_tracker.
    SAFE_DELETE(_runtime_filter_cache);
    SAFE_DELETE(_driver_limiter);
    SAFE_DELETE(_pipeline_timer);
    SAFE_DELETE(_broker_client_cache);
    SAFE_DELETE(_frontend_client_cache);
    SAFE_DELETE(_backend_client_cache);
    SAFE_DELETE(_result_queue_mgr);
    SAFE_DELETE(_result_mgr);
    SAFE_DELETE(_stream_mgr);
    SAFE_DELETE(_batch_write_mgr);
    SAFE_DELETE(_external_scan_context_mgr);
    SAFE_DELETE(_lake_tablet_manager);
    SAFE_DELETE(_lake_update_manager);
    SAFE_DELETE(_lake_replication_txn_manager);
    SAFE_DELETE(_cache_mgr);
    SAFE_DELETE(_put_combined_txn_log_thread_pool);
    SAFE_DELETE(_diagnose_daemon);
    _dictionary_cache_pool.reset();
    _automatic_partition_pool.reset();
    _metrics = nullptr;
}

void ExecEnv::_wait_for_fragments_finish() {
    if (config::loop_count_wait_fragments_finish < 0) {
        LOG(WARNING) << "'config::loop_count_wait_fragments_finish' is set to a negative integer, ignore it.";
        return;
    }

    size_t max_loop_secs = config::loop_count_wait_fragments_finish * 10;
    if (max_loop_secs == 0) {
        return;
    }

    size_t running_fragments = _get_running_fragments_count();
    size_t loop_secs = 0;

    while (running_fragments > 0 && loop_secs < max_loop_secs) {
        LOG(INFO) << running_fragments << " fragment(s) are still running...";
        sleep(1);
        running_fragments = _get_running_fragments_count();
        loop_secs++;
    }
}

size_t ExecEnv::_get_running_fragments_count() const {
    // fragment is registered in _fragment_mgr in non-pipeline env
    // while _query_context_mgr is used in pipeline engine.
    return _fragment_mgr->running_fragment_count() + _query_context_mgr->size();
}

void ExecEnv::wait_for_finish() {
    _wait_for_fragments_finish();
}

uint32_t ExecEnv::calc_pipeline_dop(int32_t pipeline_dop) const {
    if (pipeline_dop > 0) {
        return pipeline_dop;
    }

    // Default dop is a half of the number of hardware threads.
    return std::max<uint32_t>(1, _max_executor_threads / 2);
}

uint32_t ExecEnv::calc_pipeline_sink_dop(int32_t pipeline_sink_dop) const {
    if (pipeline_sink_dop > 0) {
        return pipeline_sink_dop;
    }

    // Default sink dop is the number of hardware threads with a cap of 64.
    auto dop = std::max<uint32_t>(1, _max_executor_threads);
    return std::min<uint32_t>(dop, 64);
}

ThreadPool* ExecEnv::delete_file_thread_pool() {
    return _agent_server ? _agent_server->get_thread_pool(TTaskType::DROP) : nullptr;
}

void ExecEnv::try_release_resource_before_core_dump() {
    std::set<std::string> modules;
    bool release_all = false;
    if (config::try_release_resource_before_core_dump.value() == "*") {
        release_all = true;
    } else {
        SplitStringAndParseToContainer(StringPiece(config::try_release_resource_before_core_dump), ",",
                                       &parse_resource_str, &modules);
    }

    auto need_release = [&release_all, &modules](const std::string& name) {
        return release_all || modules.contains(name);
    };

    if (_workgroup_manager != nullptr && need_release("connector_scan_executor")) {
        _workgroup_manager->for_each_executors([](auto& executors) { executors.connector_scan_executor()->close(); });
    }
    if (_workgroup_manager != nullptr && need_release("olap_scan_executor")) {
        _workgroup_manager->for_each_executors([](auto& executors) { executors.scan_executor()->close(); });
    }
    if (_thread_pool != nullptr && need_release("non_pipeline_scan_thread_pool")) {
        _thread_pool->shutdown();
    }
    if (_pipeline_prepare_pool != nullptr && need_release("pipeline_prepare_thread_pool")) {
        _pipeline_prepare_pool->shutdown();
    }
    if (_pipeline_sink_io_pool != nullptr && need_release("pipeline_sink_io_thread_pool")) {
        _pipeline_sink_io_pool->shutdown();
    }
    if (_query_rpc_pool != nullptr && need_release("query_rpc_thread_pool")) {
        _query_rpc_pool->shutdown();
    }
    if (_agent_server != nullptr && need_release("publish_version_worker_pool")) {
        _agent_server->stop_task_worker_pool(TaskWorkerType::PUBLISH_VERSION);
    }
    if (_workgroup_manager != nullptr && need_release("wg_driver_executor")) {
        _workgroup_manager->for_each_executors([](auto& executors) { executors.driver_executor()->close(); });
    }
}

pipeline::DriverExecutor* ExecEnv::wg_driver_executor() {
    return _workgroup_manager->shared_executors()->driver_executor();
}
workgroup::ScanExecutor* ExecEnv::scan_executor() {
    return _workgroup_manager->shared_executors()->scan_executor();
}
workgroup::ScanExecutor* ExecEnv::connector_scan_executor() {
    return _workgroup_manager->shared_executors()->connector_scan_executor();
}

} // namespace starrocks
