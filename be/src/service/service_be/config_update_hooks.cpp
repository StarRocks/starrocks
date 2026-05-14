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

#include "service/service_be/config_update_hooks.h"

#include <algorithm>
#include <limits>
#include <string>
#include <vector>

#include "agent/agent_common.h"
#include "agent/agent_server.h"
#include "cache/datacache.h"
#include "cache/datacache_utils.h"
#include "cache/mem_cache/page_cache.h"
#include "common/config_agent_fwd.h"
#include "common/config_cache_fwd.h"
#include "common/config_compaction_fwd.h"
#include "common/config_exec_env_fwd.h"
#include "common/config_exec_flow_fwd.h"
#include "common/config_ingest_fwd.h"
#include "common/config_lake_fwd.h"
#include "common/config_merge_commit_fwd.h"
#include "common/config_primary_key_fwd.h"
#include "common/config_runtime_fwd.h"
#include "common/config_staros_worker_fwd.h"
#include "common/config_storage_fwd.h"
#include "common/config_update_registry.h"
#include "common/logging.h"
#include "common/status.h"
#include "common/system/cpu_info.h"
#include "common/thread/priority_thread_pool.hpp"
#include "common/util/bthreads/executor.h"
#include "exec/workgroup/scan_executor.h"
#include "runtime/batch_write/batch_write_mgr.h"
#include "runtime/batch_write/txn_state_cache.h"
#include "runtime/exec_env.h"
#include "runtime/load_channel_mgr.h"
#include "storage/compaction_manager.h"
#include "storage/lake/compaction_scheduler.h"
#include "storage/lake/lake_persistent_index_parallel_compact_mgr.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/update_manager.h"
#include "storage/load_spill_block_manager.h"
#include "storage/memtable_flush_executor.h"
#include "storage/persistent_index_compaction_manager.h"
#include "storage/persistent_index_load_executor.h"
#include "storage/segment_flush_executor.h"
#include "storage/segment_replicate_executor.h"
#include "storage/storage_engine.h"
#include "storage/update_manager.h"

#ifdef USE_STAROS
#include "common/gflags_utils.h"
#include "staros_integration/staros_starcache.h"
#include "staros_integration/staros_worker.h"
#include "staros_integration/staros_worker_runtime.h"
#endif // USE_STAROS

namespace starrocks {

void register_config_update_hooks(ExecEnv* exec_env) {
    auto* registry = ConfigUpdateRegistry::instance();

    registry->register_callback("scanner_thread_pool_thread_num", [=]() -> Status {
        LOG(INFO) << "set scanner_thread_pool_thread_num:" << config::scanner_thread_pool_thread_num;
        exec_env->thread_pool()->set_num_thread(config::scanner_thread_pool_thread_num);
        return Status::OK();
    });
#ifndef __APPLE__
    registry->register_callback("storage_page_cache_limit", [=]() -> Status {
        StoragePageCache* cache = DataCache::GetInstance()->page_cache();
        if (cache == nullptr || !cache->is_initialized()) {
            return Status::InternalError("Page cache is not initialized");
        }

        ASSIGN_OR_RETURN(int64_t cache_limit, DataCache::GetInstance()->get_datacache_limit());
        cache_limit = DataCache::GetInstance()->check_datacache_limit(cache_limit);
        cache->set_capacity(cache_limit);
        return Status::OK();
    });
#endif
#ifndef __APPLE__
    registry->register_callback("disable_storage_page_cache", [=]() -> Status {
        StoragePageCache* cache = DataCache::GetInstance()->page_cache();
        if (cache == nullptr || !cache->is_initialized()) {
            return Status::InternalError("Page cache is not initialized");
        }
        if (config::disable_storage_page_cache) {
            cache->set_capacity(0);
        } else {
            ASSIGN_OR_RETURN(int64_t cache_limit, DataCache::GetInstance()->get_datacache_limit());
            cache_limit = DataCache::GetInstance()->check_datacache_limit(cache_limit);
            cache->set_capacity(cache_limit);
        }
        return Status::OK();
    });
#endif
#ifndef __APPLE__
    registry->register_callback("datacache_mem_size", [=]() -> Status {
        LocalMemCacheEngine* cache = DataCache::GetInstance()->local_mem_cache();
        if (cache == nullptr || !cache->is_initialized()) {
            return Status::InternalError("Local cache is not initialized");
        }

        size_t mem_size = 0;
        Status st = DataCacheUtils::parse_conf_datacache_mem_size(
                config::datacache_mem_size, GlobalEnv::GetInstance()->process_mem_limit(), &mem_size);
        if (!st.ok()) {
            LOG(WARNING) << "Failed to update datacache mem size";
            return st;
        }
        return cache->update_mem_quota(mem_size);
    });
    registry->register_callback("datacache_disk_size", [=]() -> Status {
        LocalDiskCacheEngine* cache = DataCache::GetInstance()->local_disk_cache();
        if (cache == nullptr || !cache->is_initialized()) {
            return Status::InternalError("Local cache is not initialized");
        }

        std::vector<DirSpace> spaces;
        cache->disk_spaces(&spaces);
        for (auto& space : spaces) {
            ASSIGN_OR_RETURN(int64_t disk_size, DataCacheUtils::parse_conf_datacache_disk_size(
                                                        space.path, config::datacache_disk_size, -1));
            if (disk_size < 0) {
                LOG(WARNING) << "Failed to update datacache disk spaces for the invalid disk_size: " << disk_size;
                return Status::InternalError("Fail to update datacache disk spaces");
            }
            space.size = disk_size;
        }
        return cache->update_disk_spaces(spaces);
    });
    registry->register_callback("datacache_inline_item_count_limit", [=]() -> Status {
        LocalDiskCacheEngine* cache = DataCache::GetInstance()->local_disk_cache();
        if (cache == nullptr || !cache->is_initialized()) {
            return Status::InternalError("Local cache is not initialized");
        }
        return cache->update_inline_cache_count_limit(config::datacache_inline_item_count_limit);
    });
#endif
    registry->register_callback("max_compaction_concurrency", [=]() -> Status {
        if (!config::enable_event_based_compaction_framework) {
            return Status::InvalidArgument(
                    "This parameter is mutable when the Event-based Compaction Framework is enabled.");
        }
        return StorageEngine::instance()->compaction_manager()->update_max_threads(config::max_compaction_concurrency);
    });
    registry->register_callback("flush_thread_num_per_store", [=]() -> Status {
        const size_t dir_cnt = StorageEngine::instance()->get_stores().size();
        Status st1 = StorageEngine::instance()->memtable_flush_executor()->update_max_threads(
                config::flush_thread_num_per_store * dir_cnt);
        Status st2 = StorageEngine::instance()->segment_replicate_executor()->update_max_threads(
                config::flush_thread_num_per_store * dir_cnt);
        Status st3 = StorageEngine::instance()->segment_flush_executor()->update_max_threads(
                config::flush_thread_num_per_store * dir_cnt);
        if (!st1.ok() || !st2.ok() || !st3.ok()) {
            return Status::InvalidArgument("Failed to update flush_thread_num_per_store.");
        }
        return st1;
    });
    registry->register_callback("lake_flush_thread_num_per_store", [=]() -> Status {
        return StorageEngine::instance()->lake_memtable_flush_executor()->update_max_threads(
                MemTableFlushExecutor::calc_max_threads_for_lake_table(StorageEngine::instance()->get_stores()));
    });
    registry->register_callback("update_compaction_num_threads_per_disk", [=]() -> Status {
        StorageEngine::instance()->increase_update_compaction_thread(config::update_compaction_num_threads_per_disk);
        return Status::OK();
    });
    registry->register_callback("pindex_major_compaction_num_threads", [=]() -> Status {
        PersistentIndexCompactionManager* mgr =
                StorageEngine::instance()->update_manager()->get_pindex_compaction_mgr();
        if (mgr != nullptr) {
            const int max_pk_index_compaction_thread_cnt = std::max(1, config::pindex_major_compaction_num_threads);
            return mgr->update_max_threads(max_pk_index_compaction_thread_cnt);
        }
        return Status::OK();
    });
    registry->register_callback("pindex_load_thread_pool_num_max", [=]() -> Status {
        LOG(INFO) << "Set pindex_load_thread_pool_num_max: " << config::pindex_load_thread_pool_num_max;
        return StorageEngine::instance()->update_manager()->get_pindex_load_executor()->refresh_max_thread_num();
    });
    registry->register_callback("update_memory_limit_percent", [=]() -> Status {
        Status st = StorageEngine::instance()->update_manager()->update_primary_index_memory_limit(
                config::update_memory_limit_percent);
#if defined(USE_STAROS) && !defined(BE_TEST)
        st = exec_env->lake_update_manager()->update_primary_index_memory_limit(config::update_memory_limit_percent);
#endif
        return st;
    });
    registry->register_callback("dictionary_cache_refresh_threadpool_size", [=]() -> Status {
        if (exec_env->dictionary_cache_pool() != nullptr) {
            return exec_env->dictionary_cache_pool()->update_max_threads(
                    config::dictionary_cache_refresh_threadpool_size);
        }
        return Status::OK();
    });
    registry->register_callback("transaction_publish_version_worker_count", [=]() -> Status {
        Status st1 = ExecEnv::GetInstance()
                             ->agent_server()
                             ->get_thread_pool(TTaskType::PUBLISH_VERSION)
                             ->update_max_threads(std::max(MIN_TRANSACTION_PUBLISH_WORKER_COUNT,
                                                           config::transaction_publish_version_worker_count));
        Status st2 = ExecEnv::GetInstance()->put_aggregate_metadata_thread_pool()->update_max_threads(
                std::max(MIN_TRANSACTION_PUBLISH_WORKER_COUNT, config::transaction_publish_version_worker_count));
        if (!st1.ok() || !st2.ok()) {
            return Status::InvalidArgument("Failed to update transaction_publish_version_worker_count.");
        }
        return st1;
    });
    registry->register_callback("transaction_publish_version_thread_pool_num_min", [=]() -> Status {
        auto thread_pool = ExecEnv::GetInstance()->agent_server()->get_thread_pool(TTaskType::PUBLISH_VERSION);
        return thread_pool->update_min_threads(std::max(MIN_TRANSACTION_PUBLISH_WORKER_COUNT,
                                                        config::transaction_publish_version_thread_pool_num_min));
    });
    registry->register_callback("lake_metadata_fetch_thread_count", [=]() -> Status {
        if (exec_env->lake_metadata_fetch_thread_pool() != nullptr) {
            return exec_env->lake_metadata_fetch_thread_pool()->update_max_threads(
                    std::max(1, config::lake_metadata_fetch_thread_count));
        }
        return Status::OK();
    });
    registry->register_callback("parallel_clone_task_per_path", [=]() -> Status {
        exec_env->agent_server()->update_max_thread_by_type(TTaskType::CLONE, config::parallel_clone_task_per_path);
        return Status::OK();
    });
    registry->register_callback("make_snapshot_worker_count", [=]() -> Status {
        exec_env->agent_server()->update_max_thread_by_type(TTaskType::MAKE_SNAPSHOT,
                                                            config::make_snapshot_worker_count);
        return Status::OK();
    });
    registry->register_callback("release_snapshot_worker_count", [=]() -> Status {
        exec_env->agent_server()->update_max_thread_by_type(TTaskType::RELEASE_SNAPSHOT,
                                                            config::release_snapshot_worker_count);
        return Status::OK();
    });
    registry->register_callback("upload_worker_count", [=]() -> Status {
        exec_env->agent_server()->update_max_thread_by_type(TTaskType::UPLOAD, config::upload_worker_count);
        return Status::OK();
    });
    registry->register_callback("download_worker_count", [=]() -> Status {
        exec_env->agent_server()->update_max_thread_by_type(TTaskType::DOWNLOAD, config::download_worker_count);
        exec_env->agent_server()->update_max_thread_by_type(TTaskType::MOVE, config::download_worker_count);
        return Status::OK();
    });
    registry->register_callback("replication_threads", [=]() -> Status {
        exec_env->agent_server()->update_max_thread_by_type(TTaskType::REMOTE_SNAPSHOT, config::replication_threads);
        exec_env->agent_server()->update_max_thread_by_type(TTaskType::REPLICATE_SNAPSHOT, config::replication_threads);
        return Status::OK();
    });
    registry->register_callback("alter_tablet_worker_count", [=]() -> Status {
        exec_env->agent_server()->update_max_thread_by_type(TTaskType::ALTER, config::alter_tablet_worker_count);
        return Status::OK();
    });
    registry->register_callback("update_tablet_meta_info_worker_count", [=]() -> Status {
        exec_env->agent_server()->update_max_thread_by_type(TTaskType::UPDATE_TABLET_META_INFO,
                                                            std::max(1, config::update_tablet_meta_info_worker_count));
        return Status::OK();
    });
    registry->register_callback("lake_metadata_cache_limit", [=]() -> Status {
        auto tablet_mgr = exec_env->lake_tablet_manager();
        if (tablet_mgr != nullptr) tablet_mgr->update_metacache_limit(config::lake_metadata_cache_limit);
        return Status::OK();
    });
    registry->register_callback("pk_index_parallel_execution_threadpool_max_threads", [=]() -> Status {
        auto thread_pool = exec_env->pk_index_execution_thread_pool();
        if (thread_pool != nullptr) {
            return thread_pool->update_max_threads(config::pk_index_parallel_execution_threadpool_max_threads);
        }
        return Status::OK();
    });
    registry->register_callback("lake_partial_update_thread_pool_max_threads", [=]() -> Status {
        auto thread_pool = exec_env->lake_partial_update_thread_pool();
        if (thread_pool != nullptr) {
            int max_thread_count = config::lake_partial_update_thread_pool_max_threads;
            if (max_thread_count <= 0) {
                max_thread_count = CpuInfo::num_cores() / 2;
            }
            return thread_pool->update_max_threads(std::max(1, max_thread_count));
        }
        return Status::OK();
    });
    registry->register_callback("pk_index_memtable_flush_threadpool_max_threads", [=]() -> Status {
        auto thread_pool = exec_env->pk_index_memtable_flush_thread_pool();
        if (thread_pool != nullptr) {
            return thread_pool->update_max_threads(config::pk_index_memtable_flush_threadpool_max_threads);
        }
        return Status::OK();
    });
    registry->register_callback("pk_index_parallel_compaction_threadpool_max_threads", [=]() -> Status {
        auto mgr = exec_env->parallel_compact_mgr();
        if (mgr != nullptr) {
            return mgr->update_max_threads(config::pk_index_parallel_compaction_threadpool_max_threads);
        }
        return Status::OK();
    });
#ifdef USE_STAROS
    registry->register_callback("starlet_use_star_cache", [=]() -> Status {
        update_staros_starcache();
        return Status::OK();
    });
    registry->register_callback("starlet_star_cache_mem_size_percent", [=]() -> Status {
        update_staros_starcache();
        return Status::OK();
    });
    registry->register_callback("starlet_star_cache_mem_size_bytes", [=]() -> Status {
        update_staros_starcache();
        return Status::OK();
    });
#endif
    registry->register_callback("transaction_apply_worker_count", [=]() -> Status {
        int max_thread_cnt = CpuInfo::num_cores();
        if (config::transaction_apply_worker_count > 0) {
            max_thread_cnt = config::transaction_apply_worker_count;
        }
        return StorageEngine::instance()->update_manager()->apply_thread_pool()->update_max_threads(max_thread_cnt);
    });
    registry->register_callback("transaction_apply_thread_pool_num_min", [=]() -> Status {
        int min_thread_cnt = config::transaction_apply_thread_pool_num_min;
        return StorageEngine::instance()->update_manager()->apply_thread_pool()->update_min_threads(min_thread_cnt);
    });
    registry->register_callback("get_pindex_worker_count", [=]() -> Status {
        int max_thread_cnt = CpuInfo::num_cores();
        if (config::get_pindex_worker_count > 0) {
            max_thread_cnt = config::get_pindex_worker_count;
        }
        return StorageEngine::instance()->update_manager()->get_pindex_thread_pool()->update_max_threads(
                max_thread_cnt);
    });
    registry->register_callback("drop_tablet_worker_count", [=]() -> Status {
        int max_thread_cnt = std::max((int)CpuInfo::num_cores() / 2, (int)1);
        if (config::drop_tablet_worker_count > 0) {
            max_thread_cnt = config::drop_tablet_worker_count;
        }
        auto thread_pool = ExecEnv::GetInstance()->agent_server()->get_thread_pool(TTaskType::DROP);
        return thread_pool->update_max_threads(max_thread_cnt);
    });
    registry->register_callback("make_snapshot_worker_count", [=]() -> Status {
        auto thread_pool = ExecEnv::GetInstance()->agent_server()->get_thread_pool(TTaskType::MAKE_SNAPSHOT);
        return thread_pool->update_max_threads(config::make_snapshot_worker_count);
    });
    registry->register_callback("release_snapshot_worker_count", [=]() -> Status {
        auto thread_pool = ExecEnv::GetInstance()->agent_server()->get_thread_pool(TTaskType::RELEASE_SNAPSHOT);
        return thread_pool->update_max_threads(config::release_snapshot_worker_count);
    });
    registry->register_callback("pipeline_connector_scan_thread_num_per_cpu", [=]() -> Status {
        LOG(INFO) << "set pipeline_connector_scan_thread_num_per_cpu:"
                  << config::pipeline_connector_scan_thread_num_per_cpu;
        if (config::pipeline_connector_scan_thread_num_per_cpu > 0) {
            ExecEnv::GetInstance()->workgroup_manager()->change_num_connector_scan_threads(
                    config::pipeline_connector_scan_thread_num_per_cpu * CpuInfo::num_cores());
        }
        return Status::OK();
    });
    registry->register_callback("enable_resource_group_cpu_borrowing", [=]() -> Status {
        LOG(INFO) << "set enable_resource_group_cpu_borrowing:" << config::enable_resource_group_cpu_borrowing;
        ExecEnv::GetInstance()->workgroup_manager()->change_enable_resource_group_cpu_borrowing(
                config::enable_resource_group_cpu_borrowing);
        return Status::OK();
    });
    registry->register_callback("create_tablet_worker_count", [=]() -> Status {
        LOG(INFO) << "set create_tablet_worker_count:" << config::create_tablet_worker_count;
        auto thread_pool = ExecEnv::GetInstance()->agent_server()->get_thread_pool(TTaskType::CREATE);
        return thread_pool->update_max_threads(config::create_tablet_worker_count);
    });
    registry->register_callback("check_consistency_worker_count", [=]() -> Status {
        auto thread_pool = ExecEnv::GetInstance()->agent_server()->get_thread_pool(TTaskType::CHECK_CONSISTENCY);
        return thread_pool->update_max_threads(std::max(1, config::check_consistency_worker_count));
    });
    registry->register_callback("load_channel_rpc_thread_pool_num", [=]() -> Status {
        LOG(INFO) << "set load_channel_rpc_thread_pool_num:" << config::load_channel_rpc_thread_pool_num;
        return ExecEnv::GetInstance()->load_channel_mgr()->async_rpc_pool()->update_max_threads(
                config::load_channel_rpc_thread_pool_num);
    });
    registry->register_callback("exec_state_report_max_threads", [=]() -> Status {
        LOG(INFO) << "set exec_state_report_max_threads:" << config::exec_state_report_max_threads;
        ExecEnv::GetInstance()->workgroup_manager()->change_exec_state_report_max_threads(
                config::exec_state_report_max_threads);
        return Status::OK();
    });
    registry->register_callback("priority_exec_state_report_max_threads", [=]() -> Status {
        LOG(INFO) << "set priority_exec_state_report_max_threads:" << config::priority_exec_state_report_max_threads;
        ExecEnv::GetInstance()->workgroup_manager()->change_priority_exec_state_report_max_threads(
                config::priority_exec_state_report_max_threads);
        return Status::OK();
    });
    registry->register_callback("number_tablet_writer_threads", [=]() -> Status {
        int max_delta_writer_thread_num = caculate_delta_writer_thread_num(config::number_tablet_writer_threads);
        LOG(INFO) << "set max delta writer thread num: " << max_delta_writer_thread_num;
        bthreads::ThreadPoolExecutor* executor =
                static_cast<bthreads::ThreadPoolExecutor*>(StorageEngine::instance()->async_delta_writer_executor());
        return executor->get_thread_pool()->update_max_threads(max_delta_writer_thread_num);
    });
    registry->register_callback("compact_threads", [=]() -> Status {
        auto tablet_manager = exec_env->lake_tablet_manager();
        if (tablet_manager != nullptr) {
            tablet_manager->compaction_scheduler()->update_compact_threads(config::compact_threads);
        }
        return Status::OK();
    });
    registry->register_callback("load_spill_merge_memory_limit_percent", [=]() -> Status {
        // The change of load spill merge memory will be reflected in the max thread cnt of load spill merge pool.
        return StorageEngine::instance()->load_spill_block_merge_executor()->refresh_max_thread_num();
    });
    registry->register_callback("load_spill_merge_max_thread", [=]() -> Status {
        return StorageEngine::instance()->load_spill_block_merge_executor()->refresh_max_thread_num();
    });
    registry->register_callback("load_spill_memory_usage_per_merge", [=]() -> Status {
        return StorageEngine::instance()->load_spill_block_merge_executor()->refresh_max_thread_num();
    });
    registry->register_callback("merge_commit_txn_state_cache_capacity", [=]() -> Status {
        LOG(INFO) << "set merge_commit_txn_state_cache_capacity: " << config::merge_commit_txn_state_cache_capacity;
        auto batch_write_mgr = exec_env->batch_write_mgr();
        if (batch_write_mgr) {
            batch_write_mgr->txn_state_cache()->set_capacity(config::merge_commit_txn_state_cache_capacity);
        }
        return Status::OK();
    });

#ifdef USE_STAROS
#define UPDATE_STARLET_CONFIG(BE_CONFIG, STARLET_CONFIG)                                           \
    registry->register_callback(#BE_CONFIG, [=]() {                                                \
        auto val = std::to_string(config::BE_CONFIG);                                              \
        if (staros::starlet::common::GFlagsUtils::UpdateFlagValue(#STARLET_CONFIG, val).empty()) { \
            LOG(WARNING) << "Failed to update " << #STARLET_CONFIG;                                \
            return Status::InvalidArgument("Failed to update " + std::string(#BE_CONFIG) + ".");   \
        }                                                                                          \
        return Status::OK();                                                                       \
    });

    UPDATE_STARLET_CONFIG(starlet_cache_thread_num, cachemgr_threadpool_size);
    UPDATE_STARLET_CONFIG(starlet_fs_stream_buffer_size_bytes, fs_stream_buffer_size_bytes);
    UPDATE_STARLET_CONFIG(starlet_fs_read_prefetch_enable, fs_enable_buffer_prefetch);
    UPDATE_STARLET_CONFIG(starlet_fs_read_prefetch_threadpool_size, fs_buffer_prefetch_threadpool_size);
    UPDATE_STARLET_CONFIG(starlet_fslib_s3client_nonread_max_retries, fslib_s3client_nonread_max_retries);
    UPDATE_STARLET_CONFIG(starlet_fslib_s3client_nonread_retry_scale_factor, fslib_s3client_nonread_retry_scale_factor);
    UPDATE_STARLET_CONFIG(starlet_fslib_s3client_connect_timeout_ms, fslib_s3client_connect_timeout_ms);
    if (config::object_storage_request_timeout_ms >= 0 &&
        config::object_storage_request_timeout_ms <= std::numeric_limits<int32_t>::max()) {
        UPDATE_STARLET_CONFIG(object_storage_request_timeout_ms, fslib_s3client_request_timeout_ms);
    }
    UPDATE_STARLET_CONFIG(s3_use_list_objects_v1, fslib_s3client_use_list_objects_v1);
    UPDATE_STARLET_CONFIG(starlet_delete_files_max_key_in_batch, delete_files_max_key_in_batch);
#undef UPDATE_STARLET_CONFIG

#ifndef BUILD_FORMAT_LIB
    registry->register_callback("starlet_filesystem_instance_cache_capacity", [=]() -> Status {
        LOG(INFO) << "set starlet_filesystem_instance_cache_capacity:"
                  << config::starlet_filesystem_instance_cache_capacity;
        auto worker = get_staros_worker();
        if (worker) {
            worker->set_fs_cache_capacity(config::starlet_filesystem_instance_cache_capacity);
        }
        return Status::OK();
    });
#endif

#endif // USE_STAROS
}

} // namespace starrocks
