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
//   https://github.com/apache/incubator-doris/blob/master/be/src/http/action/update_config_action.cpp

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

#include "http/action/update_config_action.h"

#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <mutex>
#include <string>

#include "agent/agent_common.h"
#include "agent/agent_server.h"
#include "block_cache/block_cache.h"
#include "common/configbase.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/workgroup/scan_executor.h"
#include "gutil/strings/substitute.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "storage/compaction_manager.h"
#include "storage/lake/compaction_scheduler.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/update_manager.h"
#include "storage/memtable_flush_executor.h"
#include "storage/page_cache.h"
#include "storage/persistent_index_compaction_manager.h"
#include "storage/segment_flush_executor.h"
#include "storage/segment_replicate_executor.h"
#include "storage/storage_engine.h"
#include "storage/update_manager.h"
#include "util/bthreads/executor.h"
#include "util/priority_thread_pool.hpp"

#ifdef USE_STAROS
#include "common/gflags_utils.h"
#include "service/staros_worker.h"
#endif // USE_STAROS

namespace starrocks {

const static std::string HEADER_JSON = "application/json";

std::atomic<UpdateConfigAction*> UpdateConfigAction::_instance(nullptr);

Status UpdateConfigAction::update_config(const std::string& name, const std::string& value) {
    std::call_once(_once_flag, [&]() {
        _config_callback.emplace("scanner_thread_pool_thread_num", [&]() -> Status {
            LOG(INFO) << "set scanner_thread_pool_thread_num:" << config::scanner_thread_pool_thread_num;
            _exec_env->thread_pool()->set_num_thread(config::scanner_thread_pool_thread_num);
            return Status::OK();
        });
        _config_callback.emplace("storage_page_cache_limit", [&]() -> Status {
            int64_t cache_limit = GlobalEnv::GetInstance()->get_storage_page_cache_size();
            cache_limit = GlobalEnv::GetInstance()->check_storage_page_cache_size(cache_limit);
            StoragePageCache::instance()->set_capacity(cache_limit);
            return Status::OK();
        });
        _config_callback.emplace("disable_storage_page_cache", [&]() -> Status {
            if (config::disable_storage_page_cache) {
                StoragePageCache::instance()->set_capacity(0);
            } else {
                int64_t cache_limit = GlobalEnv::GetInstance()->get_storage_page_cache_size();
                cache_limit = GlobalEnv::GetInstance()->check_storage_page_cache_size(cache_limit);
                StoragePageCache::instance()->set_capacity(cache_limit);
            }
            return Status::OK();
        });
        _config_callback.emplace("datacache_mem_size", [&]() -> Status {
            int64_t mem_limit = MemInfo::physical_mem();
            if (GlobalEnv::GetInstance()->process_mem_tracker()->has_limit()) {
                mem_limit = GlobalEnv::GetInstance()->process_mem_tracker()->limit();
            }
            size_t mem_size = parse_conf_datacache_mem_size(config::datacache_mem_size, mem_limit);
            Status st = BlockCache::instance()->update_mem_quota(mem_size, true);
            return st;
        });
        _config_callback.emplace("datacache_disk_size", [&]() -> Status {
            std::vector<DirSpace> spaces;
            Status st = parse_conf_datacache_disk_spaces(config::datacache_disk_path, config::datacache_disk_size,
                                                         config::ignore_broken_disk, &spaces);
            if (!st.ok()) {
                LOG(WARNING) << "Failed to update datacache disk spaces";
                return st;
            }
            Status st = BlockCache::instance()->adjust_disk_spaces(spaces);
            return st;
        });
        _config_callback.emplace("datacache_disk_path", _config_callback["datacache_disk_size"]);
        _config_callback.emplace("max_compaction_concurrency", [&]() -> Status {
            Status st = StorageEngine::instance()->compaction_manager()->update_max_threads(
                    config::max_compaction_concurrency);
            return st;
        });
        _config_callback.emplace("flush_thread_num_per_store", [&]() -> Status {
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
        _config_callback.emplace("lake_flush_thread_num_per_store", [&]() -> Status {
            Status st = StorageEngine::instance()->lake_memtable_flush_executor()->update_max_threads(
                    MemTableFlushExecutor::calc_max_threads_for_lake_table(StorageEngine::instance()->get_stores()));
            return st;
        });
        _config_callback.emplace("update_compaction_num_threads_per_disk", [&]() -> Status {
            StorageEngine::instance()->increase_update_compaction_thread(
                    config::update_compaction_num_threads_per_disk);
            return Status::OK();
        });
        _config_callback.emplace("pindex_major_compaction_num_threads", [&]() -> Status {
            PersistentIndexCompactionManager* mgr =
                    StorageEngine::instance()->update_manager()->get_pindex_compaction_mgr();
            if (mgr != nullptr) {
                const int max_pk_index_compaction_thread_cnt = std::max(1, config::pindex_major_compaction_num_threads);
                Status st = mgr->update_max_threads(max_pk_index_compaction_thread_cnt);
                return st;
            }
            return Status::OK();
        });
        _config_callback.emplace("update_memory_limit_percent", [&]() -> Status {
            Status st = StorageEngine::instance()->update_manager()->update_primary_index_memory_limit(
                    config::update_memory_limit_percent);
#if defined(USE_STAROS) && !defined(BE_TEST)
            st = _exec_env->lake_update_manager()->update_primary_index_memory_limit(
                    config::update_memory_limit_percent);
#endif
            return st;
        });
        _config_callback.emplace("dictionary_cache_refresh_threadpool_size", [&]() -> Status {
            if (_exec_env->dictionary_cache_pool() != nullptr) {
                Status st = _exec_env->dictionary_cache_pool()->update_max_threads(
                        config::dictionary_cache_refresh_threadpool_size);
                return st;
            }
            return Status::OK();
        });
        _config_callback.emplace("transaction_publish_version_worker_count", [&]() -> Status {
            auto thread_pool = ExecEnv::GetInstance()->agent_server()->get_thread_pool(TTaskType::PUBLISH_VERSION);
            Status st = thread_pool->update_max_threads(
                    std::max(MIN_TRANSACTION_PUBLISH_WORKER_COUNT, config::transaction_publish_version_worker_count));
            return st;
        });
        _config_callback.emplace("transaction_publish_version_thread_pool_num_min", [&]() -> Status {
            auto thread_pool = ExecEnv::GetInstance()->agent_server()->get_thread_pool(TTaskType::PUBLISH_VERSION);
            Status st = thread_pool->update_min_threads(std::max(
                    MIN_TRANSACTION_PUBLISH_WORKER_COUNT, config::transaction_publish_version_thread_pool_num_min));
            return st;
        });
        _config_callback.emplace("parallel_clone_task_per_path", [&]() -> Status {
            _exec_env->agent_server()->update_max_thread_by_type(TTaskType::CLONE,
                                                                 config::parallel_clone_task_per_path);
            return Status::OK();
        });
        _config_callback.emplace("replication_threads", [&]() -> Status {
            _exec_env->agent_server()->update_max_thread_by_type(TTaskType::REPLICATE_SNAPSHOT,
                                                                 config::replication_threads);
            return Status::OK();
        });
        _config_callback.emplace("alter_tablet_worker_count", [&]() -> Status {
            _exec_env->agent_server()->update_max_thread_by_type(TTaskType::ALTER, config::alter_tablet_worker_count);
            return Status::OK();
        });
        _config_callback.emplace("lake_metadata_cache_limit", [&]() {
            auto tablet_mgr = _exec_env->lake_tablet_manager();
            if (tablet_mgr != nullptr) tablet_mgr->update_metacache_limit(config::lake_metadata_cache_limit);
        });
#ifdef USE_STAROS
        _config_callback.emplace("starlet_use_star_cache", [&]() -> Status {
            update_staros_starcache();
            return Status::OK();
        });
        _config_callback.emplace("starlet_star_cache_mem_size_percent", [&]() -> Status {
            update_staros_starcache();
            return Status::OK();
        });
        _config_callback.emplace("starlet_star_cache_mem_size_bytes", [&]() -> Status {
            update_staros_starcache();
            return Status::OK();
        });
#endif
        _config_callback.emplace("transaction_apply_worker_count", [&]() -> Status {
            int max_thread_cnt = CpuInfo::num_cores();
            if (config::transaction_apply_worker_count > 0) {
                max_thread_cnt = config::transaction_apply_worker_count;
            }
            Status st = StorageEngine::instance()->update_manager()->apply_thread_pool()->update_max_threads(
                    max_thread_cnt);
            return st;
        });
        _config_callback.emplace("transaction_apply_thread_pool_num_min", [&]() -> Status {
            int min_thread_cnt = config::transaction_apply_thread_pool_num_min;
            Status st = StorageEngine::instance()->update_manager()->apply_thread_pool()->update_min_threads(
                    min_thread_cnt);
            return st;
        });
        _config_callback.emplace("get_pindex_worker_count", [&]() -> Status {
            int max_thread_cnt = CpuInfo::num_cores();
            if (config::get_pindex_worker_count > 0) {
                max_thread_cnt = config::get_pindex_worker_count;
            }
            Status st = StorageEngine::instance()->update_manager()->get_pindex_thread_pool()->update_max_threads(
                    max_thread_cnt);
            return st;
        });
        _config_callback.emplace("drop_tablet_worker_count", [&]() -> Status {
            auto thread_pool = ExecEnv::GetInstance()->agent_server()->get_thread_pool(TTaskType::DROP);
            Status st = thread_pool->update_max_threads(config::drop_tablet_worker_count);
            return st;
        });
        _config_callback.emplace("make_snapshot_worker_count", [&]() -> Status {
            auto thread_pool = ExecEnv::GetInstance()->agent_server()->get_thread_pool(TTaskType::MAKE_SNAPSHOT);
            Status st = thread_pool->update_max_threads(config::make_snapshot_worker_count);
            return st;
        });
        _config_callback.emplace("release_snapshot_worker_count", [&]() -> Status {
            auto thread_pool = ExecEnv::GetInstance()->agent_server()->get_thread_pool(TTaskType::RELEASE_SNAPSHOT);
            Status st = thread_pool->update_max_threads(config::release_snapshot_worker_count);
            return st;
        });
        _config_callback.emplace("pipeline_connector_scan_thread_num_per_cpu", [&]() -> Status {
            LOG(INFO) << "set pipeline_connector_scan_thread_num_per_cpu:"
                      << config::pipeline_connector_scan_thread_num_per_cpu;
            if (config::pipeline_connector_scan_thread_num_per_cpu > 0) {
                ExecEnv::GetInstance()->workgroup_manager()->change_num_connector_scan_threads(
                        config::pipeline_connector_scan_thread_num_per_cpu * CpuInfo::num_cores());
            }
            return Status::OK();
        });
        _config_callback.emplace("enable_resource_group_cpu_borrowing", [&]() -> Status {
            LOG(INFO) << "set enable_resource_group_cpu_borrowing:" << config::enable_resource_group_cpu_borrowing;
            ExecEnv::GetInstance()->workgroup_manager()->change_enable_resource_group_cpu_borrowing(
                    config::enable_resource_group_cpu_borrowing);
            return Status::OK();
        });
        _config_callback.emplace("create_tablet_worker_count", [&]() -> Status {
            LOG(INFO) << "set create_tablet_worker_count:" << config::create_tablet_worker_count;
            auto thread_pool = ExecEnv::GetInstance()->agent_server()->get_thread_pool(TTaskType::CREATE);
            Status st = thread_pool->update_max_threads(config::create_tablet_worker_count);
            return st;
        });
        _config_callback.emplace("number_tablet_writer_threads", [&]() -> Status {
            LOG(INFO) << "set number_tablet_writer_threads:" << config::number_tablet_writer_threads;
            bthreads::ThreadPoolExecutor* executor = static_cast<bthreads::ThreadPoolExecutor*>(
                    StorageEngine::instance()->async_delta_writer_executor());
            Status st = executor->get_thread_pool()->update_max_threads(config::number_tablet_writer_threads);
            return st;
        });
        _config_callback.emplace("compact_threads", [&]() -> Status {
            auto tablet_manager = _exec_env->lake_tablet_manager();
            if (tablet_manager != nullptr) {
                tablet_manager->compaction_scheduler()->update_compact_threads(config::compact_threads);
            }
            return Status::OK();
        });

#ifdef USE_STAROS
        _config_callback.emplace("starlet_cache_thread_num", [&]() -> Status {
            if (staros::starlet::common::GFlagsUtils::UpdateFlagValue("cachemgr_threadpool_size", value).empty()) {
                LOG(WARNING) << "Failed to update cachemgr_threadpool_size";
                return Status::InvalidArgument("Failed to update starlet_cache_thread_num.");
            }
            return Status::OK();
        });
        _config_callback.emplace("starlet_cache_evict_low_water", [&]() -> Status {
            if (staros::starlet::common::GFlagsUtils::UpdateFlagValue("cachemgr_evict_low_water", value).empty()) {
                LOG(WARNING) << "Failed to update cachemgr_evict_low_water";
                return Status::InvalidArgument("Failed to update starlet_cache_evict_low_water.");
            }
            return Status::OK();
        });
        _config_callback.emplace("starlet_cache_evict_percent", [&]() -> Status {
            if (staros::starlet::common::GFlagsUtils::UpdateFlagValue("cachemgr_evict_percent", value).empty()) {
                LOG(WARNING) << "Failed to update cachemgr_evict_percent";
                return Status::InvalidArgument("Failed to update starlet_cache_evict_percent.");
            }
            return Status::OK();
        });
        _config_callback.emplace("starlet_cache_evict_throughput_mb", [&]() -> Status {
            if (staros::starlet::common::GFlagsUtils::UpdateFlagValue("cachemgr_evict_throughput_mb", value).empty()) {
                LOG(WARNING) << "Failed to update cachemgr_evict_throughput_mb";
                return Status::InvalidArgument("Failed to update starlet_cache_evict_throughput_mb.");
            }
            return Status::OK();
        });
        _config_callback.emplace("starlet_cache_evict_high_water", [&]() -> Status {
            if (staros::starlet::common::GFlagsUtils::UpdateFlagValue("cachemgr_evict_high_water", value).empty()) {
                LOG(WARNING) << "Failed to update cachemgr_evict_high_water";
                return Status::InvalidArgument("Failed to update starlet_cache_evict_high_water.");
            }
            return Status::OK();
        });
        _config_callback.emplace("starlet_fs_stream_buffer_size_bytes", [&]() -> Status {
            if (staros::starlet::common::GFlagsUtils::UpdateFlagValue("fs_stream_buffer_size_bytes", value).empty()) {
                LOG(WARNING) << "Failed to update fs_stream_buffer_size_bytes";
                return Status::InvalidArgument("Failed to update starlet_fs_stream_buffer_size_bytes.");
            }
            return Status::OK();
        });
        _config_callback.emplace("starlet_fs_read_prefetch_enable", [&]() -> Status {
            if (staros::starlet::common::GFlagsUtils::UpdateFlagValue("fs_enable_buffer_prefetch", value).empty()) {
                LOG(WARNING) << "Failed to update fs_enable_buffer_prefetch";
                return Status::InvalidArgument("Failed to update starlet_fs_read_prefetch_enable.");
            }
            return Status::OK();
        });
        _config_callback.emplace("starlet_fs_read_prefetch_threadpool_size", [&]() -> Status {
            if (staros::starlet::common::GFlagsUtils::UpdateFlagValue("fs_buffer_prefetch_threadpool_size", value)
                        .empty()) {
                LOG(WARNING) << "Failed to update fs_buffer_prefetch_threadpool_size";
                return Status::InvalidArgument("Failed to update starlet_fs_read_prefetch_threadpool_size.");
            }
            return Status::OK();
        });
        _config_callback.emplace("starlet_cache_evict_interval", [&]() -> Status {
            if (staros::starlet::common::GFlagsUtils::UpdateFlagValue("cachemgr_evict_interval", value).empty()) {
                LOG(WARNING) << "Failed to update cachemgr_evict_interval";
                return Status::InvalidArgument("Failed to update starlet_cache_evict_interval.");
            }
            return Status::OK();
        });
        _config_callback.emplace("starlet_fslib_s3client_nonread_max_retries", [&]() -> Status {
            if (staros::starlet::common::GFlagsUtils::UpdateFlagValue("fslib_s3client_nonread_max_retries", value)
                        .empty()) {
                LOG(WARNING) << "Failed to update fslib_s3client_nonread_max_retries";
                return Status::InvalidArgument("Failed to update starlet_fslib_s3client_nonread_max_retries.");
            }
            return Status::OK();
        });
        _config_callback.emplace("starlet_fslib_s3client_nonread_retry_scale_factor", [&]() -> Status {
            if (staros::starlet::common::GFlagsUtils::UpdateFlagValue("fslib_s3client_nonread_retry_scale_factor",
                                                                      value)
                        .empty()) {
                LOG(WARNING) << "Failed to update fslib_s3client_nonread_retry_scale_factor";
                return Status::InvalidArgument("Failed to update starlet_fslib_s3client_nonread_retry_scale_factor.");
            }
            return Status::OK();
        });
        _config_callback.emplace("starlet_fslib_s3client_connect_timeout_ms", [&]() -> Status {
            if (staros::starlet::common::GFlagsUtils::UpdateFlagValue("fslib_s3client_connect_timeout_ms", value)
                        .empty()) {
                LOG(WARNING) << "Failed to update fslib_s3client_connect_timeout_ms";
                return Status::InvalidArgument("Failed to update starlet_fslib_s3client_connect_timeout_ms.");
            }
            return Status::OK();
        });
        _config_callback.emplace("s3_use_list_objects_v1", [&]() -> Status {
            if (staros::starlet::common::GFlagsUtils::UpdateFlagValue("fslib_s3client_use_list_objects_v1", value)
                        .empty()) {
                LOG(WARNING) << "Failed to update fslib_s3client_use_list_objects_v1";
                return Status::InvalidArgument("Failed to update s3_use_list_objects_v1.");
            }
            return Status::OK();
        });
        _config_callback.emplace("starlet_delete_files_max_key_in_batch", [&]() -> Status {
            if (staros::starlet::common::GFlagsUtils::UpdateFlagValue("delete_files_max_key_in_batch", value).empty()) {
                LOG(WARNING) << "Failed to update delete_files_max_key_in_batch";
                return Status::InvalidArgument("Failed to update starlet_delete_files_max_key_in_batch.");
            }
            return Status::OK();
        });
#endif // USE_STAROS
    });

    Status s = config::set_config(name, value);
    if (s.ok()) {
        LOG(INFO) << "set_config " << name << "=" << value << " success";
        if (_config_callback.count(name)) {
            s = _config_callback[name]();
            if (!s.ok()) {
                Status rollback_status = config::rollback_config(name);
                if (!rollback_status.ok()) {
                    LOG(WARNING) << strings::Substitute("Failed to rollback config: $0.", name);
                }
            }
        }
    }
    return s;
}

void UpdateConfigAction::handle(HttpRequest* req) {
    LOG(INFO) << req->debug_string();

    Status s;
    std::string msg;
    if (req->params()->size() != 1) {
        s = Status::InvalidArgument("");
        msg = "Now only support to set a single config once, via 'config_name=new_value'";
    } else {
        DCHECK(req->params()->size() == 1);
        const std::string& config = req->params()->begin()->first;
        const std::string& new_value = req->params()->begin()->second;
        s = update_config(config, new_value);
        if (!s.ok()) {
            LOG(WARNING) << "set_config " << config << "=" << new_value << " failed";
            msg = strings::Substitute("set $0=$1 failed, reason: $2", config, new_value, s.to_string());
        }
    }

    std::string status(s.ok() ? "OK" : "BAD");
    rapidjson::Document root;
    root.SetObject();
    root.AddMember("status", rapidjson::Value(status.c_str(), status.size()), root.GetAllocator());
    root.AddMember("msg", rapidjson::Value(msg.c_str(), msg.size()), root.GetAllocator());
    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);

    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    HttpChannel::send_reply(req, HttpStatus::OK, strbuf.GetString());
}

} // namespace starrocks
