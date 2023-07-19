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
#include "common/configbase.h"
#include "common/logging.h"
#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "storage/compaction_manager.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/update_manager.h"
#include "storage/memtable_flush_executor.h"
#include "storage/page_cache.h"
#include "storage/persistent_index_compaction_manager.h"
#include "storage/segment_flush_executor.h"
#include "storage/segment_replicate_executor.h"
#include "storage/storage_engine.h"
#include "storage/update_manager.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks {

const static std::string HEADER_JSON = "application/json";

std::atomic<UpdateConfigAction*> UpdateConfigAction::_instance(nullptr);

Status UpdateConfigAction::update_config(const std::string& name, const std::string& value) {
    std::call_once(_once_flag, [&]() {
        _config_callback.emplace("scanner_thread_pool_thread_num", [&]() {
            LOG(INFO) << "set scanner_thread_pool_thread_num:" << config::scanner_thread_pool_thread_num;
            _exec_env->thread_pool()->set_num_thread(config::scanner_thread_pool_thread_num);
        });
        _config_callback.emplace("storage_page_cache_limit", [&]() {
            int64_t cache_limit = GlobalEnv::GetInstance()->get_storage_page_cache_size();
            cache_limit = GlobalEnv::GetInstance()->check_storage_page_cache_size(cache_limit);
            StoragePageCache::instance()->set_capacity(cache_limit);
        });
        _config_callback.emplace("disable_storage_page_cache", [&]() {
            if (config::disable_storage_page_cache) {
                StoragePageCache::instance()->set_capacity(0);
            } else {
                int64_t cache_limit = GlobalEnv::GetInstance()->get_storage_page_cache_size();
                cache_limit = GlobalEnv::GetInstance()->check_storage_page_cache_size(cache_limit);
                StoragePageCache::instance()->set_capacity(cache_limit);
            }
        });
        _config_callback.emplace("max_compaction_concurrency", [&]() {
            StorageEngine::instance()->compaction_manager()->update_max_threads(config::max_compaction_concurrency);
        });
        _config_callback.emplace("flush_thread_num_per_store", [&]() {
            const size_t dir_cnt = StorageEngine::instance()->get_stores().size();
            StorageEngine::instance()->memtable_flush_executor()->update_max_threads(
                    config::flush_thread_num_per_store * dir_cnt);
            StorageEngine::instance()->segment_replicate_executor()->update_max_threads(
                    config::flush_thread_num_per_store * dir_cnt);
            StorageEngine::instance()->segment_flush_executor()->update_max_threads(config::flush_thread_num_per_store *
                                                                                    dir_cnt);
        });
        _config_callback.emplace("update_compaction_num_threads_per_disk", [&]() {
            StorageEngine::instance()->increase_update_compaction_thread(
                    config::update_compaction_num_threads_per_disk);
        });
        _config_callback.emplace("pindex_bg_compaction_num_threads", [&]() {
            PersistentIndexCompactionManager* mgr =
                    StorageEngine::instance()->update_manager()->get_pindex_compaction_mgr();
            if (mgr != nullptr) {
                (void)mgr->update_max_threads(config::pindex_bg_compaction_num_threads);
            }
        });
        _config_callback.emplace("update_memory_limit_percent", [&]() {
            StorageEngine::instance()->update_manager()->update_primary_index_memory_limit(
                    config::update_memory_limit_percent);
#if defined(USE_STAROS) && !defined(BE_TEST)
            _exec_env->lake_update_manager()->update_primary_index_memory_limit(config::update_memory_limit_percent);
#endif
        });
        _config_callback.emplace("transaction_publish_version_worker_count", [&]() {
            auto thread_pool = ExecEnv::GetInstance()->agent_server()->get_thread_pool(TTaskType::PUBLISH_VERSION);
            (void)thread_pool->update_max_threads(
                    std::max(MIN_TRANSACTION_PUBLISH_WORKER_COUNT, config::transaction_publish_version_worker_count));
        });
        _config_callback.emplace("parallel_clone_task_per_path", [&]() {
            _exec_env->agent_server()->update_max_thread_by_type(TTaskType::CLONE,
                                                                 config::parallel_clone_task_per_path);
        });
        _config_callback.emplace("alter_tablet_worker_count", [&]() {
            _exec_env->agent_server()->update_max_thread_by_type(TTaskType::ALTER, config::alter_tablet_worker_count);
        });
        _config_callback.emplace("lake_metadata_cache_limit", [&]() {
            auto tablet_mgr = _exec_env->lake_tablet_manager();
            if (tablet_mgr != nullptr) tablet_mgr->update_metacache_limit(config::lake_metadata_cache_limit);
        });
        _config_callback.emplace("transaction_apply_worker_count", [&]() {
            int max_thread_cnt = CpuInfo::num_cores();
            if (config::transaction_apply_worker_count > 0) {
                max_thread_cnt = config::transaction_apply_worker_count;
            }
            (void)StorageEngine::instance()->update_manager()->apply_thread_pool()->update_max_threads(max_thread_cnt);
        });
        _config_callback.emplace("get_pindex_worker_count", [&]() {
            int max_thread_cnt = CpuInfo::num_cores();
            if (config::get_pindex_worker_count > 0) {
                max_thread_cnt = config::get_pindex_worker_count;
            }
            (void)StorageEngine::instance()->update_manager()->get_pindex_thread_pool()->update_max_threads(
                    max_thread_cnt);
        });
    });

    Status s = config::set_config(name, value);
    if (s.ok()) {
        LOG(INFO) << "set_config " << name << "=" << value << " success";
        if (_config_callback.count(name)) {
            _config_callback[name]();
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
