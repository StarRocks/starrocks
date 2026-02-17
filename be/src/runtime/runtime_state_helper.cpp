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

#include "runtime/runtime_state_helper.h"

#include <filesystem>
#include <fstream>
#include <sstream>

#include "cache/datacache.h"
#include "cache/datacache_utils.h"
#include "cache/disk_cache/block_cache.h"
#include "common/logging.h"
#include "common/object_pool.h"
#include "exec/pipeline/query_context.h"
#ifdef USE_STAROS
#include "fslib/star_cache_handler.h"
#endif
#include "fs/fs_util.h"
#include "runtime/exec_env.h"
#include "runtime/load_path_mgr.h"
#include "runtime/query_statistics.h"
#include "runtime/runtime_filter_worker.h"
#include "runtime/runtime_state.h"

#ifdef STARROCKS_JIT_ENABLE
#include "exprs/jit/jit_engine.h"
#endif

namespace starrocks {

void RuntimeStateHelper::init_runtime_filter_port(RuntimeState* state) {
    state->_runtime_filter_port = state->_obj_pool->add(new RuntimeFilterPort(state));
}

ObjectPool* RuntimeStateHelper::global_obj_pool(const RuntimeState* state) {
    if (state->_query_ctx == nullptr) {
        return state->obj_pool();
    }
    return state->_query_ctx->object_pool();
}

Status RuntimeStateHelper::create_error_log_file(RuntimeState* state) {
    RETURN_IF_ERROR(state->_exec_env->load_path_mgr()->get_load_error_file_name(state->_fragment_instance_id,
                                                                                &state->_error_log_file_path));
    std::string error_log_absolute_path =
            state->_exec_env->load_path_mgr()->get_load_error_absolute_path(state->_error_log_file_path);
    state->_error_log_file = new std::ofstream(error_log_absolute_path, std::ifstream::out);
    if (!state->_error_log_file->is_open()) {
        std::stringstream error_msg;
        error_msg << "Fail to open error file: [" << state->_error_log_file_path << "].";
        LOG(WARNING) << error_msg.str();
        return Status::InternalError(error_msg.str());
    }
    return Status::OK();
}

Status RuntimeStateHelper::create_rejected_record_file(RuntimeState* state) {
    auto rejected_record_absolute_path = state->_exec_env->load_path_mgr()->get_load_rejected_record_absolute_path(
            "", state->_db, state->_load_label, state->_txn_id, state->_fragment_instance_id);
    RETURN_IF_ERROR(fs::create_directories(std::filesystem::path(rejected_record_absolute_path).parent_path()));

    state->_rejected_record_file = std::make_unique<std::ofstream>(rejected_record_absolute_path, std::ifstream::out);
    if (!state->_rejected_record_file->is_open()) {
        std::stringstream error_msg;
        error_msg << "Fail to open rejected record file: [" << rejected_record_absolute_path << "].";
        LOG(WARNING) << error_msg.str();
        return Status::InternalError(error_msg.str());
    }
    LOG(WARNING) << "rejected record file path " << rejected_record_absolute_path;
    state->_rejected_record_file_path = rejected_record_absolute_path;
    return Status::OK();
}

std::shared_ptr<QueryStatisticsRecvr> RuntimeStateHelper::query_recv(RuntimeState* state) {
    return state->_query_ctx->maintained_query_recv();
}

std::atomic_int64_t* RuntimeStateHelper::mutable_total_spill_bytes(RuntimeState* state) {
    return state->_query_ctx->mutable_total_spill_bytes();
}

bool RuntimeStateHelper::is_jit_enabled(const RuntimeState* state) {
#ifdef STARROCKS_JIT_ENABLE
    return JITEngine::get_instance()->support_jit() && state->_query_options.__isset.jit_level &&
           state->_query_options.jit_level != 0;
#else
    return false;
#endif
}

void RuntimeStateHelper::update_load_datacache_metrics(const RuntimeState* state,
                                                       TReportExecStatusParams* load_params) {
#ifndef __APPLE__
    if (!state->_query_options.__isset.catalog) {
        return;
    }

    TLoadDataCacheMetrics metrics{};
    metrics.__set_read_bytes(state->_num_datacache_read_bytes.load(std::memory_order_relaxed));
    metrics.__set_read_time_ns(state->_num_datacache_read_time_ns.load(std::memory_order_relaxed));
    metrics.__set_write_bytes(state->_num_datacache_write_bytes.load(std::memory_order_relaxed));
    metrics.__set_write_time_ns(state->_num_datacache_write_time_ns.load(std::memory_order_relaxed));
    metrics.__set_count(state->_num_datacache_count.load(std::memory_order_relaxed));

    TDataCacheMetrics t_metrics{};
    const auto* mem_cache = DataCache::GetInstance()->local_mem_cache();
    if (mem_cache != nullptr && mem_cache->is_initialized()) {
        t_metrics.__set_status(TDataCacheStatus::NORMAL);
        DataCacheUtils::set_metrics_to_thrift(t_metrics, mem_cache->cache_metrics());
    }

    if (state->_query_options.catalog == "default_catalog") {
#ifdef USE_STAROS
        if (config::starlet_use_star_cache) {
            starcache::CacheMetrics cache_metrics;
            staros::starlet::fslib::star_cache_get_metrics(&cache_metrics);
            DataCacheUtils::set_disk_metrics_to_thrift(t_metrics, cache_metrics);
            metrics.__set_metrics(t_metrics);
            load_params->__set_load_datacache_metrics(metrics);
        }
#endif // USE_STAROS
    } else {
        const LocalDiskCacheEngine* disk_cache = DataCache::GetInstance()->local_disk_cache();
        if (disk_cache != nullptr && disk_cache->is_initialized()) {
            DataCacheUtils::set_metrics_to_thrift(t_metrics, disk_cache->cache_metrics());
            metrics.__set_metrics(t_metrics);
            load_params->__set_load_datacache_metrics(metrics);
        }
    }
#endif // __APPLE__
}

} // namespace starrocks
