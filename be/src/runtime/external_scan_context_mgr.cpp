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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/external_scan_context_mgr.cpp

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

#include "runtime/external_scan_context_mgr.h"

#include <chrono>
#include <functional>
#include <memory>
#include <vector>

#include "exec/pipeline/query_context.h"
#include "runtime/fragment_mgr.h"
#include "runtime/result_queue_mgr.h"
#include "util/starrocks_metrics.h"
#include "util/thread.h"
#include "util/uid_util.h"

namespace starrocks {

ExternalScanContextMgr::ExternalScanContextMgr(ExecEnv* exec_env) : _exec_env(exec_env) {
    // start the reaper thread for gc the expired context
    _keep_alive_reaper = std::make_unique<std::thread>(
            std::bind<void>(std::mem_fn(&ExternalScanContextMgr::gc_expired_context), this));
    Thread::set_thread_name(_keep_alive_reaper.get()->native_handle(), "kepalive_reaper");
    REGISTER_GAUGE_STARROCKS_METRIC(active_scan_context_count, [this]() {
        std::lock_guard<std::mutex> l(_lock);
        return _active_contexts.size();
    });
}

ExternalScanContextMgr::~ExternalScanContextMgr() {
    {
        std::lock_guard<std::mutex> l(_lock);
        _closing = true;
    }
    // Only _keep_alive_reaper is expected to be waiting for _cv.
    _cv.notify_one();
    _keep_alive_reaper->join();
}

Status ExternalScanContextMgr::create_scan_context(std::shared_ptr<ScanContext>* p_context) {
    std::string context_id = generate_uuid_string();
    std::shared_ptr<ScanContext> context(new ScanContext(context_id));
    // context->last_access_time  = time(NULL);
    {
        std::lock_guard<std::mutex> l(_lock);
        _active_contexts.insert(std::make_pair(context_id, context));
    }
    *p_context = context;
    return Status::OK();
}

Status ExternalScanContextMgr::get_scan_context(const std::string& context_id,
                                                std::shared_ptr<ScanContext>* p_context) {
    {
        std::lock_guard<std::mutex> l(_lock);
        auto iter = _active_contexts.find(context_id);
        if (iter != _active_contexts.end()) {
            *p_context = iter->second;
        } else {
            LOG(WARNING) << "get_scan_context error: context id [ " << context_id << " ] not found";
            std::stringstream msg;
            msg << "context_id: " << context_id << " not found";
            return Status::NotFound(msg.str());
        }
    }
    return Status::OK();
}

Status ExternalScanContextMgr::clear_scan_context(const std::string& context_id) {
    std::shared_ptr<ScanContext> context;
    {
        std::lock_guard<std::mutex> l(_lock);
        auto iter = _active_contexts.find(context_id);
        if (iter != _active_contexts.end()) {
            context = iter->second;
            if (context == nullptr) {
                _active_contexts.erase(context_id);
                return Status::OK();
            }
            iter = _active_contexts.erase(iter);
        }
    }
    if (context != nullptr) {
        // cancel pipeline
        const auto& fragment_instance_id = context->fragment_instance_id;
        if (auto query_ctx = _exec_env->query_context_mgr()->get(context->query_id); query_ctx != nullptr) {
            if (auto fragment_ctx = query_ctx->fragment_mgr()->get(fragment_instance_id); fragment_ctx != nullptr) {
                std::stringstream msg;
                msg << "FragmentContext(id=" << print_id(fragment_instance_id) << ") cancelled by close_scanner";
                fragment_ctx->cancel(Status::Cancelled(msg.str()));
            }
        }
        LOG(INFO) << "close scan context: context id [ " << context_id << " ], fragment instance id [ "
                  << print_id(fragment_instance_id) << " ]";
        // clear the fragment instance's related result queue
        RETURN_IF_ERROR(_exec_env->result_queue_mgr()->cancel(fragment_instance_id));
    }
    return Status::OK();
}

std::vector<std::map<std::string, std::string>> ExternalScanContextMgr::get_active_contexts() {
    std::vector<std::map<std::string, std::string>> _active_contexts_message;
    {
        for (auto& [context_id, v] : _active_contexts) {
            std::map<std::string, std::string> context_message;
            context_message["context_id"] = context_id;
            context_message["query_id"] = print_id(v->query_id);
            context_message["last_access_time"] = ctime(&v->last_access_time);
            context_message["keep_alive_min"] = std::to_string(v->keep_alive_min);
            context_message["offset"] = std::to_string(v->offset);
            _active_contexts_message.push_back(context_message);
        }
    }
    return _active_contexts_message;
}

Status ExternalScanContextMgr::clear_inactive_scan_contexts(const int64_t inactive_time_s) {
    std::vector<std::string> contexts_to_clear;
    time_t now;
    time(&now);

    // Iterate over active contexts and prepare a list of context IDs to clear.
    for (const auto& [context_id, v] : _active_contexts) {
        int64_t diff = now - v->last_access_time;
        if (diff > inactive_time_s) {
            contexts_to_clear.push_back(context_id);
        }
    }

    // Clear the inactive scan contexts using the prepared list.
    for (const auto& context_id : contexts_to_clear) {
        Status st = clear_scan_context(context_id);
        if (!st.ok()) {
            std::string msg = strings::Substitute("Fail to clear_scan_context. error: $0, context id: $1", st.message(),
                                                  context_id);
            LOG(WARNING) << msg;
            return Status::InternalError(msg);
        }
    }

    return Status::OK();
}

void ExternalScanContextMgr::gc_expired_context() {
#ifndef BE_TEST
    while (true) {
        time_t current_time = time(nullptr);
        std::vector<std::shared_ptr<ScanContext>> expired_contexts;
        {
            std::unique_lock<std::mutex> l(_lock);
            _cv.wait_for(l, std::chrono::seconds(starrocks::config::scan_context_gc_interval_min * 60));

            if (_closing) {
                return;
            }

            for (auto iter = _active_contexts.begin(); iter != _active_contexts.end();) {
                auto context = iter->second;
                if (context == nullptr) {
                    iter = _active_contexts.erase(iter);
                    continue;
                }
                // being processed or timeout is disabled
                if (context->last_access_time == -1) {
                    ++iter; // advance one entry
                    continue;
                }
                // free context if context is idle for context->keep_alive_min
                if (current_time - context->last_access_time > context->keep_alive_min * 60) {
                    LOG(INFO) << "gc expired scan context: context id [ " << context->context_id << " ]";
                    expired_contexts.push_back(context);
                    iter = _active_contexts.erase(iter);
                } else {
                    ++iter; // advanced
                }
            }
        }
        for (const auto& expired_context : expired_contexts) {
            // must cancel the fragment instance, otherwise return thrift transport TTransportException
            WARN_IF_ERROR(
                    _exec_env->fragment_mgr()->cancel(expired_context->fragment_instance_id),
                    strings::Substitute("Fail to cancel fragment $0", print_id(expired_context->fragment_instance_id)));
            WARN_IF_ERROR(_exec_env->result_queue_mgr()->cancel(expired_context->fragment_instance_id),
                          strings::Substitute("Fail to cancel fragment $0 in result queue mgr",
                                              print_id(expired_context->fragment_instance_id)));
        }
    }
#endif
}
} // namespace starrocks
