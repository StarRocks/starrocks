// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/runtime_state.cpp

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

#include "runtime/runtime_state.h"

#include <boost/algorithm/string/join.hpp>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

#include "common/logging.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exec/exec_node.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/load_path_mgr.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_filter_worker.h"
#include "util/load_error_hub.h"
#include "util/pretty_printer.h"
#include "util/timezone_utils.h"
#include "util/uid_util.h"

namespace starrocks {

// for ut only
RuntimeState::RuntimeState(const TUniqueId& fragment_instance_id, const TQueryOptions& query_options,
                           const TQueryGlobals& query_globals, ExecEnv* exec_env)
        : _unreported_error_idx(0),
          _obj_pool(new ObjectPool()),
          _is_cancelled(false),
          _per_fragment_instance_idx(0),
          _num_rows_load_total(0),
          _num_rows_load_filtered(0),
          _num_rows_load_unselected(0),
          _num_print_error_rows(0) {
    _profile = std::make_shared<RuntimeProfile>("Fragment " + print_id(fragment_instance_id));
    Status status = init(fragment_instance_id, query_options, query_globals, exec_env);
    DCHECK(status.ok());
}

RuntimeState::RuntimeState(const TUniqueId& query_id, const TUniqueId& fragment_instance_id,
                           const TQueryOptions& query_options, const TQueryGlobals& query_globals, ExecEnv* exec_env)
        : _unreported_error_idx(0),
          _query_id(query_id),
          _obj_pool(new ObjectPool()),
          _is_cancelled(false),
          _per_fragment_instance_idx(0),
          _num_rows_load_total(0),
          _num_rows_load_filtered(0),
          _num_rows_load_unselected(0),
          _num_print_error_rows(0) {
    _profile = std::make_shared<RuntimeProfile>("Fragment " + print_id(fragment_instance_id));
    Status status = init(fragment_instance_id, query_options, query_globals, exec_env);
    DCHECK(status.ok());
}

RuntimeState::RuntimeState(const TQueryGlobals& query_globals)
        : _unreported_error_idx(0), _obj_pool(new ObjectPool()), _is_cancelled(false), _per_fragment_instance_idx(0) {
    _profile = std::make_shared<RuntimeProfile>("<unnamed>");
    _query_options.batch_size = DEFAULT_BATCH_SIZE;
    if (query_globals.__isset.time_zone) {
        _timezone = query_globals.time_zone;
        _timestamp_ms = query_globals.timestamp_ms;
    } else if (!query_globals.now_string.empty()) {
        _timezone = TimezoneUtils::default_time_zone;
        DateTimeValue dt;
        dt.from_date_str(query_globals.now_string.c_str(), query_globals.now_string.size());
        int64_t timestamp;
        dt.unix_timestamp(&timestamp, _timezone);
        _timestamp_ms = timestamp * 1000;
    } else {
        //Unit test may set into here
        _timezone = TimezoneUtils::default_time_zone;
        _timestamp_ms = 0;
    }
    TimezoneUtils::find_cctz_time_zone(_timezone, _timezone_obj);
}

RuntimeState::~RuntimeState() {
    // close error log file
    if (_error_log_file != nullptr && _error_log_file->is_open()) {
        _error_log_file->close();
        delete _error_log_file;
        _error_log_file = nullptr;
    }

    if (_error_hub != nullptr) {
        _error_hub->close();
    }

    if (_exec_env != nullptr && _exec_env->thread_mgr() != nullptr) {
        _exec_env->thread_mgr()->unregister_pool(_resource_pool);
    }
}

Status RuntimeState::init(const TUniqueId& fragment_instance_id, const TQueryOptions& query_options,
                          const TQueryGlobals& query_globals, ExecEnv* exec_env) {
    _fragment_instance_id = fragment_instance_id;
    _query_options = query_options;
    if (query_globals.__isset.time_zone) {
        _timezone = query_globals.time_zone;
        _timestamp_ms = query_globals.timestamp_ms;
    } else if (!query_globals.now_string.empty()) {
        _timezone = TimezoneUtils::default_time_zone;
        DateTimeValue dt;
        dt.from_date_str(query_globals.now_string.c_str(), query_globals.now_string.size());
        int64_t timestamp;
        dt.unix_timestamp(&timestamp, _timezone);
        _timestamp_ms = timestamp * 1000;
    } else {
        //Unit test may set into here
        _timezone = TimezoneUtils::default_time_zone;
        _timestamp_ms = 0;
    }
    if (query_globals.__isset.last_query_id) {
        _last_query_id = query_globals.last_query_id;
    }
    TimezoneUtils::find_cctz_time_zone(_timezone, _timezone_obj);

    _exec_env = exec_env;

    if (_query_options.max_errors <= 0) {
        // TODO: fix linker error and uncomment this
        //_query_options.max_errors = config::max_errors;
        _query_options.max_errors = 100;
    }

    if (_query_options.batch_size <= 0) {
        _query_options.batch_size = DEFAULT_BATCH_SIZE;
    }

    // Register with the thread mgr
    if (exec_env != nullptr) {
        _resource_pool = exec_env->thread_mgr()->register_pool();
        DCHECK(_resource_pool != nullptr);
    }
    _db_name = "insert_stmt";
    _import_label = print_id(fragment_instance_id);
    _runtime_filter_port = _obj_pool->add(new RuntimeFilterPort(this));

    return Status::OK();
}

void RuntimeState::init_mem_trackers(const TUniqueId& query_id) {
    bool has_query_mem_tracker = _query_options.__isset.mem_limit && (_query_options.mem_limit > 0);
    int64_t bytes_limit = has_query_mem_tracker ? _query_options.mem_limit : -1;
    auto* mem_tracker_counter = ADD_COUNTER(_profile.get(), "MemoryLimit", TUnit::BYTES);
    mem_tracker_counter->set(bytes_limit);

    _query_mem_tracker = std::make_shared<MemTracker>(MemTracker::QUERY, bytes_limit, runtime_profile()->name(),
                                                      _exec_env->query_pool_mem_tracker());
    _instance_mem_tracker =
            std::make_shared<MemTracker>(_profile.get(), -1, runtime_profile()->name(), _query_mem_tracker.get());
    _instance_mem_pool = std::make_unique<MemPool>();
}

Status RuntimeState::init_instance_mem_tracker() {
    _instance_mem_tracker = std::make_unique<MemTracker>(-1);
    _instance_mem_pool = std::make_unique<MemPool>();
    return Status::OK();
}

std::string RuntimeState::error_log() {
    std::lock_guard<std::mutex> l(_error_log_lock);
    return boost::algorithm::join(_error_log, "\n");
}

bool RuntimeState::log_error(const std::string& error) {
    std::lock_guard<std::mutex> l(_error_log_lock);

    if (_error_log.size() < _query_options.max_errors) {
        _error_log.push_back(error);
        return true;
    }

    return false;
}

void RuntimeState::log_error(const Status& status) {
    if (status.ok()) {
        return;
    }

    log_error(status.get_error_msg());
}

void RuntimeState::get_unreported_errors(std::vector<std::string>* new_errors) {
    std::lock_guard<std::mutex> l(_error_log_lock);

    if (_unreported_error_idx < _error_log.size()) {
        new_errors->assign(_error_log.begin() + _unreported_error_idx, _error_log.end());
        _unreported_error_idx = _error_log.size();
    }
}

Status RuntimeState::set_mem_limit_exceeded(MemTracker* tracker, int64_t failed_allocation_size,
                                            const std::string* msg) {
    DCHECK_GE(failed_allocation_size, 0);
    {
        std::lock_guard<std::mutex> l(_process_status_lock);
        if (_process_status.ok()) {
            if (msg != nullptr) {
                _process_status = Status::MemoryLimitExceeded(*msg);
            } else {
                _process_status = Status::MemoryLimitExceeded("Memory limit exceeded");
            }
        } else {
            return _process_status;
        }
    }

    DCHECK(_query_mem_tracker.get() != nullptr);
    std::stringstream ss;
    ss << "Memory Limit Exceeded\n";
    if (failed_allocation_size != 0) {
        DCHECK(tracker != nullptr);
        ss << "  " << tracker->label() << " could not allocate "
           << PrettyPrinter::print(failed_allocation_size, TUnit::BYTES) << " without exceeding limit." << std::endl;
    }

    log_error(ss.str());
    DCHECK(_process_status.is_mem_limit_exceeded());
    return _process_status;
}

Status RuntimeState::check_query_state(const std::string& msg) {
    // TODO: it would be nice if this also checked for cancellation, but doing so breaks
    // cases where we use Status::Cancelled("Cancelled") to indicate that the limit was reached.
    return query_status();
}

Status RuntimeState::check_mem_limit(const std::string& msg) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wnonnull-compare"
    RETURN_IF_LIMIT_EXCEEDED(this, msg);
#pragma pop
    return Status::OK();
}

const int64_t MAX_ERROR_NUM = 50;

Status RuntimeState::create_error_log_file() {
    _exec_env->load_path_mgr()->get_load_error_file_name(_db_name, _import_label, _fragment_instance_id,
                                                         &_error_log_file_path);
    std::string error_log_absolute_path =
            _exec_env->load_path_mgr()->get_load_error_absolute_path(_error_log_file_path);
    _error_log_file = new std::ofstream(error_log_absolute_path, std::ifstream::out);
    if (!_error_log_file->is_open()) {
        std::stringstream error_msg;
        error_msg << "Fail to open error file: [" << _error_log_file_path << "].";
        LOG(WARNING) << error_msg.str();
        return Status::InternalError(error_msg.str());
    }
    return Status::OK();
}

void RuntimeState::append_error_msg_to_file(const std::string& line, const std::string& error_msg, bool is_summary) {
    if (_query_options.query_type != TQueryType::LOAD) {
        return;
    }
    // If file havn't been opened, open it here
    if (_error_log_file == nullptr) {
        Status status = create_error_log_file();
        if (!status.ok()) {
            LOG(WARNING) << "Create error file log failed. because: " << status.get_error_msg();
            if (_error_log_file != nullptr) {
                _error_log_file->close();
                delete _error_log_file;
                _error_log_file = nullptr;
            }
            return;
        }
    }

    // if num of printed error row exceeds the limit, and this is not a summary message,
    // return
    if (_num_print_error_rows.fetch_add(1, std::memory_order_relaxed) > MAX_ERROR_NUM && !is_summary) {
        return;
    }

    std::stringstream out;
    if (is_summary) {
        out << "Summary: ";
        out << error_msg;
    } else {
        // Note: export reason first in case src line too long and be truncated.
        out << "Reason: " << error_msg;
        out << ". src line: [" << line << "]; ";
    }

    if (!out.str().empty()) {
        (*_error_log_file) << out.str() << std::endl;
        export_load_error(out.str());
    }
}

const int64_t HUB_MAX_ERROR_NUM = 10;

void RuntimeState::export_load_error(const std::string& err_msg) {
    if (_error_hub == nullptr) {
        if (_load_error_hub_info == nullptr) {
            return;
        }
        LoadErrorHub::create_hub(_exec_env, _load_error_hub_info.get(), _error_log_file_path, &_error_hub);
    }

    LoadErrorHub::ErrorMsg err(_load_job_id, err_msg);
    // TODO(lingbin): think if should check return value?
    _error_hub->export_error(err);
}

int64_t RuntimeState::get_load_mem_limit() const {
    if (_query_options.__isset.load_mem_limit && _query_options.load_mem_limit > 0) {
        return _query_options.load_mem_limit;
    }
    return 0;
}

const vectorized::GlobalDictMaps& RuntimeState::get_query_global_dict_map() const {
    return _query_global_dicts;
}

const vectorized::GlobalDictMaps& RuntimeState::get_load_global_dict_map() const {
    return _load_global_dicts;
}

vectorized::GlobalDictMaps* RuntimeState::mutable_query_global_dict_map() {
    return &_query_global_dicts;
}

Status RuntimeState::init_query_global_dict(const GlobalDictLists& global_dict_list) {
    return _build_global_dict(global_dict_list, &_query_global_dicts);
}

Status RuntimeState::init_load_global_dict(const GlobalDictLists& global_dict_list) {
    return _build_global_dict(global_dict_list, &_load_global_dicts);
}

Status RuntimeState::_build_global_dict(const GlobalDictLists& global_dict_list, vectorized::GlobalDictMaps* result) {
    for (const auto& global_dict : global_dict_list) {
        DCHECK_EQ(global_dict.ids.size(), global_dict.strings.size());
        vectorized::GlobalDictMap dict_map;
        vectorized::RGlobalDictMap rdict_map;
        int dict_sz = global_dict.ids.size();
        for (int i = 0; i < dict_sz; ++i) {
            const std::string& dict_key = global_dict.strings[i];
            auto* data = _instance_mem_pool->allocate(dict_key.size());
            memcpy(data, dict_key.data(), dict_key.size());
            Slice slice(data, dict_key.size());
            dict_map.emplace(slice, global_dict.ids[i]);
            rdict_map.emplace(global_dict.ids[i], slice);
        }
        result->emplace(uint32_t(global_dict.columnId), std::make_pair(std::move(dict_map), std::move(rdict_map)));
    }
    return Status::OK();
}

} // end namespace starrocks
