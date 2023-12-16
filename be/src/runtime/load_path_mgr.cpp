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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/load_path_mgr.cpp

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

#include "runtime/load_path_mgr.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <boost/algorithm/string/join.hpp>
#include <string>

#include "common/config.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "gen_cpp/Types_types.h"
#include "runtime/base_load_path_mgr.h"
#include "runtime/exec_env.h"
#include "storage/olap_define.h"
#include "storage/storage_engine.h"
#include "util/thread.h"

namespace starrocks {

static const uint32_t MAX_SHARD_NUM = 1024;
static const std::string SHARD_PREFIX = "__shard_";

LoadPathMgr::LoadPathMgr(ExecEnv* exec_env) : _exec_env(exec_env), _idx(0), _next_shard(0) {}
LoadPathMgr::~LoadPathMgr() {
    _stop.set_value(true);
    pthread_join(_cleaner_id, nullptr);
}
Status LoadPathMgr::init() {
    _path_vec.clear();
    for (auto& path : _exec_env->store_paths()) {
        _path_vec.push_back(path.path + MINI_PREFIX);
    }
    LOG(INFO) << "Load path configured to [" << boost::join(_path_vec, ",") << "]";

    // error log is saved in first root path
    _error_log_dir = _exec_env->store_paths()[0].path + ERROR_LOG_PREFIX;

    // check and make dir
    RETURN_IF_ERROR(fs::create_directories(_error_log_dir));

    _idx = 0;
    _stop_future = _stop.get_future();
    if (pthread_create(&_cleaner_id, nullptr, LoadPathMgr::cleaner, this)) {
        return Status::InternalError("Fail to create thread 'load_path_mgr'");
    }
    Thread::set_thread_name(_cleaner_id, "load_path_mgr");
    return Status::OK();
}

void* LoadPathMgr::cleaner(void* param) {
    auto* mgr = (LoadPathMgr*)param;
    while (true) {
        static constexpr auto one_hour = std::chrono::seconds(3600);
        // clean every one hour
        auto status = mgr->stop_future().wait_for(one_hour);
        if (status == std::future_status::ready) {
            return nullptr;
        } else if (status == std::future_status::timeout) {
            mgr->clean();
        }
    }
    return nullptr;
}

Status LoadPathMgr::allocate_dir(const std::string& db, const std::string& label, std::string* prefix) {
    if (_path_vec.empty()) {
        return Status::InternalError("No load path configed.");
    }
    std::string path;
    auto size = _path_vec.size();
    auto retry = size;
    Status status = Status::OK();
    while (retry--) {
        {
            // add SHARD_PREFIX for compatible purpose
            std::lock_guard<std::mutex> l(_lock);
            std::string shard = SHARD_PREFIX + std::to_string(_next_shard++ % MAX_SHARD_NUM);
            path = _path_vec[_idx] + "/" + db + "/" + shard + "/" + label;
            _idx = (_idx + 1) % size;
        }
        status = fs::create_directories(path);
        if (LIKELY(status.ok())) {
            *prefix = path;
            return Status::OK();
        } else {
            LOG(WARNING) << "create dir failed:" << path << ", error msg:" << status.message();
        }
    }

    return status;
}

bool LoadPathMgr::is_too_old(time_t cur_time, const std::string& label_dir, int64_t reserve_hours) {
    struct stat dir_stat;
    if (stat(label_dir.c_str(), &dir_stat)) {
        // State failed, just information
        PLOG(WARNING) << "stat directory failed.path=" << label_dir;
        return false;
    }

    if ((cur_time - dir_stat.st_mtime) < reserve_hours * 3600) {
        return false;
    }

    return true;
}

void LoadPathMgr::get_load_data_path(std::vector<std::string>* data_paths) {
    data_paths->insert(data_paths->end(), _path_vec.begin(), _path_vec.end());
}

const std::string ERROR_FILE_NAME = "error_log";
const std::string REJECTED_RECORD_FILE_NAME = "rejected_record";

Status LoadPathMgr::get_load_error_file_name(const TUniqueId& fragment_instance_id, std::string* error_path) {
    std::stringstream ss;
    // add shard sub dir to file path
    ss << ERROR_FILE_NAME << "_" << std::hex << fragment_instance_id.hi << "_" << fragment_instance_id.lo;
    *error_path = ss.str();
    return Status::OK();
}

std::string LoadPathMgr::get_load_error_absolute_path(const std::string& file_path) {
    std::string path;
    path.append(_error_log_dir);
    path.append("/");
    path.append(file_path);
    return path;
}

std::string LoadPathMgr::get_load_rejected_record_absolute_path(const std::string& rejected_record_dir,
                                                                const std::string& db, const std::string& label,
                                                                const int64_t id,
                                                                const TUniqueId& fragment_instance_id) {
    std::string path;
    if (rejected_record_dir.empty()) {
        path = _exec_env->store_paths()[0].path + REJECTED_RECORD_PREFIX;
    } else {
        path = rejected_record_dir;
    }

    std::stringstream ss;
    ss << path << "/" << db << "/" << label << "/" << id << "/" << std::hex << fragment_instance_id.hi << "_"
       << fragment_instance_id.lo;
    return ss.str();
}

void LoadPathMgr::process_path(time_t now, const std::string& path, int64_t reserve_hours) {
    if (!is_too_old(now, path, reserve_hours)) {
        return;
    }
    LOG(INFO) << "Going to remove path. path=" << path;
    Status status = fs::remove_all(path);
    if (status.ok()) {
        LOG(INFO) << "Remove path success. path=" << path;
    } else {
        LOG(WARNING) << "Remove path failed. path=" << path;
    }
}

void LoadPathMgr::clean_one_path(const std::string& path) {
    FileSystem* fs = FileSystem::Default();

    std::vector<std::string> dbs;
    Status status = fs->get_children(path, &dbs);
    // path may not exist
    if (!status.ok() && !status.is_not_found()) {
        LOG(WARNING) << "scan one path to delete directory failed. path=" << path;
        return;
    }

    time_t now = time(nullptr);
    for (auto& db : dbs) {
        std::string db_dir = path + "/" + db;
        std::vector<std::string> sub_dirs;
        status = fs->get_children(db_dir, &sub_dirs);
        if (!status.ok()) {
            LOG(WARNING) << "scan db of trash dir failed, continue. dir=" << db_dir;
            continue;
        }
        // delete this file
        for (auto& sub_dir : sub_dirs) {
            std::string sub_path = db_dir + "/" + sub_dir;
            // for compatible
            if (sub_dir.find(SHARD_PREFIX) == 0) {
                // sub_dir starts with SHARD_PREFIX
                // process shard sub dir
                std::vector<std::string> labels;
                Status status = fs->get_children(sub_path, &labels);
                if (!status.ok()) {
                    LOG(WARNING) << "scan one path to delete directory failed. path=" << sub_path;
                    continue;
                }
                for (auto& label : labels) {
                    std::string label_dir = sub_path + "/" + label;
                    process_path(now, label_dir, config::load_data_reserve_hours);
                }
            } else {
                // process label dir
                process_path(now, sub_path, config::load_data_reserve_hours);
            }
        }
    }
}

void LoadPathMgr::clean() {
    for (auto& path : _path_vec) {
        clean_one_path(path);
    }
    clean_error_log();
}

void LoadPathMgr::clean_error_log() {
    FileSystem* fs = FileSystem::Default();

    time_t now = time(nullptr);
    std::vector<std::string> sub_dirs;
    Status status = fs->get_children(_error_log_dir, &sub_dirs);
    if (!status.ok()) {
        LOG(WARNING) << "scan error_log dir failed. dir=" << _error_log_dir;
        return;
    }

    for (auto& sub_dir : sub_dirs) {
        std::string sub_path = _error_log_dir + "/" + sub_dir;
        // for compatible
        if (sub_dir.find(SHARD_PREFIX) == 0) {
            // sub_dir starts with SHARD_PREFIX
            // process shard sub dir
            std::vector<std::string> error_log_files;
            Status status = fs->get_children(sub_path, &error_log_files);
            if (!status.ok()) {
                LOG(WARNING) << "scan one path to delete directory failed. path=" << sub_path;
                continue;
            }
            for (auto& error_log : error_log_files) {
                std::string error_log_path = sub_path + "/" + error_log;
                process_path(now, error_log_path, config::load_error_log_reserve_hours);
            }
        } else {
            // process error log file
            process_path(now, sub_path, config::load_error_log_reserve_hours);
        }
    }
}

} // namespace starrocks
