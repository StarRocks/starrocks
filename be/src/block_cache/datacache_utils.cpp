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

#include "block_cache/datacache_utils.h"

#include <fmt/format.h>

#include <filesystem>

#include "fs/fs.h"
#include "gutil/strings/split.h"
#include "util/parse_util.h"

namespace starrocks {
void DataCacheUtils::set_metrics_from_thrift(TDataCacheMetrics& t_metrics, const DataCacheMetrics& metrics) {
    switch (metrics.status) {
    case DataCacheStatus::NORMAL:
        t_metrics.__set_status(TDataCacheStatus::NORMAL);
        break;
    case DataCacheStatus::UPDATING:
        t_metrics.__set_status(TDataCacheStatus::UPDATING);
        break;
    case DataCacheStatus::LOADING:
        t_metrics.__set_status(TDataCacheStatus::LOADING);
        break;
    default:
        t_metrics.__set_status(TDataCacheStatus::ABNORMAL);
    }

    t_metrics.__set_disk_quota_bytes(metrics.disk_quota_bytes);
    t_metrics.__set_disk_used_bytes(metrics.disk_used_bytes);
    t_metrics.__set_mem_quota_bytes(metrics.mem_quota_bytes);
    t_metrics.__set_mem_used_bytes(metrics.mem_used_bytes);
}

Status DataCacheUtils::parse_conf_datacache_mem_size(const std::string& conf_mem_size_str, int64_t mem_limit,
                                                     size_t* mem_size) {
    int64_t parsed_mem_size = ParseUtil::parse_mem_spec(conf_mem_size_str, mem_limit);
    if (mem_limit > 0 && parsed_mem_size > mem_limit) {
        LOG(WARNING) << "the configured datacache memory size exceeds the limit, decreased it to the limit value."
                     << "mem_size: " << parsed_mem_size << ", mem_limit: " << mem_limit;
        parsed_mem_size = mem_limit;
    }
    if (parsed_mem_size < 0) {
        LOG(ERROR) << "invalid mem size for datacache: " << parsed_mem_size;
        return Status::InvalidArgument("invalid mem size for datacache");
    }
    *mem_size = parsed_mem_size;
    return Status::OK();
}

int64_t DataCacheUtils::parse_conf_datacache_disk_size(const std::string& disk_path, const std::string& disk_size_str,
                                                       int64_t disk_limit) {
    if (disk_limit <= 0) {
        std::filesystem::path dpath(disk_path);
        std::error_code ec;
        auto space_info = std::filesystem::space(dpath, ec);
        if (ec) {
            LOG(ERROR) << "fail to get disk space info, path: " << dpath << ", error: " << ec.message();
            return -1;
        }
        disk_limit = space_info.capacity;
    }
    int64_t disk_size = ParseUtil::parse_mem_spec(disk_size_str, disk_limit);
    if (disk_size > disk_limit) {
        LOG(WARNING) << "the configured datacache disk size exceeds the disk limit, decreased it to the limit value."
                     << ", path: " << disk_path << ", disk_size: " << disk_size << ", disk_limit: " << disk_limit;
        disk_size = disk_limit;
    }
    return disk_size;
}

Status DataCacheUtils::parse_conf_datacache_disk_paths(const std::string& config_path, std::vector<std::string>* paths,
                                                       bool ignore_broken_disk) {
    if (config_path.empty()) {
        return Status::OK();
    }

    size_t duplicated_count = 0;
    std::vector<std::string> path_vec = strings::Split(config_path, ";", strings::SkipWhitespace());
    for (auto& item : path_vec) {
        StripWhiteSpace(&item);
        item.erase(item.find_last_not_of('/') + 1);
        if (item.empty() || item[0] != '/') {
            LOG(WARNING) << "invalid datacache path. path: " << item;
            continue;
        }

        Status status = FileSystem::Default()->create_dir_if_missing(item);
        if (!status.ok()) {
            LOG(WARNING) << "datacache path can not be created. path: " << item;
            continue;
        }

        string canonicalized_path;
        status = FileSystem::Default()->canonicalize(item, &canonicalized_path);
        if (!status.ok()) {
            LOG(WARNING) << "datacache path can not be canonicalized. may be not exist. path: " << item;
            continue;
        }
        if (std::find(paths->begin(), paths->end(), canonicalized_path) != paths->end()) {
            LOG(WARNING) << "duplicated datacache disk path: " << item << ", ignore it.";
            ++duplicated_count;
            continue;
        }
        paths->emplace_back(canonicalized_path);
    }
    if ((path_vec.size() != (paths->size() + duplicated_count) && ignore_broken_disk)) {
        LOG(WARNING) << "fail to parse datacache_disk_path config. value: " << config_path;
        return Status::InvalidArgument("fail to parse datacache_disk_path");
    }
    return Status::OK();
}

Status DataCacheUtils::parse_conf_datacache_disk_spaces(const std::string& config_disk_path,
                                                        const std::string& config_disk_size, bool ignore_broken_disk,
                                                        std::vector<DirSpace>* disk_spaces) {
    std::vector<std::string> paths;
    RETURN_IF_ERROR(parse_conf_datacache_disk_paths(config_disk_path, &paths, ignore_broken_disk));
    for (auto& p : paths) {
        int64_t disk_size = parse_conf_datacache_disk_size(p, config_disk_size, -1);
        if (disk_size < 0) {
            LOG(ERROR) << "invalid disk size for datacache: " << disk_size;
            return Status::InvalidArgument("invalid disk size for datacache");
        }
        disk_spaces->push_back({.path = p, .size = static_cast<size_t>(disk_size)});
    }
    return Status::OK();
}

void DataCacheUtils::clean_residual_datacache(const std::string& disk_path) {
    if (!FileSystem::Default()->path_exists(disk_path).ok()) {
        // ignore none existed disk path
        return;
    }
    auto st = FileSystem::Default()->iterate_dir2(disk_path, [&](DirEntry entry) {
        if (!entry.is_dir.value_or(false) && entry.name.find("blockfile_") == 0) {
            auto file = fmt::format("{}/{}", disk_path, entry.name);
            auto ret = FileSystem::Default()->delete_file(file);
            LOG_IF(WARNING, !ret.ok()) << "fail to delete residual datacache file: " << file
                                       << ", reason: " << ret.message();
        }
        return true;
    });
    LOG_IF(WARNING, !st.ok()) << "fail to clean residual datacache data, reason: " << st.message();
}

} // namespace starrocks
