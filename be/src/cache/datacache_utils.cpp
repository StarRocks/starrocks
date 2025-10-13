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

#include "cache/datacache_utils.h"

#include <fmt/format.h>
#include <sys/stat.h>

#include <filesystem>

#include "absl/status/statusor.h"
#include "absl/strings/str_split.h"
#include "cache/mem_cache/local_mem_cache_engine.h"
#include "fs/fs.h"
#include "gutil/strings/split.h"
#include "util/parse_util.h"

namespace starrocks {

void DataCacheUtils::set_metrics_to_thrift(TDataCacheMetrics& t_metrics, const DataCacheDiskMetrics& metrics) {
    t_metrics.__set_status(DataCacheStatusUtils::to_thrift(metrics.status));
    t_metrics.__set_disk_quota_bytes(metrics.disk_quota_bytes);
    t_metrics.__set_disk_used_bytes(metrics.disk_used_bytes);
}

void DataCacheUtils::set_metrics_to_thrift(TDataCacheMetrics& t_metrics, const DataCacheMemMetrics& metrics) {
    t_metrics.__set_mem_quota_bytes(metrics.mem_quota_bytes);
    t_metrics.__set_mem_used_bytes(metrics.mem_used_bytes);
}

#ifdef WITH_STARCACHE
void DataCacheUtils::set_disk_metrics_to_thrift(TDataCacheMetrics& t_metrics, const StarCacheMetrics& metrics) {
    t_metrics.__set_status(DataCacheStatusUtils::to_thrift(static_cast<DataCacheStatus>(metrics.status)));
    t_metrics.__set_disk_quota_bytes(metrics.disk_quota_bytes);
    t_metrics.__set_disk_used_bytes(metrics.disk_used_bytes);
}
#endif

Status DataCacheUtils::parse_conf_datacache_mem_size(const std::string& conf_mem_size_str, int64_t mem_limit,
                                                     size_t* mem_size) {
    ASSIGN_OR_RETURN(int64_t parsed_mem_size, ParseUtil::parse_mem_spec(conf_mem_size_str, mem_limit));
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

StatusOr<int64_t> DataCacheUtils::parse_conf_datacache_disk_size(const std::string& disk_path,
                                                                 const std::string& disk_size_str, int64_t disk_limit) {
    if (disk_limit <= 0) {
        std::filesystem::path dpath(disk_path);
        std::error_code ec;
        auto space_info = std::filesystem::space(dpath, ec);
        if (ec) {
            auto err_str = fmt::format("fail to get disk space info, path: {}, error: {}", disk_path, ec.message());
            LOG(ERROR) << err_str;
            return Status::InternalError(err_str);
        }
        disk_limit = space_info.capacity;
    }
    ASSIGN_OR_RETURN(int64_t disk_size, ParseUtil::parse_mem_spec(disk_size_str, disk_limit));
    if (disk_size > disk_limit) {
        LOG(WARNING) << "the configured datacache disk size exceeds the disk limit, decreased it to the limit value."
                     << ", path: " << disk_path << ", disk_size: " << disk_size << ", disk_limit: " << disk_limit;
        disk_size = disk_limit;
    }
    LOG(INFO) << "parse_conf_datacache_disk_size, disk_path: " << disk_path << ", disk_size: " << disk_size << ", disk_limit: " << disk_limit
        << ", disk_size_str: " << disk_size_str;
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

Status DataCacheUtils::change_disk_path(const std::string& old_disk_path, const std::string& new_disk_path) {
    std::filesystem::path old_path(old_disk_path);
    std::filesystem::path new_path(new_disk_path);
    if (std::filesystem::exists(old_path)) {
        if (disk_device_id(old_path.c_str()) != disk_device_id(new_path.c_str())) {
            LOG(ERROR) << "fail to rename the old datacache directory [" << old_path.string() << "] to the new one ["
                       << new_path.string() << "] because they are located on different disks.";
            return Status::InternalError("The old datacache directory is different from the new one");
        }
        std::error_code ec;
        std::filesystem::remove_all(new_path, ec);
        if (!ec) {
            std::filesystem::rename(old_path, new_path, ec);
        }
        if (ec) {
            LOG(ERROR) << "fail to rename the old datacache directory [" << old_path.string() << "] to the new one ["
                       << new_path.string() << "], reason: " << ec.message();
            return Status::InternalError("fail to handle the old starlet_cache data");
        }
    }
    return Status::OK();
}

dev_t DataCacheUtils::disk_device_id(const std::string& disk_path) {
    std::filesystem::path cur_path(disk_path);
    cur_path = std::filesystem::absolute(cur_path);

    // Traverse from the current path to the ancestor node and find the first existing path
    while (!cur_path.empty()) {
        if (std::filesystem::exists(cur_path) || cur_path == cur_path.root_path()) {
            break;
        }
        cur_path = cur_path.parent_path();
    }

    struct stat s;
    if (stat(cur_path.c_str(), &s) != 0) {
        return 0;
    }
    return s.st_dev;
}

#ifdef USE_STAROS
StatusOr<std::vector<std::string>> DataCacheUtils::get_corresponding_starlet_cache_dir(
        const std::vector<StorePath>& store_paths, const std::string& starlet_cache_dir) {
    std::vector<std::string> corresponding_starlet_dirs;
    if (starlet_cache_dir.empty()) {
        return corresponding_starlet_dirs;
    }
    absl::StatusOr<std::vector<std::string>> vec_or = absl::StrSplit(starlet_cache_dir, ':', absl::SkipWhitespace());
    if (!vec_or.ok()) {
        std::string error_str = "Fail to parse starlet_cache_dir, error: " + std::string(vec_or.status().message());
        return Status::InternalError(error_str);
    }
    std::vector<std::string> starlet_paths = *vec_or;
    if (starlet_paths.empty()) {
        return corresponding_starlet_dirs;
    }
    std::unordered_map<dev_t, std::string> starlet_devices;
    for (auto& starlet_path : starlet_paths) {
        auto id = DataCacheUtils::disk_device_id(starlet_path);
        if (id == 0) {
            std::string error_str =
                    "Fail to get device id for " + starlet_path + ", error: " + std::string(strerror(errno));
            return Status::InternalError(error_str);
        }
        auto iter = starlet_devices.find(id);
        if (iter == starlet_devices.end()) {
            starlet_devices[id] = starlet_path;
        } else {
            std::string error_str = "Find 2 starlet cache dir on same device, " + starlet_path + ":" + iter->second;
            return Status::InternalError(error_str);
        }
    }

    for (auto& store_path : store_paths) {
        std::string root_path = store_path.path;
        auto id = DataCacheUtils::disk_device_id(root_path);
        if (id == 0) {
            std::string error_str =
                    "Fail to get device id for " + root_path + ", error: " + std::string(strerror(errno));
            return Status::InternalError(error_str);
        }
        auto iter = starlet_devices.find(id);
        if (iter != starlet_devices.end()) {
            corresponding_starlet_dirs.push_back(iter->second + "/star_cache");
            starlet_devices.erase(id);
        } else {
            corresponding_starlet_dirs.push_back(root_path + "/starlet_cache/star_cache");
        }
    }
    if (!starlet_devices.empty()) {
        std::string error_str = "can not find corresponding storage path for starlet cache dir, ";
        for (auto& e : starlet_devices) {
            error_str += e.second + ":";
        }
        return Status::InternalError(error_str);
    }
    return corresponding_starlet_dirs;
}
#endif

} // namespace starrocks
