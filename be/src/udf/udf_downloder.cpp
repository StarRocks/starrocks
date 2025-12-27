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

#include "udf_downloder.h"

#include "fs/fs.h"
#include "fs/fs_util.h"

namespace starrocks {

std::mutex udf_downloder::_download_mutex;
std::unordered_map<std::string, std::shared_ptr<std::mutex>> udf_downloder::_path_mutexes;

Status udf_downloder::download_remote_file_2_local(const std::string& remotePath, std::string& localPath, const FSOptions& options) {
    auto mtx = get_mutex_for_path(localPath);
    std::lock_guard<std::mutex> lock(*mtx);
    udf_downloder downloader;
    RETURN_IF_ERROR(downloader.setup_local_file_path(localPath));
    LOG(INFO) << fmt::format("Downloading udf file from {}", remotePath);
    RETURN_IF_ERROR(downloader.do_download(remotePath, localPath, options));
    LOG(INFO) << fmt::format("Successfully downloaded udf file from {} to {}", remotePath, localPath);

    return Status::OK();
}

Status udf_downloder::setup_local_file_path(const std::string& local_path) {
    std::string dir_path = local_path.substr(0, local_path.find_last_of('/'));
    RETURN_IF_ERROR(FileSystem::Default()->create_dir_recursive(dir_path));
    LOG(INFO) << "Successfully setup local file path";
    return Status::OK();
}

Status udf_downloder::do_download(const std::string& remotePath, std::string& localPath, const FSOptions& options) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateUniqueFromString(remotePath, options));
    if (!fs) {
        LOG(ERROR) << fmt::format("No matching filesystem for {}", remotePath);
        return Status::NotFound(fmt::format("No matching filesystem available for {}", remotePath));
    }
    ASSIGN_OR_RETURN(auto source_file, fs->new_sequential_file(remotePath));
    ASSIGN_OR_RETURN(auto local_file, FileSystem::Default()->new_writable_file(localPath));
    auto res = fs::copy(source_file.get(), local_file.get(), 1024 * 1024);
    if (!res.ok()) {
        return res.status();
    }
    return Status::OK();
}

std::shared_ptr<std::mutex> udf_downloder::get_mutex_for_path(const std::string& localPath) {
    std::lock_guard<std::mutex> map_lock(_download_mutex);
    auto iter = _path_mutexes.find(localPath);
    if (iter == _path_mutexes.end()) {
        auto mtx = std::make_shared<std::mutex>();
        _path_mutexes.emplace(localPath, mtx);
        return mtx;
    }
    return iter->second;
}

}