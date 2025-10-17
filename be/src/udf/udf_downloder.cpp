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


Status udf_downloder::download_remote_file_2_local(const std::string& remotePath, std::string& localPath) {
    auto mtx = get_mutex_for_path(localPath);
    std::lock_guard<std::mutex> lock(*mtx);
    udf_downloder downloader;
    RETURN_IF_ERROR(downloader.setup_local_file_path(localPath));
    LOG(INFO) << "Downloading udf file from " << remotePath;
    RETURN_IF_ERROR(downloader.do_download(remotePath, localPath));
    LOG(INFO) << "Successfully downloaded udf file from " << remotePath << " to " << localPath;
    return Status::OK();
}

Status udf_downloder::setup_local_file_path(const std::string& local_path) {
    RETURN_IF_ERROR(FileSystem::Default()->path_exists(local_path));
    RETURN_IF_ERROR(FileSystem::Default()->delete_file(local_path));
    LOG(INFO) << "Removed existing file " << local_path;
    std::string dir_path = local_path.substr(0, local_path.find_last_of('/'));
    if (!dir_path.empty()) {
        RETURN_IF_ERROR(FileSystem::Default()->create_dir(dir_path));
    }
    return Status::OK();
}

Status udf_downloder::do_download(const std::string& remotePath, std::string& localPath) {
        ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(remotePath));
        if (!fs) {
            return Status::NotFound("No matching filesystem available for " + remotePath);
        }
        std::unique_ptr<WritableFile> local_writable_file;
        ASSIGN_OR_RETURN(auto remoteFile, fs->new_sequential_file(remotePath));
        ASSIGN_OR_RETURN(local_writable_file, FileSystem::Default()->new_writable_file(localPath));
        auto res = fs::copy(remoteFile.get(), local_writable_file.get(), 1024 * 1024);
        if (!res.ok()) {
            return Status::RuntimeError("Failed to download file from " + remotePath + " to " + localPath);
        }
        RETURN_IF_ERROR(local_writable_file->close());
        return Status::OK();
    }
}