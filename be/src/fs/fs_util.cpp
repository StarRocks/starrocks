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

#include "fs/fs_util.h"

#include <fmt/format.h>

#include <iomanip>
#include <set>
#include <sstream>

#include "util/md5.h"

namespace starrocks::fs {

Status list_dirs_files(FileSystem* fs, const std::string& path, std::set<std::string>* dirs,
                       std::set<std::string>* files) {
    Status st;
    RETURN_IF_ERROR(fs->iterate_dir(path, [&](std::string_view name) {
        auto full_path = fmt::format("{}/{}", path, name);
        auto is_dir = fs->is_directory(full_path);
        if (!is_dir.ok()) {
            st = is_dir.status();
            return false;
        }
        if (*is_dir && dirs != nullptr) {
            dirs->emplace(name);
        } else if (!*is_dir && files != nullptr) {
            files->emplace(name);
        }
        return true;
    }));
    RETURN_IF_ERROR(st);
    return Status::OK();
}

Status list_dirs_files(const std::string& path, std::set<std::string>* dirs, std::set<std::string>* files) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(path));
    return list_dirs_files(fs.get(), path, dirs, files);
}

StatusOr<std::string> md5sum(const std::string& path) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(path));
    ASSIGN_OR_RETURN(auto file, fs->new_random_access_file(path));
    ASSIGN_OR_RETURN(auto length, file->get_size());
    std::unique_ptr<unsigned char[]> buf(new (std::nothrow) unsigned char[length]);
    if (UNLIKELY(buf == nullptr)) {
        return Status::MemoryAllocFailed(fmt::format("alloca size={}", length));
    }
    RETURN_IF_ERROR(file->read_fully(buf.get(), length));
    unsigned char result[MD5_DIGEST_LENGTH];
    MD5(buf.get(), length, result);
    std::stringstream ss;
    for (unsigned char i : result) {
        ss << std::setfill('0') << std::setw(2) << std::hex << (int)i;
    }
    return ss.str();
}

} // namespace starrocks::fs
