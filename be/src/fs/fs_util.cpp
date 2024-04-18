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

#include "gutil/strings/util.h"
#include "util/md5.h"
#include "util/string_parser.hpp"

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

bool is_starlet_uri(std::string_view uri) {
    return HasPrefixString(uri, "staros://");
}

std::string build_starlet_uri(int64_t shard_id, std::string_view path) {
    while (!path.empty() && path.front() == '/') {
        path.remove_prefix(1);
    }
    return path.empty() ? fmt::format("staros://{}", shard_id) : fmt::format("staros://{}/{}", shard_id, path);
}

// Expected format of uri: staros://ShardID/path/to/file
StatusOr<std::pair<std::string, int64_t>> parse_starlet_uri(std::string_view uri) {
    std::string_view path = uri;
    if (!HasPrefixString(path, "staros://")) {
        return Status::InvalidArgument(fmt::format("Invalid starlet URI: {}", uri));
    }
    path.remove_prefix(sizeof("staros://") - 1);
    auto end_shard_id = path.find('/');
    if (end_shard_id == std::string::npos) {
        end_shard_id = path.size();
    }

    StringParser::ParseResult result;
    auto shard_id = StringParser::string_to_int<int64_t>(path.data(), end_shard_id, &result);
    if (result != StringParser::PARSE_SUCCESS) {
        return Status::InvalidArgument(fmt::format("Invalid starlet URI: {}", uri));
    }
    path.remove_prefix(std::min<size_t>(path.size(), end_shard_id + 1));
    return std::make_pair(std::string(path), shard_id);
};

} // namespace starrocks::fs
