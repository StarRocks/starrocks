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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/user_function_cache.cpp

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

#include "runtime/user_function_cache.h"

#include <any>
#include <atomic>
#include <boost/algorithm/string/predicate.hpp> // boost::algorithm::ends_with
#include <memory>
#include <utility>
#include <vector>

#include "common/status.h"
#include "fmt/compile.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "gutil/strings/split.h"
#include "util/download_util.h"
#include "util/dynamic_util.h"
#include "util/spinlock.h"

namespace starrocks {

static const int kLibShardNum = 128;

// function cache entry, store information for
struct UserFunctionCacheEntry {
    UserFunctionCacheEntry(int64_t fid_, std::string checksum_, std::string lib_file_)
            : function_id(fid_), checksum(std::move(checksum_)), lib_file(std::move(lib_file_)) {
        function_type = UserFunctionCache::get_function_type(lib_file);
    }
    ~UserFunctionCacheEntry();

    int64_t function_id = 0;
    // used to check if this library is valid.
    std::string checksum;

    // library file
    std::string lib_file;

    // make it atomic variable instead of holding a lock
    std::atomic<bool> is_loaded{false};

    // Set to true when this library is not needed.
    // e.g. deleting some unused library to re
    std::atomic<bool> should_delete_library{false};

    // lock to make sure only one can load this cache
    std::mutex load_lock;

    // To reduce cache lock held time, cache entry is
    // added to cache map before library is downloaded.
    // And this is used to indicate whether library is downloaded.
    bool is_downloaded = false;

    // used to lookup a symbol
    // used for native function
    void* lib_handle = nullptr;

    // function type
    int function_type;

    std::any cache_handle;
};

UserFunctionCacheEntry::~UserFunctionCacheEntry() {
    // close lib_handle if it was opened
    if (lib_handle != nullptr) {
        dynamic_close(lib_handle);
        lib_handle = nullptr;
    }

    // delete library file if should_delete_library is set
    if (should_delete_library.load()) {
        unlink(lib_file.c_str());
    }
}

UserFunctionCache::UserFunctionCache() = default;

UserFunctionCache::~UserFunctionCache() {
    std::lock_guard<std::mutex> l(_cache_lock);
    _entry_map.clear();
}

UserFunctionCache* UserFunctionCache::instance() {
    static UserFunctionCache s_cache;
    return &s_cache;
}

Status UserFunctionCache::init(const std::string& lib_dir) {
    DCHECK(_lib_dir.empty());
    _lib_dir = lib_dir;
    // 1. dynamic open current process
    RETURN_IF_ERROR(dynamic_open(nullptr, &_current_process_handle));
    // 2. load all cached
    RETURN_IF_ERROR(_load_cached_lib());
    return Status::OK();
}

Status UserFunctionCache::get_libpath(int64_t fid, const std::string& url, const std::string& checksum,
                                      std::string* libpath) {
    UserFunctionCacheEntryPtr entry;
    RETURN_IF_ERROR(_get_cache_entry(fid, url, checksum, &entry,
                                     [](const auto& entry) -> StatusOr<std::any> { return std::any{}; }));
    *libpath = entry->lib_file;
    return Status::OK();
}

StatusOr<std::any> UserFunctionCache::load_cacheable_java_udf(
        int64_t fid, const std::string& url, const std::string& checksum,
        const std::function<StatusOr<std::any>(const std::string& path)>& loader) {
    UserFunctionCacheEntryPtr entry;
    RETURN_IF_ERROR(_get_cache_entry(fid, url, checksum, &entry, loader));
    return entry->cache_handle;
}

// Now we only support JAVA_UDF
Status UserFunctionCache::_load_entry_from_lib(const std::string& dir, const std::string& file) {
    if (!boost::algorithm::ends_with(file, JAVA_UDF_SUFFIX)) {
        return Status::InternalError(fmt::format("unknown library file format {}", file));
    }

    std::vector<std::string> split_parts = strings::Split(file, ".");
    if (split_parts.size() != 3) {
        return Status::InternalError("user function's name should be function_id.checksum.type");
    }
    int64_t function_id = std::stol(split_parts[0]);
    std::string checksum = split_parts[1];
    auto it = _entry_map.find(function_id);
    if (it != _entry_map.end()) {
        LOG(WARNING) << "meet a same function id user function library, function_id=" << function_id
                     << ", one_checksum=" << checksum << ", other_checksum=" << it->second->checksum;
        return Status::InternalError("duplicate function id");
    }
    // create a cache entry and put it into entry map
    auto entry = std::make_shared<UserFunctionCacheEntry>(function_id, checksum, dir + "/" + file);
    entry->is_downloaded = true;
    _entry_map[function_id] = entry;

    return Status::OK();
}
int UserFunctionCache::get_function_type(const std::string& url) {
    if (boost::algorithm::ends_with(url, JAVA_UDF_SUFFIX)) {
        return UDF_TYPE_JAVA;
    }
    return UDF_TYPE_UNKNOWN;
}

Status UserFunctionCache::_load_cached_lib() {
    // create library directory if not exist
    RETURN_IF_ERROR(fs::create_directories(_lib_dir));

    for (int i = 0; i < kLibShardNum; ++i) {
        std::string sub_dir = _lib_dir + "/" + std::to_string(i);
        RETURN_IF_ERROR(fs::create_directories(sub_dir));

        auto scan_cb = [this, &sub_dir](std::string_view file) {
            if (file == "." || file == "..") {
                return true;
            }
            auto st = _load_entry_from_lib(sub_dir, std::string(file));
            if (!st.ok()) {
                LOG(WARNING) << "load a library failed, dir=" << sub_dir << ", file=" << file;
            }
            return true;
        };
        RETURN_IF_ERROR(FileSystem::Default()->iterate_dir(sub_dir, scan_cb));
    }
    return Status::OK();
}
template <class Loader>
Status UserFunctionCache::_get_cache_entry(int64_t fid, const std::string& url, const std::string& checksum,
                                           UserFunctionCacheEntryPtr* output_entry, Loader&& loader) {
    std::string shuffix = "tmp";
    int type = get_function_type(url);
    if (type == UDF_TYPE_JAVA) {
        shuffix = JAVA_UDF_SUFFIX;
    }

    UserFunctionCacheEntryPtr entry;
    {
        std::lock_guard<std::mutex> l(_cache_lock);
        auto it = _entry_map.find(fid);
        if (it != _entry_map.end()) {
            entry = it->second;
        } else {
            entry = std::make_shared<UserFunctionCacheEntry>(fid, checksum, _make_lib_file(fid, checksum, shuffix));
            _entry_map.emplace(fid, entry);
        }
    }
    auto st = _load_cache_entry(url, entry, loader);
    if (!st.ok()) {
        LOG(WARNING) << "fail to load cache entry, fid=" << fid;
        // if we load a cache entry failed, I think we should delete this entry cache
        // evenif this cache was valid before.
        _destroy_cache_entry(entry);
        return st;
    }

    *output_entry = entry;
    return Status::OK();
}

void UserFunctionCache::_destroy_cache_entry(UserFunctionCacheEntryPtr& entry) {
    // 1. we remove cache entry from entry map
    {
        std::lock_guard<std::mutex> l(_cache_lock);
        _entry_map.erase(entry->function_id);
    }
    entry->should_delete_library.store(true);
}

template <class Loader>
Status UserFunctionCache::_load_cache_entry(const std::string& url, UserFunctionCacheEntryPtr& entry, Loader&& loader) {
    if (entry->is_loaded.load()) {
        return Status::OK();
    }

    std::unique_lock<std::mutex> l(entry->load_lock);
    if (!entry->is_downloaded) {
        RETURN_IF_ERROR(_download_lib(url, entry));
    }

    RETURN_IF_ERROR(_load_cache_entry_internal(entry, loader));
    return Status::OK();
}

// entry's lock must be held
Status UserFunctionCache::_download_lib(const std::string& url, UserFunctionCacheEntryPtr& entry) {
    DCHECK(!entry->is_downloaded);

    std::string tmp_file = entry->lib_file + ".tmp";
    std::string target_file = entry->lib_file;
    std::string expected_checksum = entry->checksum;
    RETURN_IF_ERROR(DownloadUtil::download(url, tmp_file, target_file, expected_checksum));

    entry->function_type = get_function_type(url);

    // check download
    entry->is_downloaded = true;
    return Status::OK();
}

// entry's lock must be held
template <class Loader>
Status UserFunctionCache::_load_cache_entry_internal(UserFunctionCacheEntryPtr& entry, Loader&& loader) {
    if (entry->function_type == UDF_TYPE_JAVA) {
        // nothing to do
        ASSIGN_OR_RETURN(entry->cache_handle, loader(entry->lib_file));
    } else {
        return Status::InternalError(fmt::format("unsupport udf type: {}", entry->function_type));
    }
    entry->is_loaded.store(true);
    return Status::OK();
}

std::string UserFunctionCache::_make_lib_file(int64_t function_id, const std::string& checksum,
                                              const std::string& shuffix) {
    int shard = std::abs(function_id % kLibShardNum);
    return fmt::format("{}/{}/{}.{}{}", _lib_dir, shard, function_id, checksum, shuffix);
}

} // namespace starrocks
