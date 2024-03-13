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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/user_function_cache.h

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

#pragma once

#include <any>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "common/status.h"
#include "common/statusor.h"

namespace starrocks {

struct UserFunctionCacheEntry;

// This class is used for caching user-defined functions.
// We will support UDF/UDAF/UDTF and user-defined window functions.
// A user-defined function has a unique function id, and we get the
// corresponding function based on the function id. If the function does
// not exist or if the md5 does not match, then the corresponding lib will be
// downloaded from the specified URL. when user wants to
// change its implementation(URL), StarRocks will generate a new function
// id.
class UserFunctionCache {
public:
    static constexpr const char* JAVA_UDF_SUFFIX = ".jar";
    static constexpr int UDF_TYPE_UNKNOWN = -1;
    static constexpr int UDF_TYPE_JAVA = 1;

    using UserFunctionCacheEntryPtr = std::shared_ptr<UserFunctionCacheEntry>;
    // local_dir is the directory which contain cached library.
    UserFunctionCache();
    ~UserFunctionCache();

    // initialize this cache, call this function before others
    Status init(const std::string& local_path);

    static UserFunctionCache* instance();

    Status get_libpath(int64_t fid, const std::string& url, const std::string& checksum, std::string* libpath);
    StatusOr<std::any> load_cacheable_java_udf(
            int64_t fid, const std::string& url, const std::string& checksum,
            const std::function<StatusOr<std::any>(const std::string& entry)>& loader);

    static int get_function_type(const std::string& url);

private:
    Status _load_cached_lib();
    Status _load_entry_from_lib(const std::string& dir, const std::string& file);
    template <class Loader>
    Status _get_cache_entry(int64_t fid, const std::string& url, const std::string& checksum,
                            UserFunctionCacheEntryPtr* output_entry, Loader&& loader);
    template <class Loader>
    Status _load_cache_entry(const std::string& url, UserFunctionCacheEntryPtr& entry, Loader&& loader);
    Status _download_lib(const std::string& url, UserFunctionCacheEntryPtr& entry);
    template <class Loader>
    Status _load_cache_entry_internal(UserFunctionCacheEntryPtr& entry, Loader&& loader);
    std::string _make_lib_file(int64_t function_id, const std::string& checksum, const std::string& shuffix);
    void _destroy_cache_entry(UserFunctionCacheEntryPtr& entry);

private:
    std::string _lib_dir;
    void* _current_process_handle = nullptr;

    std::mutex _cache_lock;
    std::unordered_map<int64_t, std::shared_ptr<UserFunctionCacheEntry>> _entry_map;
};

} // namespace starrocks
