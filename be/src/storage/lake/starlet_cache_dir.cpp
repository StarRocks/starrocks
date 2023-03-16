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

#include "storage/lake/starlet_cache_dir.h"

namespace starrocks::lake {

StarletCacheDir::StarletCacheDir(const std::string& path, TStorageMedium::type storage_medium)
        : _path(path), _storage_medium(storage_medium), _available_bytes(0), _disk_capacity_bytes(0) {}

Status StarletCacheDir::update_capacity() {
    ASSIGN_OR_RETURN(auto space_info, FileSystem::Default()->space(_path));
    _available_bytes = space_info.available;
    _disk_capacity_bytes = space_info.capacity;
    return Status::OK();
}

StarletCacheDirInfo StarletCacheDir::get_dir_info() {
    StarletCacheDirInfo info;

    info.path = _path;
    info.storage_medium = _storage_medium;
    info.capacity = _disk_capacity_bytes;
    info.available = _available_bytes;
    return info;
}

} // namespace starrocks::lake
