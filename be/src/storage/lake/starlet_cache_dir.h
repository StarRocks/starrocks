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

#pragma once

#include <string>

#include "common/status.h"
#include "fs/fs.h"
#include "gen_cpp/Types_types.h"

namespace starrocks::lake {

struct StarletCacheDirInfo {
    StarletCacheDirInfo() = default;

    ~StarletCacheDirInfo() = default;

    std::string path;
    TStorageMedium::type storage_medium; // storage medium: SSD|HDD

    int64_t capacity{1};
    int64_t available{0};
    int64_t used_capacity{0};
};

class StarletCacheDir {
public:
    explicit StarletCacheDir(const std::string& path, TStorageMedium::type storage_medium = TStorageMedium::HDD);

    ~StarletCacheDir() = default;

    const std::string& path() const { return _path; }

    Status update_capacity();

    StarletCacheDirInfo get_dir_info();

private:
    std::string _path;
    TStorageMedium::type _storage_medium;
    int64_t _available_bytes;
    int64_t _disk_capacity_bytes;
};

} // namespace starrocks::lake
