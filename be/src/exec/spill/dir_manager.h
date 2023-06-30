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

#include <sys/statfs.h>

#include <atomic>
#include <memory>

#include "common/status.h"
#include "common/statusor.h"
#include "fs/fs.h"

namespace starrocks::spill {

// Dir describes a specific directory, including the directory name and the corresponding FileSystem
// @TODO(silverbullet233): maintain some stats, such as the capacity
class Dir {
public:
    Dir(std::string dir, std::shared_ptr<FileSystem> fs, int64_t max_dir_size)
            : _dir(std::move(dir)), _fs(fs), _max_size(max_dir_size) {}

    FileSystem* fs() const { return _fs.get(); }
    std::string dir() const { return _dir; }

    int64_t get_current_size() const { return _current_size.load(); }

    void set_current_size(int64_t value) { _current_size.store(value); }

    int64_t get_max_size() const { return _max_size; }

private:
    std::string _dir;
    std::shared_ptr<FileSystem> _fs;
    int64_t _max_size;
    std::atomic<int64_t> _current_size;
};
using DirPtr = std::shared_ptr<Dir>;

struct AcquireDirOptions {
    // @TOOD(silverbullet233): support more properties when acquiring dir, such as the preference of dir selection
};

// DirManager is used to manage all spill-available directories,
// BlockManager should rely on DirManager to decide which directory to put Block in.
// DirManager is thread-safe.
class DirManager {
public:
    DirManager() = default;
    ~DirManager() = default;

    Status init();

    StatusOr<Dir*> acquire_writable_dir(const AcquireDirOptions& opts);

private:
    bool is_same_disk(const std::string& path1, const std::string& path2) {
        struct statfs stat1, stat2;
        statfs(path1.c_str(), &stat1);
        statfs(path2.c_str(), &stat2);
        return stat1.f_fsid.__val[0] == stat2.f_fsid.__val[0] && stat1.f_fsid.__val[1] == stat2.f_fsid.__val[1];
    }

    std::atomic<size_t> _idx = 0;
    std::vector<DirPtr> _dirs;
};

} // namespace starrocks::spill