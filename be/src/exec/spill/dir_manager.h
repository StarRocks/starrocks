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
    Dir(std::string dir, std::shared_ptr<FileSystem> fs) : _dir(std::move(dir)), _fs(fs) {}

    FileSystem* fs() const { return _fs.get(); }
    std::string dir() const { return _dir; }

private:
    std::string _dir;
    std::shared_ptr<FileSystem> _fs;
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

    Status init(const std::string& spill_dirs);

    StatusOr<Dir*> acquire_writable_dir(const AcquireDirOptions& opts);

private:
    std::atomic<size_t> _idx = 0;
    std::vector<DirPtr> _dirs;
};

} // namespace starrocks::spill