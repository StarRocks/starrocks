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

#include <memory>

#include "common/status.h"
#include "common/statusor.h"
#include "fs/fs.h"

namespace starrocks {
namespace spill {

class Dir {
public:
    Dir(const std::string& dir, std::shared_ptr<FileSystem> fs) : _dir(dir), _fs(fs) {}

    FileSystem* fs() { return _fs.get(); }
    std::string dir() { return _dir; }

private:
    std::string _dir;
    std::shared_ptr<FileSystem> _fs;
    // @TODO maintain stats, such as capacity
};
using DirPtr = std::shared_ptr<Dir>;

struct AcquireDirOptions {
    // @TBD
};

class DirManager {
public:
    DirManager() = default;
    ~DirManager() = default;

    Status init();

    StatusOr<Dir*> acquire_writable_dir(const AcquireDirOptions& opts);

private:
    std::vector<DirPtr> _dirs;
};

} // namespace spill
} // namespace starrocks