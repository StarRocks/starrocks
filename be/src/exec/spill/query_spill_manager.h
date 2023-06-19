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
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "exec/spill/log_block_manager.h"
#include "fs/fs.h"
#include "gen_cpp/Types_types.h"

namespace starrocks::spill {
class QuerySpillManager {
public:
    QuerySpillManager(const TUniqueId& uid) : _uid(uid) { _block_manager = std::make_unique<LogBlockManager>(uid); }

    void increase_spilling_operators() { _spilling_operators++; }
    void decrease_spilling_operators() { _spilling_operators--; }
    size_t spilling_operators() { return _spilling_operators; }

    void increase_spillable_operators() { _spillable_operators++; }
    void decrease_spillable_operators() { _spillable_operators--; }
    size_t spillable_operators() { return _spillable_operators; }

    BlockManager* block_manager() const { return _block_manager.get(); }

private:
    TUniqueId _uid;
    std::unique_ptr<BlockManager> _block_manager;
    std::atomic_size_t _spilling_operators = 0;
    size_t _spillable_operators = 0;
};
} // namespace starrocks::spill
