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

#include "exec/spill/global_spill_manager.h"
#include "exec/spill/log_block_manager.h"
#include "gen_cpp/Types_types.h"

namespace starrocks {
class TQueryOptions;
}

namespace starrocks::spill {
class QuerySpillManager {
public:
    QuerySpillManager(const TUniqueId& uid, GlobalSpillManager* global_spill_manager, DirManager* spill_dir_mgr)
            : _uid(uid), _global_spill_manager(global_spill_manager), _spill_dir_mgr(spill_dir_mgr) {}

    Status init_block_manager(const TQueryOptions& query_options);

    void increase_spillable_operators() { _global_spill_manager->increase_spillable_operators(); }
    void decrease_spillable_operators() { _global_spill_manager->decrease_spillable_operators(); }

    void inc_reserve_bytes(size_t bytes) { _global_spill_manager->inc_reserve_bytes(bytes); }
    void dec_reserve_bytes(size_t bytes) { _global_spill_manager->dec_reserve_bytes(bytes); }

    BlockManager* block_manager() const { return _block_manager.get(); }

private:
    Status init_local_block_manager();

    TUniqueId _uid;
    std::unique_ptr<BlockManager> _block_manager;
    std::unique_ptr<DirManager> _remote_dir_manager;
    GlobalSpillManager* _global_spill_manager = nullptr;
    DirManager* _spill_dir_mgr = nullptr;
};
} // namespace starrocks::spill
