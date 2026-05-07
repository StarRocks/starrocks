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

#include "exec/spill/operator_mem_resource_manager.h"

#include <algorithm>

#include "runtime/runtime_state.h"

namespace starrocks::spill {
void OperatorMemoryResourceManager::prepare(QuerySpillManager* query_spill_manager, bool spillable, bool releaseable,
                                            size_t reserved_bytes) {
    _performance_level = MEM_RESOURCE_DEFAULE_MEMORY;
    _spillable = spillable;
    _releaseable = releaseable || spillable;
    _query_spill_manager = query_spill_manager;
    if (_spillable) {
        DCHECK(_query_spill_manager != nullptr);
        _res_guard.inc_spillable_operators();
    }
    if (_query_spill_manager != nullptr) {
        _res_guard.inc_reserve_bytes(reserved_bytes);
    }
}

bool OperatorMemoryResourceManager::enter_low_memory_mode() {
    if (_performance_level < MEM_RESOURCE_LOW_MEMORY) {
        _performance_level = MEM_RESOURCE_LOW_MEMORY;
        return true;
    }
    return false;
}

size_t OperatorMemoryResourceManager::compute_available_memory_bytes(const RuntimeState& runtime_state) {
    // TODO: think about multi-operators
    size_t available = runtime_state.spill_mem_table_size() * runtime_state.spill_mem_table_num();
    available = std::max<size_t>(available, runtime_state.spill_operator_min_bytes());
    available = std::min<size_t>(available, runtime_state.spill_operator_max_bytes());
    return available;
}

void OperatorMemoryResourceManager::close() {
    _res_guard.reset();
    _performance_level = MEM_RESOURCE_DEFAULE_MEMORY;
    _spillable = false;
    _releaseable = false;
    _query_spill_manager = nullptr;
}

void OperatorMemoryResourceManager::ResGuard::reset() noexcept {
    auto* query_spill_manager = _manager.query_spill_manager();
    if (query_spill_manager == nullptr) {
        DCHECK(_reserved_bytes == 0);
        DCHECK(_spill_operators == 0);
        return;
    }

    query_spill_manager->dec_reserve_bytes(_reserved_bytes);

    if (_spill_operators > 0) {
        query_spill_manager->decrease_spillable_operators();
    }

    _reserved_bytes = 0;
    _spill_operators = 0;
}

void OperatorMemoryResourceManager::ResGuard::inc_reserve_bytes(size_t bytes) {
    auto* query_spill_manager = _manager.query_spill_manager();
    query_spill_manager->inc_reserve_bytes(bytes);
    _reserved_bytes += bytes;
}

void OperatorMemoryResourceManager::ResGuard::inc_spillable_operators() {
    auto* query_spill_manager = _manager.query_spill_manager();
    DCHECK_EQ(_spill_operators, 0);
    query_spill_manager->increase_spillable_operators();
    _spill_operators = 1;
}

} // namespace starrocks::spill
