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

#include "common/config.h"
#include "exec/pipeline/operator.h"

namespace starrocks::spill {
void OperatorMemoryResourceManager::prepare(OP* op, QuerySpillManager* query_spill_manager) {
    _op = op;
    _spillable = op->spillable();
    _releaseable = op->releaseable();
    _releaseable |= _spillable;
    _query_spill_manager = query_spill_manager;
    if (_spillable) {
        DCHECK(_query_spill_manager != nullptr);
        _res_guard.inc_spillable_operators();
    }
    if (_query_spill_manager != nullptr) {
        size_t reserved_bytes = 0;
        if (_spillable) {
            reserved_bytes = operator_avaliable_memory_bytes();
        } else if (op->releaseable()) {
            reserved_bytes = config::local_exchange_buffer_mem_limit_per_driver;
        }
        _res_guard.inc_reserve_bytes(reserved_bytes);
    }
}

void OperatorMemoryResourceManager::to_low_memory_mode() {
    if (_performance_level < MEM_RESOURCE_LOW_MEMORY) {
        _performance_level = MEM_RESOURCE_LOW_MEMORY;
        _op->set_execute_mode(_performance_level);
    }
}

size_t OperatorMemoryResourceManager::operator_avaliable_memory_bytes() {
    // TODO: think about multi-operators
    auto* runtime_state = _op->runtime_state();
    size_t avaliable = runtime_state->spill_mem_table_size() * runtime_state->spill_mem_table_num();
    avaliable = std::max<size_t>(avaliable, runtime_state->spill_operator_min_bytes());
    avaliable = std::min<size_t>(avaliable, runtime_state->spill_operator_max_bytes());
    return avaliable;
}

void OperatorMemoryResourceManager::close() {
    _res_guard.reset();
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