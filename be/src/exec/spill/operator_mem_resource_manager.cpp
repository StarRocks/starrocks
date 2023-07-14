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

#include "exec/pipeline/operator.h"

namespace starrocks::spill {
void OperatorMemoryResourceManager::prepare(OP* op, QuerySpillManager* query_spill_manager) {
    _op = op;
    _spillable = op->spillable();
    _releaseable = op->releaseable();
    _releaseable |= _spillable;
    _query_spill_manager = query_spill_manager;
    if (_spillable) {
        _query_spill_manager->increase_spillable_operators();
    }
}

void OperatorMemoryResourceManager::to_low_memory_mode() {
    if (_performance_level < MEM_RESOURCE_LOW_MEMORY) {
        _performance_level = MEM_RESOURCE_LOW_MEMORY;
        _op->set_execute_mode(_performance_level);
        if (_spillable) {
            _query_spill_manager->increase_spilling_operators();
        }
        if (_op->releaseable()) {
            set_releasing();
        }
    }
}

void OperatorMemoryResourceManager::close() {
    if (_performance_level == MEM_RESOURCE_LOW_MEMORY && _query_spill_manager != nullptr) {
        _query_spill_manager->decrease_spilling_operators();
    }
}

} // namespace starrocks::spill