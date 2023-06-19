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

#include "exec/pipeline/pipeline_fwd.h"
#include "exec/spill/query_spill_manager.h"

namespace starrocks::spill {
enum MEM_RESOURCE {
    MEM_RESOURCE_DEFAULE_MEMORY = 0,
    MEM_RESOURCE_MID_MEMORY = 1,
    MEM_RESOURCE_LOW_MEMORY = 2,
};

class OperatorMemoryResourceManager {
public:
    using OP = pipeline::Operator;

    void prepare(OP* op, QuerySpillManager* query_spill_manager);

    void close();

    bool releaseable() const { return _releaseable && _performance_level <= MEM_RESOURCE_LOW_MEMORY; }

    bool spillable() const { return _spillable; }

    void to_low_memory_mode();

    // For the current operator available memory (estimated value)
    size_t operator_avaliable_memory_bytes();

private:
    // performance level. Determine the execution mode and whether memory can be freed early
    // A higher performance level will allow the operator to execute with less memory, which will reduce performance
    int _performance_level = MEM_RESOURCE_DEFAULE_MEMORY;
    bool _spillable = false;
    bool _releaseable = false;
    OP* _op = nullptr;
    QuerySpillManager* _query_spill_manager = nullptr;
};
} // namespace starrocks::spill