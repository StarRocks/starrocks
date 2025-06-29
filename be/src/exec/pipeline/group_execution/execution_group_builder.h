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

#include <cstddef>
#include <unordered_map>

#include "exec/pipeline/group_execution/execution_group_fwd.h"

namespace starrocks {
class TGroupExecutionParam;

namespace pipeline {
enum class ExecutionGroupType {
    NORMAL,
    COLOCATE,
};

class ExecutionGroupBuilder {
public:
    ExecutionGroupBuilder() = default;
    ~ExecutionGroupBuilder() = default;

    static ExecutionGroupPtr create_normal_exec_group();
    static std::unordered_map<int32_t, ExecutionGroupPtr> create_colocate_exec_groups(const TGroupExecutionParam& param,
                                                                                      size_t physical_dop);
};
} // namespace pipeline
} // namespace starrocks
