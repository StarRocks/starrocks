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

#include <vector>

#include "common/status.h"
#include "gen_cpp/StarrocksExternalService_types.h"
#include "gen_cpp/Types_types.h"

namespace starrocks {

class ExecEnv;

namespace query_orchestration {

class QueryOrchestrator {
public:
    explicit QueryOrchestrator(ExecEnv* exec_env);

    Status exec_external_plan_fragment(const TScanOpenParams& params, const TUniqueId& fragment_instance_id,
                                       std::vector<TScanColumnDesc>* selected_columns, TUniqueId* query_id);

private:
    ExecEnv* _exec_env;
};

} // namespace query_orchestration
} // namespace starrocks
