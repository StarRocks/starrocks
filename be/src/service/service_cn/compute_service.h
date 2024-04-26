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

#include "service/backend_base.h"

namespace starrocks {

// This class just forward rpc requests to actual handlers, used
// to bind multiple services on single port.
class ComputeService : public BackendServiceBase {
public:
    explicit ComputeService(ExecEnv* exec_env);

<<<<<<< HEAD:be/src/service/service_cn/compute_service.h
    ~ComputeService() override;
=======
    static void append_int_conjunct(TExprOpcode::type opcode, SlotId slot_id, int value, std::vector<TExpr>* tExprs);
    static void append_string_conjunct(TExprOpcode::type opcode, SlotId slot_id, std::string value,
                                       std::vector<TExpr>* tExprs);
>>>>>>> 7fe278ca54 ([BugFix] Fix parquet footer not have min max statistics caused inaccurate query results (#44489)):be/test/formats/parquet/parquet_ut_base.h
};

} // namespace starrocks