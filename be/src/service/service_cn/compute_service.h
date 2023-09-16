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

    ~ComputeService() override;
};

<<<<<<< HEAD:be/src/service/service_cn/compute_service.h
} // namespace starrocks
=======
    public static final String FORCE_PREAGGREGATION = "force_preaggregation";

    public static final String LIMITED = "limited";

    public static final String PANIC = "panic";

    public static final String DOUBLE = "double";

    public static final String DECIMAL = "decimal";
}
>>>>>>> ab38cf5f79 ([Feature] Substitute large decimal types with double/decimal (#31171)):fe/fe-core/src/main/java/com/starrocks/qe/SessionVariableConstants.java
