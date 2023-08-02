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

#include "compute_service.h"

namespace starrocks {

ComputeService::ComputeService(ExecEnv* exec_env) : BackendServiceBase(exec_env) {}

ComputeService::~ComputeService() = default;

<<<<<<< HEAD:be/src/service/service_cn/compute_service.cpp
} // namespace starrocks
=======
    public static final String FORCE_STREAMING = "force_streaming";

    public static final String FORCE_PREAGGREGATION = "force_preaggregation";

    public static final String LIMITED = "limited";
}
>>>>>>> 61d47e4a9c ([Enhancement] support limited memory stream aggregate (#28402)):fe/fe-core/src/main/java/com/starrocks/qe/SessionVariableConstants.java
