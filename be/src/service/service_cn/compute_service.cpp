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

<<<<<<< HEAD:be/src/service/service_cn/compute_service.cpp
namespace starrocks {

ComputeService::ComputeService(ExecEnv* exec_env) : BackendServiceBase(exec_env) {}

ComputeService::~ComputeService() = default;

} // namespace starrocks
=======
public enum HiveAccessType {
    NONE, CREATE, ALTER, DROP, INDEX, LOCK, SELECT, UPDATE, USE, READ, WRITE, ALL, SERVICEADMIN, TEMPUDFADMIN
}
>>>>>>> fc74a4dd60 ([Enhancement] Fix the checkstyle of semicolons (#33130)):fe/fe-core/src/main/java/com/starrocks/privilege/ranger/hive/HiveAccessType.java
