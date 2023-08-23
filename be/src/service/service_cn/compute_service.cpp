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

<<<<<<< HEAD:be/src/service/service_cn/compute_service.cpp
ComputeService::ComputeService(ExecEnv* exec_env) : BackendServiceBase(exec_env) {}
=======
import com.google.common.collect.ImmutableList;

import java.util.List;
>>>>>>> 83dbcebb84 ([Enhancement] Compatible with the default partition value of the new hudi version (#29606)):fe/fe-core/src/main/java/com/starrocks/catalog/JDBCPartitionKey.java

ComputeService::~ComputeService() = default;

<<<<<<< HEAD:be/src/service/service_cn/compute_service.cpp
} // namespace starrocks
=======
    @Override
    public List<String> nullPartitionValueList() {
        return ImmutableList.of(JDBCTable.PARTITION_NULL_VALUE);
    }
}
>>>>>>> 83dbcebb84 ([Enhancement] Compatible with the default partition value of the new hudi version (#29606)):fe/fe-core/src/main/java/com/starrocks/catalog/JDBCPartitionKey.java
