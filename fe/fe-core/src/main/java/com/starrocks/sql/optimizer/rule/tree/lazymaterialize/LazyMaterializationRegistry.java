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

package com.starrocks.sql.optimizer.rule.tree.lazymaterialize;

import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;

import java.util.Map;

public class LazyMaterializationRegistry {
    private static final Map<Class<?>, LazyMaterializationSupport> HANDLERS = Maps.newHashMap();

    static {
        HANDLERS.put(PhysicalIcebergScanOperator.class, new IcebergV3LazyMaterializationSupport());
    }

    static LazyMaterializationSupport getHandler(PhysicalScanOperator scanOperator) {
        return HANDLERS.getOrDefault(scanOperator.getClass(), new NotSupportedLazyMaterialization());
    }
}
