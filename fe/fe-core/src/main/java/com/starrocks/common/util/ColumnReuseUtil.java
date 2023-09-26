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
package com.starrocks.common.util;

import com.google.api.client.util.Lists;
import com.google.common.collect.Sets;
import com.starrocks.sql.optimizer.operator.scalar.CloneOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ColumnReuseUtil {

    /**
     * Currently, the backend has not supported COW mechanism, referring to the same Column may lead to crash.
     * So we need to hack the projectMap to avoid this situation. For every ColumnRefColumn that occurs more than
     * once, we need to wrap it with a CloneOperator starting from the second occurrence.
     */
    public static void hackReusedColumnRefOperator(Map<ColumnRefOperator, ScalarOperator> projectMap) {
        if (projectMap == null) {
            return;
        }
        List<Map.Entry<ColumnRefOperator, ScalarOperator>> entries = projectMap.entrySet().stream()
                .filter(entry -> entry.getValue().isColumnRef())
                .collect(Collectors.toList());
        Set<ColumnRefOperator> valueColumnRefs = Sets.newHashSet();
        List<ColumnRefOperator> reinsertKeyColumnRefs = Lists.newArrayList();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : entries) {
            ColumnRefOperator valueColumnRef = entry.getValue().cast();
            if (valueColumnRefs.contains(valueColumnRef)) {
                reinsertKeyColumnRefs.add(entry.getKey());
            } else {
                valueColumnRefs.add(valueColumnRef);
            }
        }

        for (ColumnRefOperator key : reinsertKeyColumnRefs) {
            ColumnRefOperator value = projectMap.remove(key).cast();
            projectMap.put(key, new CloneOperator(value));
        }
    }
}
