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

package com.starrocks.sql.optimizer.rule.tree.lowcardinality;

import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class StructManager {
    // Maintains a mapping from struct ColumnRefs to field ColumnRefs to use for encoded fields.
    // Currently only fields which correspond to a ColumnRef can be encoded. All field ColumnRefOperators should have
    // STRING or ARRAY<STRING> types. Nested structs are not supported.
    private Map<Integer, Map<String, ColumnRefOperator>> structRefToFieldStringRefMap = Maps.newHashMap();

    // Maintains a mapping from struct operators to field ColumnRefs to use for encoded fields.
    // Currently only fields which correspond to a ColumnRef can be encoded. All field ColumnRefOperators should have
    // STRING or ARRAY<STRING> types. Nested structs are not supported.
    private Map<ScalarOperator, Map<String, ColumnRefOperator>> structOpToFieldStringRefMap =
            Maps.newIdentityHashMap();

    private final boolean enableStructLowCardinalityOptimize;

    public StructManager(boolean enableStructLowCardinalityOptimize) {
        this.enableStructLowCardinalityOptimize = enableStructLowCardinalityOptimize;
    }

    public boolean isEnableStructLowCardinalityOptimize() {
        return enableStructLowCardinalityOptimize;
    }

    public Map<String, ColumnRefOperator> getFieldStringRefMap(ScalarOperator operator) {
        if (operator.isColumnRef()) {
            ColumnRefOperator c = operator.cast();
            return structRefToFieldStringRefMap.get(c.getId());
        }
        return structOpToFieldStringRefMap.get(operator);
    }

    public Map<String, ColumnRefOperator> getFieldStringRefMap(Integer c) {
        return structRefToFieldStringRefMap.get(c);
    }

    boolean contains(Integer cid) {
        return structRefToFieldStringRefMap.containsKey(cid);
    }

    boolean contains(ScalarOperator op) {
        return structOpToFieldStringRefMap.containsKey(op) ||
                op.isColumnRef() && structRefToFieldStringRefMap.containsKey(((ColumnRefOperator) op).getId());
    }

    public void setFieldMapping(CallOperator call, Map<String, ColumnRefOperator> fieldMap) {
        structOpToFieldStringRefMap.put(call, fieldMap);
    }

    public void addIfStruct(ColumnRefOperator col, ScalarOperator define) {
        Map<String, ColumnRefOperator> fieldsMap = getFieldStringRefMap(define);
        if (fieldsMap != null) {
            structRefToFieldStringRefMap.putIfAbsent(col.getId(), fieldsMap);
        }
    }

    public void finalize(Set<Integer> allStringColumns) {
        structOpToFieldStringRefMap = structOpToFieldStringRefMap.entrySet().stream()
                .map(e -> Map.entry(
                        e.getKey(),
                        e.getValue().entrySet().stream()
                                .filter(e2 -> allStringColumns.contains(e2.getValue().getId()))
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                ))
                .filter(entry -> !entry.getValue().isEmpty())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (oldVal, newVal) -> oldVal,
                        IdentityHashMap::new));

        structRefToFieldStringRefMap = structRefToFieldStringRefMap.entrySet().stream()
                .map(e -> Map.entry(
                        e.getKey(),
                        e.getValue().entrySet().stream()
                                .filter(e2 -> allStringColumns.contains(e2.getValue().getId()))
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                ))
                .filter(entry -> !entry.getValue().isEmpty())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
