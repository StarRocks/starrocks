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

package com.starrocks.sql.optimizer.base;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class EquivalenceClasses implements Cloneable {
    private final Map<ColumnRefOperator, Set<ColumnRefOperator>> columnToEquivalenceClass;

    private List<Set<ColumnRefOperator>> cacheColumnToEquivalenceClass;

    public EquivalenceClasses() {
        columnToEquivalenceClass = Maps.newHashMap();
    }

    @Override
    public EquivalenceClasses clone() {
        final EquivalenceClasses ec = new EquivalenceClasses();
        for (Map.Entry<ColumnRefOperator, Set<ColumnRefOperator>> entry :
                this.columnToEquivalenceClass.entrySet()) {
            ec.columnToEquivalenceClass.put(entry.getKey(), Sets.newLinkedHashSet(entry.getValue()));
        }
        ec.cacheColumnToEquivalenceClass = null;
        return ec;
    }

    public void addEquivalence(ColumnRefOperator left, ColumnRefOperator right) {
        cacheColumnToEquivalenceClass = null;

        Set<ColumnRefOperator> s1 = columnToEquivalenceClass.get(left);
        Set<ColumnRefOperator> s2 = columnToEquivalenceClass.get(right);

        if (s1 != null && s2 != null) {
            if (s1.size() < s2.size()) {
                Set<ColumnRefOperator> tmpSet = s2;
                s2 = s1;
                s1 = tmpSet;
            }
            for (ColumnRefOperator columnRefOperator : s2) {
                s1.add(columnRefOperator);
                columnToEquivalenceClass.put(columnRefOperator, s1);
            }
        } else if (s1 != null) {
            s1.add(right);
            columnToEquivalenceClass.put(right, s1);
        } else if (s2 != null) {
            s2.add(left);
            columnToEquivalenceClass.put(left, s2);
        } else {
            Set<ColumnRefOperator> ec = Sets.newLinkedHashSet();
            ec.add(left);
            ec.add(right);
            columnToEquivalenceClass.put(left, ec);
            columnToEquivalenceClass.put(right, ec);
        }
    }

    public Set<ColumnRefOperator> getEquivalenceClass(ColumnRefOperator column) {
        return columnToEquivalenceClass.get(column);
    }

    public List<Set<ColumnRefOperator>> getEquivalenceClasses() {
        // Remove redundant equal classes, eg:
        // a,b are equal calsses:
        // cacheColumnToEquivalenceClass:
        // a -> set(a, b)
        // b -> set(a, b)
        // cacheColumnToEquivalenceClass will only return:
        // set(a, b)
        if (cacheColumnToEquivalenceClass == null) {
            cacheColumnToEquivalenceClass = Lists.newArrayList();
            Set<ColumnRefOperator> visited = Sets.newHashSet();
            for (Set<ColumnRefOperator> columnRefOperators : columnToEquivalenceClass.values()) {
                if (!visited.containsAll(columnRefOperators)) {
                    visited.addAll(columnRefOperators);
                    cacheColumnToEquivalenceClass.add(columnRefOperators);
                }
            }
        }
        return cacheColumnToEquivalenceClass;
    }
}
