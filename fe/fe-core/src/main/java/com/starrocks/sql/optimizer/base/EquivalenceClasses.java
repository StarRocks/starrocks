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

    private final Map<ColumnRefOperator, Set<ColumnRefOperator>> redundantColumnToEquivalenceClass;

    public EquivalenceClasses() {
        columnToEquivalenceClass = Maps.newHashMap();
        redundantColumnToEquivalenceClass = Maps.newHashMap();
    }

    @Override
    public EquivalenceClasses clone() {
        final EquivalenceClasses ec = new EquivalenceClasses();
        for (Map.Entry<ColumnRefOperator, Set<ColumnRefOperator>> entry :
                this.columnToEquivalenceClass.entrySet()) {
            if (!ec.columnToEquivalenceClass.containsKey(entry.getKey())) {
                Set<ColumnRefOperator> columnEcs = Sets.newLinkedHashSet(entry.getValue());
                for (ColumnRefOperator column : columnEcs) {
                    ec.columnToEquivalenceClass.put(column, columnEcs);
                }
            }
        }
        for (Map.Entry<ColumnRefOperator, Set<ColumnRefOperator>> entry :
                this.redundantColumnToEquivalenceClass.entrySet()) {
            if (!ec.redundantColumnToEquivalenceClass.containsKey(entry.getKey())) {
                Set<ColumnRefOperator> columnEcs = Sets.newLinkedHashSet(entry.getValue());
                for (ColumnRefOperator column : columnEcs) {
                    ec.redundantColumnToEquivalenceClass.put(column, columnEcs);
                }
            }
        }
        ec.cacheColumnToEquivalenceClass = null;
        return ec;
    }

    public void addEquivalence(ColumnRefOperator left, ColumnRefOperator right) {
        cacheColumnToEquivalenceClass = null;
        addElementsIntoEquivalenceClass(columnToEquivalenceClass, left, right);
    }

    private void addElementsIntoEquivalenceClass(Map<ColumnRefOperator, Set<ColumnRefOperator>> equivalenceClass,
                                                 ColumnRefOperator left, ColumnRefOperator right) {
        Set<ColumnRefOperator> s1 = equivalenceClass.get(left);
        Set<ColumnRefOperator> s2 = equivalenceClass.get(right);

        if (s1 != null && s2 != null) {
            if (s1.size() < s2.size()) {
                Set<ColumnRefOperator> tmpSet = s2;
                s2 = s1;
                s1 = tmpSet;
            }
            for (ColumnRefOperator columnRefOperator : s2) {
                s1.add(columnRefOperator);
                equivalenceClass.put(columnRefOperator, s1);
            }
        } else if (s1 != null) {
            s1.add(right);
            equivalenceClass.put(right, s1);
        } else if (s2 != null) {
            s2.add(left);
            equivalenceClass.put(left, s2);
        } else {
            Set<ColumnRefOperator> ec = Sets.newLinkedHashSet();
            ec.add(left);
            ec.add(right);
            equivalenceClass.put(left, ec);
            equivalenceClass.put(right, ec);
        }
    }

    public Set<ColumnRefOperator> getEquivalenceClass(ColumnRefOperator column) {
        return columnToEquivalenceClass.get(column);
    }

    public boolean containsKey(ColumnRefOperator column) {
        return columnToEquivalenceClass.containsKey(column);
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

    public void addRedundantEquivalence(ColumnRefOperator left, ColumnRefOperator right) {
        addElementsIntoEquivalenceClass(redundantColumnToEquivalenceClass, left, right);
    }

    public boolean containsRedundantKey(ColumnRefOperator column) {
        return redundantColumnToEquivalenceClass.containsKey(column);
    }

    public void deleteRedundantKey(ColumnRefOperator column) {
        if (!redundantColumnToEquivalenceClass.containsKey(column)) {
            return;
        }
        Set<ColumnRefOperator> redundantKeys = redundantColumnToEquivalenceClass.get(column);

        // 1. remove redundant keys
        Set<ColumnRefOperator> oldValues = null;
        for (ColumnRefOperator redundantKey : redundantKeys) {
            if (columnToEquivalenceClass.containsKey(redundantKey)) {
                oldValues = Sets.newHashSet(columnToEquivalenceClass.get(redundantKey));
                columnToEquivalenceClass.remove(redundantKey);
            }
        }

        // 2. remove redundant columns as values
        if (oldValues != null) {
            for (ColumnRefOperator col : oldValues) {
                if (columnToEquivalenceClass.containsKey(col)) {
                    columnToEquivalenceClass.get(col).removeAll(redundantKeys);
                }
            }
        }
        this.cacheColumnToEquivalenceClass = null;
    }
}
