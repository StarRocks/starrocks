// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.base;

import com.clearspring.analytics.util.Lists;
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
            if (!ec.columnToEquivalenceClass.containsKey(entry.getKey())) {
                Set<ColumnRefOperator> columnEcs = Sets.newLinkedHashSet(entry.getValue());
                for (ColumnRefOperator column : columnEcs) {
                    ec.columnToEquivalenceClass.put(column, columnEcs);
                }
            }
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
