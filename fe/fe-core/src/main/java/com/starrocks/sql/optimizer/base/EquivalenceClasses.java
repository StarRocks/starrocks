// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.base;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class EquivalenceClasses {
    private final Map<ColumnRefOperator, Set<ColumnRefOperator>> columnToEquivalenceClass;

    public EquivalenceClasses() {
        columnToEquivalenceClass = Maps.newHashMap();
    }

    public void addEquivalence(ColumnRefOperator left, ColumnRefOperator right) {
        Set<ColumnRefOperator> s1 = columnToEquivalenceClass.get(left);
        Set<ColumnRefOperator> s2 = columnToEquivalenceClass.get(right);

        if (s1 != null && s2 != null) {
            if (s1.size() > s2.size()) {
                Set<ColumnRefOperator> tmp = s1;
                s1 = s2;
                s2 = tmp;
            }
            for (ColumnRefOperator columnRefOperator : s1) {
                s2.add(columnRefOperator);
                columnToEquivalenceClass.put(columnRefOperator, s2);
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
        return columnToEquivalenceClass.values().stream().collect(Collectors.toList());
    }
}
