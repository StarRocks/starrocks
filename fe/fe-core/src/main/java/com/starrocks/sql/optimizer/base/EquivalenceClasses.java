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

    public EquivalenceClasses(EquivalenceClasses other) {
        columnToEquivalenceClass = Maps.newHashMap();
        columnToEquivalenceClass.putAll(other.columnToEquivalenceClass);
    }

    public void addEquivalence(ColumnRefOperator left, ColumnRefOperator right) {
        Set<ColumnRefOperator> s1 = columnToEquivalenceClass.get(left);
        Set<ColumnRefOperator> s2 = columnToEquivalenceClass.get(right);

        if (s1 != null && s2 != null) {
            Set<ColumnRefOperator> shortSet = s1.size() < s2.size() ? s1 : s2;
            Set<ColumnRefOperator> longSet = s1.size() < s2.size() ? s2 : s1;
            longSet.addAll(shortSet);
            for (ColumnRefOperator columnRefOperator : shortSet) {
                columnToEquivalenceClass.put(columnRefOperator, longSet);
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
