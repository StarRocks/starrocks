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


package com.starrocks.sql.optimizer.operator.physical;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.ColumnOutputInfo;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class PhysicalDecodeOperator extends PhysicalOperator {
    private final ImmutableMap<ColumnRefOperator, ColumnRefOperator> dictToStrings;
    private final Map<ColumnRefOperator, ScalarOperator> stringFunctions;

    public PhysicalDecodeOperator(ImmutableMap<ColumnRefOperator, ColumnRefOperator> dictToStrings,
                                  Map<ColumnRefOperator, ScalarOperator> stringFunctions) {
        super(OperatorType.PHYSICAL_DECODE);
        this.dictToStrings = dictToStrings;
        this.stringFunctions = stringFunctions;
        removeUnusedFunction();
    }

    // remove useless string functions
    // Some functions end up not being used, or some functions generated in intermediate,
    // and we need to remove them
    private void removeUnusedFunction() {
        Set<Integer> dictIds = this.dictToStrings.keySet().stream()
                .map(ColumnRefOperator::getId).collect(Collectors.toSet());
        Map<Integer, ScalarOperator> dictToFunctions = Maps.newHashMap();
        // build dictionary-id to string-function mapping
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : stringFunctions.entrySet()) {
            dictToFunctions.put(entry.getKey().getId(), entry.getValue());
        }

        // get all used dictionary-id
        Set<Integer> usedDictIds = Sets.newHashSet();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : stringFunctions.entrySet()) {
            if (dictIds.contains(entry.getKey().getId())) {
                usedDictIds.add(entry.getKey().getId());
            }
        }

        // get all used string functions
        // The input column of a string function may be other string function result
        // we need to add all dependent columns to the usedDictIds
        //
        // eg:
        // dict list: [3]
        // dict-id 2 <- string function 1:upper(1)
        // dict-id 3 <- string function 2:upper(2)
        // we should keep func2 and func1
        //
        Set<Integer> currentUsed = Sets.newHashSet(usedDictIds);
        Set<Integer> next = Sets.newHashSet();
        while (!currentUsed.isEmpty()) {
            next.clear();
            for (Integer usedDictId : currentUsed) {
                ScalarOperator scalarOperator = dictToFunctions.get(usedDictId);
                ColumnRefSet usedColumns = scalarOperator.getUsedColumns();
                for (int columnId : usedColumns.getColumnIds()) {
                    if (!usedDictIds.contains(columnId)) {
                        usedDictIds.add(columnId);
                        next.add(usedDictId);
                    }
                }
            }
            currentUsed.clear();
            Set<Integer> tmp = currentUsed;
            currentUsed = next;
            next = tmp;
        }

        Set<ColumnRefOperator> unusedStringFunctions = Sets.newHashSet();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : stringFunctions.entrySet()) {
            if (!usedDictIds.contains(entry.getKey().getId())) {
                unusedStringFunctions.add(entry.getKey());
            }
        }

        for (ColumnRefOperator unusedStringFunction : unusedStringFunctions) {
            stringFunctions.remove(unusedStringFunction);
        }

    }

    public ImmutableMap<Integer, Integer> getDictToStrings() {
        Map<Integer, Integer> res = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, ColumnRefOperator> entry : dictToStrings.entrySet()) {
            res.put(entry.getKey().getId(), entry.getValue().getId());
        }
        return ImmutableMap.copyOf(res);
    }

    public Map<ColumnRefOperator, ScalarOperator> getStringFunctions() {
        return stringFunctions;
    }

    @Override
    public RowOutputInfo deriveRowOutputInfo(List<OptExpression> inputs) {
        RowOutputInfo childRow = inputs.get(0).getRowOutputInfo();
        List<ColumnOutputInfo> cols = Lists.newArrayList();
        for (ColumnOutputInfo col : childRow.getColumnOutputInfo()) {
            if (dictToStrings.containsKey(col.getColumnRef())) {
                cols.add(new ColumnOutputInfo(dictToStrings.get(col.getColumnRef()), col.getColumnRef()));
            } else {
                cols.add(new ColumnOutputInfo(col.getColumnRef(), col.getColumnRef()));
            }
        }
        return new RowOutputInfo(cols, dictToStrings.values());
    }

    @Override
    public boolean equals(Object o) {
        return this == o;
    }

    @Override
    public int hashCode() {
        return dictToStrings.hashCode();
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalDecode(optExpression, context);
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        ColumnRefSet columnRefSet = new ColumnRefSet();
        columnRefSet.union(dictToStrings.keySet());
        for (ScalarOperator scalarOperator : stringFunctions.values()) {
            columnRefSet.union(scalarOperator.getUsedColumns());
        }
        return columnRefSet;
    }
}