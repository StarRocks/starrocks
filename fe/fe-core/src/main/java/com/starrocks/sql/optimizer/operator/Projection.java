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

package com.starrocks.sql.optimizer.operator;

import com.google.common.base.Preconditions;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.tree.AddDecodeNodeForDictStringRule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Projection {
    private final Map<ColumnRefOperator, ScalarOperator> columnRefMap;
    // Used for common operator compute result reuse, we need to compute
    // common sub operators firstly in BE
    private final Map<ColumnRefOperator, ScalarOperator> commonSubOperatorMap;

    public void setNeedReuseLambdaDependentExpr(boolean needReuseLambdaExpr) {
        this.needReuseLambdaDependentExpr = needReuseLambdaExpr;
    }

    public boolean needReuseLambdaDependentExpr() {
        return needReuseLambdaDependentExpr;
    }

    private boolean needReuseLambdaDependentExpr;

    public Projection(Map<ColumnRefOperator, ScalarOperator> columnRefMap) {
        this.columnRefMap = columnRefMap;
        this.commonSubOperatorMap = new HashMap<>();
    }

    public Projection(Map<ColumnRefOperator, ScalarOperator> columnRefMap,
                      Map<ColumnRefOperator, ScalarOperator> commonSubOperatorMap) {
        this.columnRefMap = columnRefMap;
        if (commonSubOperatorMap == null) {
            this.commonSubOperatorMap = new HashMap<>();
        } else {
            this.commonSubOperatorMap = commonSubOperatorMap;
        }
    }

    public Projection(Map<ColumnRefOperator, ScalarOperator> columnRefMap,
                      Map<ColumnRefOperator, ScalarOperator> commonSubOperatorMap, boolean needReuseLambdaDependentExpr) {
        this.columnRefMap = columnRefMap;
        if (commonSubOperatorMap == null) {
            this.commonSubOperatorMap = new HashMap<>();
        } else {
            this.commonSubOperatorMap = commonSubOperatorMap;
        }
        this.needReuseLambdaDependentExpr = needReuseLambdaDependentExpr;
    }

    public List<ColumnRefOperator> getOutputColumns() {
        return new ArrayList<>(columnRefMap.keySet());
    }

    public Map<ColumnRefOperator, ScalarOperator> getColumnRefMap() {
        return columnRefMap;
    }

    public Map<ColumnRefOperator, ScalarOperator> getCommonSubOperatorMap() {
        return commonSubOperatorMap;
    }

    // For sql: select *, to_bitmap(S_SUPPKEY) from table, we needn't apply global dict optimization
    // This method differ from `couldApplyStringDict` method is for ColumnRefOperator, we return false.
    public boolean needApplyStringDict(Set<Integer> childDictColumns) {
        Preconditions.checkState(!childDictColumns.isEmpty());
        ColumnRefSet dictSet = new ColumnRefSet();
        for (Integer id : childDictColumns) {
            dictSet.union(id);
        }

        for (ScalarOperator operator : columnRefMap.values()) {
            if (!operator.isColumnRef() && couldApplyStringDict(operator, dictSet, childDictColumns)) {
                return true;
            }
        }

        for (ScalarOperator operator : commonSubOperatorMap.values()) {
            if (!operator.isColumnRef() && couldApplyStringDict(operator, dictSet, childDictColumns)) {
                return true;
            }
        }
        return false;
    }

    public boolean couldApplyStringDict(Set<Integer> childDictColumns) {
        Preconditions.checkState(!childDictColumns.isEmpty());
        ColumnRefSet dictSet = new ColumnRefSet();
        for (Integer id : childDictColumns) {
            dictSet.union(id);
        }

        for (ScalarOperator operator : columnRefMap.values()) {
            if (couldApplyStringDict(operator, dictSet, childDictColumns)) {
                return true;
            }
        }

        for (ScalarOperator operator : commonSubOperatorMap.values()) {
            if (couldApplyStringDict(operator, dictSet, childDictColumns)) {
                return true;
            }
        }

        return false;
    }

    public static boolean couldApplyDictOptimize(ScalarOperator operator, Set<Integer> sids) {
        return AddDecodeNodeForDictStringRule.DecodeVisitor.couldApplyDictOptimize(operator, sids);
    }

    public static boolean cannotApplyDictOptimize(ScalarOperator operator, Set<Integer> sids) {
        return AddDecodeNodeForDictStringRule.DecodeVisitor.cannotApplyDictOptimize(operator, sids);
    }

    private boolean couldApplyStringDict(ScalarOperator operator, ColumnRefSet dictSet, Set<Integer> sids) {
        ColumnRefSet usedColumns = operator.getUsedColumns();
        if (usedColumns.isIntersect(dictSet)) {
            return couldApplyDictOptimize(operator, sids);
        }
        return false;
    }

    public void fillDisableDictOptimizeColumns(ColumnRefSet columnRefSet, Set<Integer> sids) {
        columnRefMap.forEach((k, v) -> {
            if (columnRefSet.contains(k.getId())) {
                columnRefSet.union(v.getUsedColumns());
            } else {
                fillDisableDictOptimizeColumns(v, columnRefSet, sids);
            }
        });
    }

    public boolean hasUnsupportedDictOperator(Set<Integer> stringColumnIds, Set<Integer> sids) {
        ColumnRefSet stringColumnRefSet = new ColumnRefSet();
        for (Integer stringColumnId : stringColumnIds) {
            stringColumnRefSet.union(stringColumnId);
        }

        ColumnRefSet columnRefSet = new ColumnRefSet();
        this.fillDisableDictOptimizeColumns(columnRefSet, sids);
        return columnRefSet.isIntersect(stringColumnRefSet);
    }

    private void fillDisableDictOptimizeColumns(ScalarOperator operator, ColumnRefSet columnRefSet, Set<Integer> sids) {
        if (cannotApplyDictOptimize(operator, sids)) {
            columnRefSet.union(operator.getUsedColumns());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Projection that = (Projection) o;
        return columnRefMap.equals(that.columnRefMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnRefMap.keySet());
    }

    @Override
    public String toString() {
        return columnRefMap.values().toString();
    }
}
