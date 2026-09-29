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

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class PhysicalAIProjectOperator extends PhysicalOperator {
    private final Map<ColumnRefOperator, ScalarOperator> columnRefMap;
    private final Map<ColumnRefOperator, ScalarOperator> commonSubOperatorMap;

    public PhysicalAIProjectOperator(Map<ColumnRefOperator, ScalarOperator> columnRefMap,
                                     Map<ColumnRefOperator, ScalarOperator> commonSubOperatorMap) {
        super(OperatorType.PHYSICAL_AI_PROJECT);
        this.columnRefMap = new LinkedHashMap<>(columnRefMap);
        this.commonSubOperatorMap = new LinkedHashMap<>(commonSubOperatorMap);
    }

    public Map<ColumnRefOperator, ScalarOperator> getColumnRefMap() {
        return columnRefMap;
    }

    public Map<ColumnRefOperator, ScalarOperator> getCommonSubOperatorMap() {
        return commonSubOperatorMap;
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        ColumnRefSet usedColumns = super.getUsedColumns();
        usedColumns.union(new Projection(columnRefMap, commonSubOperatorMap).getUsedColumns());
        return usedColumns;
    }

    public List<ColumnRefOperator> getOutputColumns() {
        return new ArrayList<>(columnRefMap.keySet());
    }

    @Override
    public RowOutputInfo deriveRowOutputInfo(List<OptExpression> inputs) {
        return new RowOutputInfo(columnRefMap, commonSubOperatorMap);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalAIProject(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalAIProject(optExpression, context);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!super.equals(object)) {
            return false;
        }
        PhysicalAIProjectOperator that = (PhysicalAIProjectOperator) object;
        return Objects.equals(columnRefMap, that.columnRefMap)
                && Objects.equals(commonSubOperatorMap, that.commonSubOperatorMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), columnRefMap, commonSubOperatorMap);
    }
}
