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

package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * LogicalRawValuesOperator is an optimized version of LogicalValuesOperator.
 * The key difference is that values are stored as raw constant values instead of expressions.
 *
 * Main differences from LogicalValuesOperator:
 * - Values are stored as raw constant strings obtained directly from the parser stage
 * - Avoids creating expression objects for constant values
 * - Significantly improves optimizer and deployment performance when dealing with large IN predicates
 *
 * This optimization is particularly effective for queries with large constant lists,
 * such as "SELECT * FROM table WHERE column IN (value1, value2, ..., valueN)" 
 * where N is large (hundreds or thousands of values).
 */
public class LogicalRawValuesOperator extends LogicalOperator {
    private final List<ColumnRefOperator> columnRefSet;
    private final Type constantType;
    private final String rawConstantList;
    private final int constantCount;

    public LogicalRawValuesOperator(List<ColumnRefOperator> columnRefSet,
                                    Type constantType,
                                    String rawConstantList,
                                    int constantCount) {
        super(OperatorType.LOGICAL_RAW_VALUES);
        this.columnRefSet = columnRefSet;
        this.constantType = constantType;
        this.rawConstantList = rawConstantList;
        this.constantCount = constantCount;
    }

    public List<ColumnRefOperator> getColumnRefSet() {
        return columnRefSet;
    }

    public Type getConstantType() {
        return constantType;
    }

    public String getRawConstantList() {
        return rawConstantList;
    }

    public int getConstantCount() {
        return constantCount;
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        ColumnRefSet columnRefSet = new ColumnRefSet();
        for (ColumnRefOperator columnRef : this.columnRefSet) {
            columnRefSet.union(columnRef);
        }
        return columnRefSet;
    }

    @Override
    public RowOutputInfo deriveRowOutputInfo(List<OptExpression> inputs) {
        return new RowOutputInfo(columnRefSet.stream().distinct()
                .collect(Collectors.toMap(Function.identity(), Function.identity())));
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalRawValues(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalRawValues(optExpression, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        LogicalRawValuesOperator that = (LogicalRawValuesOperator) o;
        return constantCount == that.constantCount &&
                Objects.equals(columnRefSet, that.columnRefSet) &&
                Objects.equals(constantType, that.constantType) &&
                Objects.equals(rawConstantList, that.rawConstantList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), columnRefSet, constantType, rawConstantList, constantCount);
    }

    @Override
    public String toString() {
        return String.format("LogicalRawValues{constantType=%s, count=%d, sample=%s}",
                constantType, constantCount, rawConstantList.length() > 50 ?
                        rawConstantList.substring(0, 47) + "..." : rawConstantList);
    }
}