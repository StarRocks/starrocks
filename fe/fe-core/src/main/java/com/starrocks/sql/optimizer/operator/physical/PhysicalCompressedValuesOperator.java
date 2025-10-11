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

import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Physical Compressed Values operator for efficient serialization of large constant lists.
 */
public class PhysicalCompressedValuesOperator extends PhysicalOperator {
    private final List<ColumnRefOperator> columnRefSet;
    private final Type valueType;
    private final String rawConstantList;
    private final int constantCount;

    public PhysicalCompressedValuesOperator(List<ColumnRefOperator> columnRefSet, 
                                          Type valueType,
                                          String rawConstantList, 
                                          int constantCount) {
        super(OperatorType.PHYSICAL_VALUES);
        this.columnRefSet = columnRefSet;
        this.valueType = valueType;
        this.rawConstantList = rawConstantList;
        this.constantCount = constantCount;
    }

    public List<ColumnRefOperator> getColumnRefSet() {
        return columnRefSet;
    }

    public Type getValueType() {
        return valueType;
    }

    public String getRawConstantList() {
        return rawConstantList;
    }

    public int getConstantCount() {
        return constantCount;
    }

    @Override
    public RowOutputInfo deriveRowOutputInfo(List<OptExpression> inputs) {
        return new RowOutputInfo(columnRefSet.stream().distinct()
                .collect(Collectors.toMap(Function.identity(), Function.identity())));
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalCompressedValues(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalCompressedValues(optExpression, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        PhysicalCompressedValuesOperator that = (PhysicalCompressedValuesOperator) o;
        return constantCount == that.constantCount &&
               Objects.equals(columnRefSet, that.columnRefSet) &&
               Objects.equals(valueType, that.valueType) &&
               Objects.equals(rawConstantList, that.rawConstantList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), columnRefSet, valueType, rawConstantList, constantCount);
    }

    @Override
    public String toString() {
        return String.format("PhysicalCompressedValues{type=%s, count=%d}", valueType, constantCount);
    }
}
