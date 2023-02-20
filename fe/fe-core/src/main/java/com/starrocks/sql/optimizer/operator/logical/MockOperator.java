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

import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;

import java.util.List;
import java.util.Objects;

public class MockOperator extends LogicalOperator {
    private int value;

    public MockOperator(OperatorType opType) {
        super(opType);
        this.value = 0;
    }

    public MockOperator(OperatorType opType, int value) {
        super(opType);
        this.value = value;
    }

    @Override
    public boolean isLogical() {
        return true;
    }

    public int getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(opType, value);
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        return new ColumnRefSet();
    }

    @Override
    public RowOutputInfo deriveRowOutputInfo(List<OptExpression> inputs) {
        return RowOutputInfo.createEmptyDescriptor();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Operator)) {
            return false;
        }
        MockOperator rhs = (MockOperator) obj;
        if (this == rhs) {
            return true;
        }

        return opType == rhs.getOpType() && value == rhs.value;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitMockOperator(this, context);
    }
}
