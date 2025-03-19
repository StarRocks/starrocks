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

import com.google.common.collect.Lists;
import com.starrocks.common.LocalExchangerType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.operator.ColumnOutputInfo;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;

public class PhysicalConcatenateOperator extends PhysicalSetOperation {

    private final LocalExchangerType localExchangeType;

    public PhysicalConcatenateOperator(List<ColumnRefOperator> columnRef,
                                       List<List<ColumnRefOperator>> childOutputColumns,
                                       LocalExchangerType localExchangeType, long limit) {
        super(OperatorType.PHYSICAL_CONCATENATE, columnRef, childOutputColumns, limit, null, null);
        this.localExchangeType = localExchangeType;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public RowOutputInfo deriveRowOutputInfo(List<OptExpression> inputs) {
        List<ColumnOutputInfo> columnOutputInfoList = Lists.newArrayList();
        outputColumnRefOp.stream().forEach(e -> columnOutputInfoList.add(new ColumnOutputInfo(e, e)));
        return new RowOutputInfo(columnOutputInfoList, outputColumnRefOp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }

        PhysicalConcatenateOperator that = (PhysicalConcatenateOperator) o;
        return localExchangeType == that.localExchangeType;
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalConcatenater(optExpression, context);
    }
}

