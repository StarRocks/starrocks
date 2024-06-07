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
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.operator.ColumnOutputInfo;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PhysicalSplitConsumeOperator extends PhysicalOperator {
    private final int splitId;

    private ScalarOperator splitPredicate;

    private final Map<ColumnRefOperator, ColumnRefOperator> outputColumnRefMap;

    public PhysicalSplitConsumeOperator(int splitId, ScalarOperator splitPredicate, DistributionSpec distributionSpec,
                                        Map<ColumnRefOperator, ColumnRefOperator> outputColumnRefMap) {
        // distributionSpec specifies the distribution of the input of this operator
        super(OperatorType.PHYSICAL_SPLIT_CONSUME, distributionSpec);
        this.splitId = splitId;
        this.splitPredicate = splitPredicate;
        this.outputColumnRefMap = outputColumnRefMap;
    }

    public int getSplitId() {
        return splitId;
    }

    public ScalarOperator getSplitPredicate() {
        return splitPredicate;
    }

    @Override
    public RowOutputInfo deriveRowOutputInfo(List<OptExpression> inputs) {
        List<ColumnOutputInfo> entryList = Lists.newArrayList();
        for (Map.Entry<ColumnRefOperator, ColumnRefOperator> entry : outputColumnRefMap.entrySet()) {
            entryList.add(new ColumnOutputInfo(entry.getKey(), entry.getValue()));
        }
        return new RowOutputInfo(entryList);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalSplitConsumer(optExpression, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }

        PhysicalSplitConsumeOperator that = (PhysicalSplitConsumeOperator) o;
        return Objects.equals(splitId, that.splitId) &&
                Objects.equals(splitPredicate, that.splitPredicate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), splitId, splitPredicate);
    }

    @Override
    public String toString() {
        return "PhysicalSplitConsumeOperator{" +
                "splitId='" + splitId + '\'' +
                ", predicate=" + splitPredicate +
                '}';
    }

}
