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
import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.List;
import java.util.Objects;

public class PhysicalSplitProduceOperator extends PhysicalOperator {
    private final int splitId;

    public PhysicalSplitProduceOperator(int cteId) {
        super(OperatorType.PHYSICAL_SPLIT_PRODUCE);
        this.splitId = cteId;
    }

    public int getSplitId() {
        return splitId;
    }


    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalSplitProducer(optExpression, context);
    }

    @Override
    public RowOutputInfo deriveRowOutputInfo(List<OptExpression> inputs) {
        return projectInputRow(inputs.get(0).getRowOutputInfo());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }

        PhysicalSplitProduceOperator that = (PhysicalSplitProduceOperator) o;
        return Objects.equals(splitId, that.splitId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), splitId);
    }

    @Override
    public String toString() {
        return "PhysicalSplitProduceOperator{" +
                "splitId='" + splitId + '\'' +
                '}';
    }
}
