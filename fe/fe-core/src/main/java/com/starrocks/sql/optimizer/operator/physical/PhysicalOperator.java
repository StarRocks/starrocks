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

import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.OrderSpec;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.Objects;
import java.util.Set;

public abstract class PhysicalOperator extends Operator {
    protected OrderSpec orderSpec;
    protected DistributionSpec distributionSpec;

    protected PhysicalOperator(OperatorType type) {
        this(type, DistributionSpec.createAnyDistributionSpec(), OrderSpec.createEmpty());
    }

    protected PhysicalOperator(OperatorType type, DistributionSpec distributionSpec) {
        this(type, distributionSpec, OrderSpec.createEmpty());
    }

    protected PhysicalOperator(OperatorType type, OrderSpec orderSpec) {
        this(type, DistributionSpec.createAnyDistributionSpec(), orderSpec);
    }

    protected PhysicalOperator(OperatorType type, DistributionSpec distributionSpec,
                               OrderSpec orderSpec) {
        super(type);
        this.distributionSpec = distributionSpec;
        this.orderSpec = orderSpec;
    }

    public OrderSpec getOrderSpec() {
        return orderSpec;
    }

    public DistributionSpec getDistributionSpec() {
        return distributionSpec;
    }

    @Override
    public boolean isPhysical() {
        return true;
    }

    public ColumnRefSet getUsedColumns() {
        ColumnRefSet result = new ColumnRefSet();
        if (predicate != null) {
            result.union(predicate.getUsedColumns());
        }

        if (orderSpec != null) {
            orderSpec.getOrderDescs().forEach(o -> result.union(o.getColumnRef()));
        }

        return result;
    }

    public boolean couldApplyStringDict(Set<Integer> childDictColumns) {
        return false;
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
        PhysicalOperator that = (PhysicalOperator) o;
        return Objects.equals(orderSpec, that.orderSpec) &&
                Objects.equals(distributionSpec, that.distributionSpec);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), orderSpec, distributionSpec);
    }

    public abstract static class Builder<O extends PhysicalOperator, B extends PhysicalOperator.Builder>
            extends Operator.Builder<O, B> {
        @Override
        public B withOperator(O operator) {
            super.withOperator(operator);
            builder.distributionSpec = operator.distributionSpec;
            builder.orderSpec = operator.orderSpec;
            return (B) this;
        }

        public B setOrderSpec(OrderSpec orderSpec) {
            builder.orderSpec = orderSpec;
            return (B) this;
        }
    }
}
