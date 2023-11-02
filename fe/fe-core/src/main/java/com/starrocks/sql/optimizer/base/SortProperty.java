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


package com.starrocks.sql.optimizer.base;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.TopNType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;

public class SortProperty implements PhysicalProperty {
    private final OrderSpec spec;

    public SortProperty(OrderSpec spec) {
        this.spec = spec;
    }

    public OrderSpec getSpec() {
        return spec;
    }

    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean isSatisfy(PhysicalProperty other) {
        final OrderSpec rhs = ((SortProperty) other).getSpec();
        return spec.isSatisfy(rhs);
    }

    @Override
    public int hashCode() {
        return spec.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof SortProperty)) {
            return false;
        }

        SortProperty rhs = (SortProperty) obj;
        return spec.equals(rhs.getSpec());
    }

    @Override
    public GroupExpression appendEnforcers(Group child) {
        return new GroupExpression(new PhysicalTopNOperator(spec,
                Operator.DEFAULT_LIMIT, Operator.DEFAULT_OFFSET, null, Operator.DEFAULT_LIMIT, SortPhase.FINAL,
                TopNType.ROW_NUMBER, false,
                true, null, null),
                Lists.newArrayList(child));
    }

    @Override
    public String toString() {
        return spec.getOrderDescs().toString();
    }
}
