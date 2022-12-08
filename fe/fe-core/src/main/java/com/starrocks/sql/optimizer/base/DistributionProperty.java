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
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;

public class DistributionProperty implements PhysicalProperty {
    private final DistributionSpec spec;
    private final boolean isCTERequired;

    public static final DistributionProperty EMPTY = new DistributionProperty();

    public DistributionProperty() {
        this.spec = DistributionSpec.createAnyDistributionSpec();
        this.isCTERequired = false;
    }

    public DistributionProperty(DistributionSpec spec) {
        this.spec = spec;
        this.isCTERequired = false;
    }

    public DistributionProperty(DistributionSpec spec, boolean isCTERequired) {
        this.spec = spec;
        this.isCTERequired = isCTERequired;
    }

    public DistributionSpec getSpec() {
        return spec;
    }

    public boolean isAny() {
        return spec.type == DistributionSpec.DistributionType.ANY;
    }

    public boolean isShuffle() {
        return spec.type == DistributionSpec.DistributionType.SHUFFLE;
    }

    public boolean isGather() {
        return spec.type == DistributionSpec.DistributionType.GATHER;
    }

    public boolean isBroadcast() {
        return spec.type == DistributionSpec.DistributionType.BROADCAST;
    }

    public boolean isCTERequired() {
        return isCTERequired;
    }

    @Override
    public boolean isSatisfy(PhysicalProperty other) {
        if (((DistributionProperty) other).isCTERequired()) {
            // always satisfy if parent is CTENoOp/CTEAnchor.
            // the plan will be adjusted in the father of CTENoOp/CTEAnchor, and
            // the distribution of CTENoOp/CTEAnchor always keep same with children
            return true;
        }
        DistributionSpec otherSpec = ((DistributionProperty) other).getSpec();
        return spec.isSatisfy(otherSpec);
    }

    public GroupExpression appendEnforcers(Group child) {
        return new GroupExpression(new PhysicalDistributionOperator(spec), Lists.newArrayList(child));
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
        if (!(obj instanceof DistributionProperty)) {
            return false;
        }

        DistributionProperty rhs = (DistributionProperty) obj;
        return spec.equals(rhs.getSpec()) && isCTERequired == rhs.isCTERequired;
    }
}
