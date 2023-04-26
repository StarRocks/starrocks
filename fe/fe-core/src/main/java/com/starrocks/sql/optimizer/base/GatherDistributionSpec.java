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

import com.starrocks.sql.optimizer.operator.Operator;

import java.util.Objects;

/**
 * Re-shuffle all date to one node
 */
public class GatherDistributionSpec extends DistributionSpec {
    // limit doesn't affect distribution property
    private long limit = Operator.DEFAULT_LIMIT;

    public GatherDistributionSpec() {
        super(DistributionType.GATHER);
    }

    public GatherDistributionSpec(long limit) {
        super(DistributionType.GATHER);
        this.limit = limit;
    }

    public boolean isSatisfy(DistributionSpec spec) {
        if (spec.type.equals(DistributionType.ANY)) {
            return true;
        }

        if (spec.type.equals(DistributionType.SHUFFLE) &&
                ((HashDistributionSpec) spec).getHashDistributionDesc().isAggShuffle()) {
            return true;
        }

        return spec instanceof GatherDistributionSpec;
    }

    public long getLimit() {
        return limit;
    }

    public boolean hasLimit() {
        return limit != Operator.DEFAULT_LIMIT;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        return obj instanceof GatherDistributionSpec;
    }

    @Override
    public String toString() {
        return "GATHER";
    }
}
