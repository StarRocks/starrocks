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

import java.util.Objects;

/**
 * Re-shuffle all date across all nodes in a round-robin way.
 * Currently, the round-robin distribution is used to shuffle data between the connector sink fragment (e.g. hive sink)
 * and its child fragment.
 */
public class RoundRobinDistributionSpec extends DistributionSpec {
    public RoundRobinDistributionSpec() {
        super(DistributionType.ROUND_ROBIN);
    }

    public boolean isSatisfy(DistributionSpec spec) {
        if (spec.type.equals(DistributionType.ANY)) {
            return true;
        }

        return spec instanceof RoundRobinDistributionSpec;
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

        return obj instanceof RoundRobinDistributionSpec;
    }

    @Override
    public String toString() {
        return "ROUND_ROBIN";
    }
}
