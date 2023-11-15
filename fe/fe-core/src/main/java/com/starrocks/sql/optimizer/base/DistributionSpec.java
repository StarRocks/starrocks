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

import com.starrocks.thrift.TDistributionType;

public class DistributionSpec {
    protected final DistributionType type;
    protected DistributionSpec(DistributionType type) {
        this.type = type;
    }

    @SuppressWarnings("unchecked")
    public <T> T cast() {
        return (T) this;
    }


    public DistributionType getType() {
        return type;
    }

    public static DistributionSpec createAnyDistributionSpec() {
        return AnyDistributionSpec.INSTANCE;
    }

    public static HashDistributionSpec createHashDistributionSpec(HashDistributionDesc distributionDesc) {
        return new HashDistributionSpec(distributionDesc);
    }

    public static DistributionSpec createReplicatedDistributionSpec() {
        return new ReplicatedDistributionSpec();
    }

    public static DistributionSpec createGatherDistributionSpec() {
        return new GatherDistributionSpec();
    }

    public boolean isSatisfy(DistributionSpec spec) {
        return false;
    }

    public enum DistributionType {
        ANY,
        BROADCAST,
        SHUFFLE,
        GATHER,
        ;

        public TDistributionType toThrift() {
            if (this == ANY) {
                return TDistributionType.ANY;
            } else if (this == BROADCAST) {
                return TDistributionType.BROADCAST;
            } else if (this == SHUFFLE) {
                return TDistributionType.SHUFFLE;
            } else {
                return TDistributionType.GATHER;
            }
        }
    }

    @Override
    public String toString() {
        return type.toString();
    }
}
