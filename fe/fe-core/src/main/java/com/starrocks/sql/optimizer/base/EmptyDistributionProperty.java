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

import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.GroupExpression;

public class EmptyDistributionProperty extends DistributionProperty {

    public static final EmptyDistributionProperty INSTANCE = new EmptyDistributionProperty();

    private EmptyDistributionProperty() {
        super(new AnyDistributionSpec(), false);
    }

    public boolean isAny() {
        return true;
    }

    public boolean isShuffle() {
        return false;
    }

    public boolean isGather() {
        return false;
    }

    public boolean isBroadcast() {
        return false;
    }

    public boolean isCTERequired() {
        return false;
    }

    public GroupExpression appendEnforcers(Group child) {
        throw new StarRocksPlannerException("cannot enforce empty distribution property.", ErrorType.INTERNAL_ERROR);
    }

    public DistributionProperty getNullStrictProperty() {
        throw new StarRocksPlannerException("cannot obtain null strict property from empty distribution property.",
                ErrorType.INTERNAL_ERROR);
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj;
    }

    @Override
    public String toString() {
        return "EMPTY DISTRIBUTION";
    }
}
