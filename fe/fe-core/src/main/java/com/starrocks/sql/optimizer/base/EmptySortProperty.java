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

import com.google.common.collect.ImmutableList;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.GroupExpression;

public class EmptySortProperty extends SortProperty {

    public static final EmptySortProperty INSTANCE = new EmptySortProperty();

    private EmptySortProperty() {
        super(new OrderSpec(ImmutableList.of()));
    }


    @Override
    public boolean isEmpty() {
        return true;
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
    public GroupExpression appendEnforcers(Group child) {
        throw new StarRocksPlannerException("cannot enforce empty sort property.", ErrorType.INTERNAL_ERROR);
    }

    @Override
    public String toString() {
        return "EMPTY SORT";
    }
}
