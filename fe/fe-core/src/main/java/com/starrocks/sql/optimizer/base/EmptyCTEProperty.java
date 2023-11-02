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

import com.google.common.collect.ImmutableSet;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;

public class EmptyCTEProperty extends CTEProperty {

    public static final EmptyCTEProperty INSTANCE = new EmptyCTEProperty();

    private EmptyCTEProperty() {
        super(ImmutableSet.of());
    }

    public CTEProperty removeCTE(int cteID) {
        throw new StarRocksPlannerException("cannot remove cte id from empty cte property.",
                ErrorType.INTERNAL_ERROR);
    }

    public boolean isEmpty() {
        return true;
    }

    public void merge(CTEProperty other) {
        throw new StarRocksPlannerException("cannot merge other property with empty cte property.",
                ErrorType.INTERNAL_ERROR);
    }

    @Override
    public boolean isSatisfy(PhysicalProperty other) {
        return true;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    @Override
    public boolean equals(Object o) {
        return this == o;
    }

    @Override
    public String toString() {
        return "EMPTY CTE";
    }
}
