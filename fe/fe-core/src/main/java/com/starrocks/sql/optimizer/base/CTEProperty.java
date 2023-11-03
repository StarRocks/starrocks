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

import com.google.common.base.Objects;
import com.google.common.collect.Sets;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.GroupExpression;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Set;
import java.util.stream.Collectors;

public class CTEProperty implements PhysicalProperty {
    // All cteID will be passed from top to bottom, and prune plan when meet CTENoOp node with same CTEid 
    private final Set<Integer> cteIds;

    public static CTEProperty createProperty(Set<Integer> cteIds) {
        if (CollectionUtils.isEmpty(cteIds)) {
            return EmptyCTEProperty.INSTANCE;
        } else {
            return new CTEProperty(cteIds);
        }
    }

    protected CTEProperty(Set<Integer> cteIds) {
        this.cteIds = cteIds;
    }

    public CTEProperty(int cteId) {
        cteIds = Sets.newHashSet(cteId);
    }

    public Set<Integer> getCteIds() {
        return cteIds;
    }

    public boolean isEmpty() {
        return cteIds.isEmpty();
    }

    @Override
    public boolean isSatisfy(PhysicalProperty other) {
        return true;
    }

    @Override
    public GroupExpression appendEnforcers(Group child) {
        throw new StarRocksPlannerException("cannot enforce cte property", ErrorType.INTERNAL_ERROR);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CTEProperty that = (CTEProperty) o;
        return Objects.equal(cteIds, that.cteIds);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(cteIds);
    }

    @Override
    public String toString() {
        return "[" + cteIds.stream().map(String::valueOf).collect(Collectors.joining(", ")) + "]";
    }
}
