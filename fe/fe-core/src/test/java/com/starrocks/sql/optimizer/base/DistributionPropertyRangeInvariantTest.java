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

import com.starrocks.sql.optimizer.Group;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * PR-2 invariant tests: range-distribution spec must never reach
 * distribution enforcement, and null-strict conversion is a no-op for range.
 */
class DistributionPropertyRangeInvariantTest {

    private static RangeDistributionSpec buildRange() {
        EquivalentDescriptor descriptor = new EquivalentDescriptor(100L, Collections.emptyList());
        List<DistributionCol> cols = List.of(new DistributionCol(1, true));
        descriptor.initDistributionUnionFind(cols);
        return new RangeDistributionSpec(cols, descriptor);
    }

    @Test
    void appendEnforcersRejectsRangeSpec() {
        DistributionProperty property =
                DistributionProperty.createProperty(buildRange(), false);
        Group childGroup = Mockito.mock(Group.class);
        assertThrows(IllegalStateException.class,
                () -> property.appendEnforcers(childGroup));
    }

    @Test
    void getNullStrictPropertyIsIdentityForRange() {
        // Range colocate columns are always constructed with nullStrict=true.
        // getNullStrictProperty should be a no-op (returns the same instance).
        DistributionProperty property =
                DistributionProperty.createProperty(buildRange(), false);
        assertSame(property, property.getNullStrictProperty());
    }
}
