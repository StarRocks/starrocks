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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RoundRobinDistributionSpecTest {

    @Test
    void isSatisfy() {
        DistributionSpec rr = new RoundRobinDistributionSpec();
        assertTrue(rr.isSatisfy(AnyDistributionSpec.INSTANCE));
        assertTrue(rr.isSatisfy(new RoundRobinDistributionSpec()));
        assertFalse(rr.isSatisfy(new ReplicatedDistributionSpec()));
    }

    @Test
    void testHashCode() {
        DistributionSpec rr1 = new RoundRobinDistributionSpec();
        DistributionSpec rr2 = new RoundRobinDistributionSpec();
        assertEquals(rr1.hashCode(), rr2.hashCode());
    }

    @Test
    void testEquals() {
        DistributionSpec rr1 = new RoundRobinDistributionSpec();
        DistributionSpec rr2 = new RoundRobinDistributionSpec();
        assertEquals(rr1, rr2);
    }

    @Test
    void testToString() {
        DistributionSpec rr = new RoundRobinDistributionSpec();
        assertEquals("ROUND_ROBIN", rr.toString());
    }
}