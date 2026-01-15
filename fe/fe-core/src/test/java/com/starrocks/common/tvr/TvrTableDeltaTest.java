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

package com.starrocks.common.tvr;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class TvrTableDeltaTest {
    @Test
    public void testEmptyDelta() {
        TvrTableDelta delta = TvrTableDelta.empty();
        Assertions.assertTrue(delta.isEmpty());
        Assertions.assertEquals(Optional.empty(), delta.start());
        Assertions.assertEquals(Optional.empty(), delta.end());

        Assertions.assertEquals(delta.fromSnapshot(), TvrTableSnapshot.empty());
        Assertions.assertEquals(delta.toSnapshot(), TvrTableSnapshot.empty());
    }

    @Test
    public void testNonEmptyDelta() {
        TvrVersion fromVersion = TvrVersion.of(100L);
        TvrVersion toVersion = TvrVersion.of(200L);
        TvrTableDelta delta = new TvrTableDelta(fromVersion, toVersion);
        
        Assertions.assertFalse(delta.isEmpty());
        Assertions.assertEquals(fromVersion, delta.from);
        Assertions.assertEquals(toVersion, delta.to);
        Assertions.assertEquals(fromVersion, delta.from());
        Assertions.assertEquals(toVersion, delta.to());
        Assertions.assertEquals(delta.fromSnapshot(), TvrTableSnapshot.of(fromVersion));
        Assertions.assertEquals(delta.toSnapshot(), TvrTableSnapshot.of(toVersion));
    }

    @Test
    public void testEquality() {
        // Test empty deltas
        TvrTableDelta empty1 = TvrTableDelta.empty();
        TvrTableDelta empty2 = TvrTableDelta.empty();
        Assertions.assertEquals(empty1, empty2);
        Assertions.assertTrue(empty1.equals(empty2));

        // Test non-empty deltas
        TvrVersion version100 = TvrVersion.of(100L);
        TvrVersion version200 = TvrVersion.of(200L);
        TvrVersion version300 = TvrVersion.of(300L);
        
        TvrTableDelta delta1 = new TvrTableDelta(version100, version200);
        TvrTableDelta delta2 = new TvrTableDelta(version100, version200);
        TvrTableDelta delta3 = new TvrTableDelta(version200, version300);

        // Test equality
        Assertions.assertEquals(delta1, delta2);
        Assertions.assertTrue(delta1.equals(delta2));

        // Test inequality
        Assertions.assertNotEquals(delta1, delta3);
        Assertions.assertNotEquals(delta1, empty1);
        Assertions.assertNotEquals(delta1, null);
        Assertions.assertNotEquals(delta1, "not a delta");
    }

    @Test
    public void testHashCode() {
        // Test empty deltas
        TvrTableDelta empty1 = TvrTableDelta.empty();
        TvrTableDelta empty2 = TvrTableDelta.empty();
        Assertions.assertEquals(empty1.hashCode(), empty2.hashCode());

        // Test non-empty deltas
        TvrVersion version100 = TvrVersion.of(100L);
        TvrVersion version200 = TvrVersion.of(200L);
        TvrVersion version300 = TvrVersion.of(300L);
        
        TvrTableDelta delta1 = new TvrTableDelta(version100, version200);
        TvrTableDelta delta2 = new TvrTableDelta(version100, version200);
        TvrTableDelta delta3 = new TvrTableDelta(version200, version300);

        // Equal objects should have equal hash codes
        Assertions.assertEquals(delta1.hashCode(), delta2.hashCode());

        // Different objects should have different hash codes
        Assertions.assertNotEquals(delta1.hashCode(), delta3.hashCode());
        Assertions.assertNotEquals(delta1.hashCode(), empty1.hashCode());

        // Hash code should be consistent
        Assertions.assertEquals(delta1.hashCode(), delta1.hashCode());
    }

    @Test
    public void testToString() {
        // Test empty delta
        TvrTableDelta empty = TvrTableDelta.empty();
        Assertions.assertEquals("Delta@[MIN,MIN]", empty.toString());

        // Test regular versions
        TvrVersion version100 = TvrVersion.of(100L);
        TvrVersion version200 = TvrVersion.of(200L);
        TvrTableDelta delta = new TvrTableDelta(version100, version200);
        Assertions.assertEquals("Delta@[100,200]", delta.toString());

        // Test with MIN and MAX versions
        TvrTableDelta minMaxDelta = new TvrTableDelta(TvrVersion.MIN, TvrVersion.MAX);
        Assertions.assertEquals("Delta@[MIN,MAX]", minMaxDelta.toString());
    }
}