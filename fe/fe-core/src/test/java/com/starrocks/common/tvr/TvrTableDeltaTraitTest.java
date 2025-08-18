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

public class TvrTableDeltaTraitTest {
    @Test
    public void testEmptyTrait() {
        TvrTableDeltaTrait trait = TvrTableDeltaTrait.DEFAULT;
        Assertions.assertTrue(trait.getTvrDelta().isEmpty());
        Assertions.assertEquals(TvrTableDelta.empty(), trait.getTvrDelta());
    }

    @Test
    public void testNonEmptyTrait() {
        TvrVersion fromVersion = TvrVersion.of(100L);
        TvrVersion toVersion = TvrVersion.of(200L);
        TvrTableDelta delta = new TvrTableDelta(fromVersion, toVersion);
        TvrTableDeltaTrait trait = TvrTableDeltaTrait.ofMonotonic(delta);

        Assertions.assertFalse(trait.getTvrDelta().isEmpty());
        Assertions.assertEquals(delta, trait.getTvrDelta());
        Assertions.assertEquals(fromVersion, trait.getTvrDelta().from());
        Assertions.assertEquals(toVersion, trait.getTvrDelta().to());
    }

    @Test
    public void testEquality() {
        // Test empty traits
        TvrTableDeltaTrait empty1 = TvrTableDeltaTrait.DEFAULT;
        TvrTableDeltaTrait empty2 = TvrTableDeltaTrait.DEFAULT;
        Assertions.assertEquals(empty1, empty2);
        Assertions.assertTrue(empty1.equals(empty2));

        // Test non-empty traits
        TvrVersion version100 = TvrVersion.of(100L);
        TvrVersion version200 = TvrVersion.of(200L);
        TvrVersion version300 = TvrVersion.of(300L);

        TvrTableDelta delta1 = new TvrTableDelta(version100, version200);
        TvrTableDelta delta2 = new TvrTableDelta(version100, version200);
        TvrTableDelta delta3 = new TvrTableDelta(version200, version300);

        TvrTableDeltaTrait trait1 = TvrTableDeltaTrait.ofMonotonic(delta1);
        TvrTableDeltaTrait trait2 = TvrTableDeltaTrait.ofMonotonic(delta2);
        TvrTableDeltaTrait trait3 = TvrTableDeltaTrait.ofMonotonic(delta3);

        // Test equality
        Assertions.assertEquals(trait1, trait2);
        Assertions.assertTrue(trait1.equals(trait2));

        // Test inequality
        Assertions.assertNotEquals(trait1, trait3);
        Assertions.assertNotEquals(trait1, empty1);
        Assertions.assertNotEquals(trait1, null);
        Assertions.assertNotEquals(trait1, "not a trait");
    }

    @Test
    public void testMonotonic() {
        TvrVersion version100 = TvrVersion.of(100L);
        TvrVersion version200 = TvrVersion.of(200L);
        TvrTableDelta delta1 = new TvrTableDelta(version100, version200);
        TvrTableDeltaTrait trait1 = TvrTableDeltaTrait.ofMonotonic(delta1);
        Assertions.assertTrue(trait1.isAppendOnly());
        Assertions.assertEquals(trait1.getTvrChangeType(), TvrTableDeltaTrait.TvrChangeType.MONOTONIC);
        Assertions.assertEquals(trait1.getTvrDeltaStats(), TvrDeltaStats.EMPTY);
    }

    @Test
    public void testRetractable() {
        TvrVersion version100 = TvrVersion.of(100L);
        TvrVersion version200 = TvrVersion.of(200L);
        TvrTableDelta delta1 = new TvrTableDelta(version100, version200);
        TvrTableDeltaTrait trait1 = TvrTableDeltaTrait.ofRetractable(delta1);
        Assertions.assertFalse(trait1.isAppendOnly());
        Assertions.assertEquals(trait1.getTvrChangeType(), TvrTableDeltaTrait.TvrChangeType.RETRACTABLE);
        Assertions.assertEquals(trait1.getTvrDeltaStats(), TvrDeltaStats.EMPTY);
    }

    @Test
    public void testToString() {
        // Test empty trait
        TvrTableDeltaTrait empty = TvrTableDeltaTrait.DEFAULT;
        {
            String result = empty.toString();
            Assertions.assertTrue(result.contains("Delta@[MIN,MIN]"));
            Assertions.assertTrue(result.contains("MONOTONIC"));
            Assertions.assertTrue(result.contains("{addedRows=0}"));
        }
        // Test regular versions
        TvrVersion version100 = TvrVersion.of(100L);
        TvrVersion version200 = TvrVersion.of(200L);
        TvrTableDelta delta = new TvrTableDelta(version100, version200);
        TvrTableDeltaTrait trait = TvrTableDeltaTrait.ofMonotonic(delta);
        {
            String result = trait.toString();
            Assertions.assertTrue(result.contains("Delta@[100,200]"));
            Assertions.assertTrue(result.contains("MONOTONIC"));
            Assertions.assertTrue(result.contains("{addedRows=0}"));
        }

        // Test with MIN and MAX versions
        TvrTableDelta minMaxDelta = new TvrTableDelta(TvrVersion.MIN, TvrVersion.MAX);
        TvrTableDeltaTrait minMaxTrait = TvrTableDeltaTrait.ofMonotonic(minMaxDelta);
        {
            String result = minMaxTrait.toString();
            Assertions.assertTrue(result.contains("Delta@[MIN,MAX]"));
            Assertions.assertTrue(result.contains("MONOTONIC"));
            Assertions.assertTrue(result.contains("{addedRows=0}"));
        }
    }
}