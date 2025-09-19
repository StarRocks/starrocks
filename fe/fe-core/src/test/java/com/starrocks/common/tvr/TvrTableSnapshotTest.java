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

public class TvrTableSnapshotTest {
    @Test
    public void testEmptySnapshot() {
        TvrTableSnapshot snapshot = TvrTableSnapshot.empty();
        Assertions.assertTrue(snapshot.isEmpty());
        Assertions.assertEquals(Optional.empty(), snapshot.end());
    }

    @Test
    public void testNonEmptySnapshot() {
        TvrVersion version = TvrVersion.of(100L);
        TvrTableSnapshot snapshot = new TvrTableSnapshot(version);
        Assertions.assertFalse(snapshot.isEmpty());
        Assertions.assertEquals(version, snapshot.to);
    }

    @Test
    public void testEquality() {
        // Test empty snapshots
        TvrTableSnapshot empty1 = TvrTableSnapshot.empty();
        TvrTableSnapshot empty2 = TvrTableSnapshot.empty();
        Assertions.assertEquals(empty1, empty2);
        Assertions.assertTrue(empty1.equals(empty2));

        // Test non-empty snapshots
        TvrVersion version100 = TvrVersion.of(100L);
        TvrVersion version200 = TvrVersion.of(200L);
        TvrTableSnapshot snapshot1 = new TvrTableSnapshot(version100);
        TvrTableSnapshot snapshot2 = new TvrTableSnapshot(version100);
        TvrTableSnapshot snapshot3 = new TvrTableSnapshot(version200);

        // Test equality
        Assertions.assertEquals(snapshot1, snapshot2);
        Assertions.assertTrue(snapshot1.equals(snapshot2));

        // Test inequality
        Assertions.assertNotEquals(snapshot1, snapshot3);
        Assertions.assertNotEquals(snapshot1, empty1);
        Assertions.assertNotEquals(snapshot1, null);
        Assertions.assertNotEquals(snapshot1, "not a snapshot");
    }

    @Test
    public void testHashCode() {
        // Test empty snapshots
        TvrTableSnapshot empty1 = TvrTableSnapshot.empty();
        TvrTableSnapshot empty2 = TvrTableSnapshot.empty();
        Assertions.assertEquals(empty1.hashCode(), empty2.hashCode());

        // Test non-empty snapshots
        TvrVersion version100 = TvrVersion.of(100L);
        TvrVersion version200 = TvrVersion.of(200L);
        TvrTableSnapshot snapshot1 = new TvrTableSnapshot(version100);
        TvrTableSnapshot snapshot2 = new TvrTableSnapshot(version100);
        TvrTableSnapshot snapshot3 = new TvrTableSnapshot(version200);

        // Equal objects should have equal hash codes
        Assertions.assertEquals(snapshot1.hashCode(), snapshot2.hashCode());

        // Different objects should have different hash codes
        Assertions.assertNotEquals(snapshot1.hashCode(), snapshot3.hashCode());
        Assertions.assertNotEquals(snapshot1.hashCode(), empty1.hashCode());

        // Hash code should be consistent
        Assertions.assertEquals(snapshot1.hashCode(), snapshot1.hashCode());
    }

    @Test
    public void testToString() {
        // Test empty snapshot
        TvrTableSnapshot empty = TvrTableSnapshot.empty();
        Assertions.assertEquals("Snapshot@(MIN)", empty.toString());

        // Test regular version
        TvrVersion version = TvrVersion.of(100L);
        TvrTableSnapshot snapshot = new TvrTableSnapshot(version);
        Assertions.assertEquals("Snapshot@(100)", snapshot.toString());

        // Test MIN version
        TvrTableSnapshot minSnapshot = new TvrTableSnapshot(TvrVersion.MIN);
        Assertions.assertEquals("Snapshot@(MIN)", minSnapshot.toString());

        // Test MAX version
        TvrTableSnapshot maxSnapshot = new TvrTableSnapshot(TvrVersion.MAX);
        Assertions.assertEquals("Snapshot@(MAX)", maxSnapshot.toString());
    }

}