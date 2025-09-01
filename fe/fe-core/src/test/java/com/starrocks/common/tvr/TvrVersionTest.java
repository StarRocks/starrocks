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

public class TvrVersionTest {

    @Test
    public void testConstants() {
        Assertions.assertEquals(Long.MIN_VALUE, TvrVersion.MIN_TIME);
        Assertions.assertEquals(Long.MAX_VALUE, TvrVersion.MAX_TIME);
        Assertions.assertEquals(Long.MIN_VALUE, TvrVersion.MIN.getVersion());
        Assertions.assertEquals(Long.MAX_VALUE, TvrVersion.MAX.getVersion());
    }

    @Test
    public void testOfMethod() {
        // Test MIN version
        TvrVersion minVersion = TvrVersion.of(Long.MIN_VALUE);
        Assertions.assertSame(TvrVersion.MIN, minVersion);
        Assertions.assertEquals(Long.MIN_VALUE, minVersion.getVersion());

        // Test MAX version
        TvrVersion maxVersion = TvrVersion.of(Long.MAX_VALUE);
        Assertions.assertSame(TvrVersion.MAX, maxVersion);
        Assertions.assertEquals(Long.MAX_VALUE, maxVersion.getVersion());

        // Test regular version
        long regularVersion = 12345L;
        TvrVersion version = TvrVersion.of(regularVersion);
        Assertions.assertEquals(regularVersion, version.getVersion());
        Assertions.assertNotSame(TvrVersion.MIN, version);
        Assertions.assertNotSame(TvrVersion.MAX, version);
    }

    @Test
    public void testIsMax() {
        Assertions.assertTrue(TvrVersion.MAX.isMax());
        Assertions.assertFalse(TvrVersion.MIN.isMax());
        Assertions.assertFalse(TvrVersion.of(12345L).isMax());
    }

    @Test
    public void testIsMin() {
        Assertions.assertTrue(TvrVersion.MIN.isMin());
        Assertions.assertFalse(TvrVersion.MAX.isMin());
        Assertions.assertFalse(TvrVersion.of(12345L).isMin());
    }

    @Test
    public void testGetVersion() {
        long versionValue = 98765L;
        TvrVersion version = TvrVersion.of(versionValue);
        Assertions.assertEquals(versionValue, version.getVersion());
    }

    @Test
    public void testToString() {
        Assertions.assertEquals("MIN", TvrVersion.MIN.toString());
        Assertions.assertEquals("MAX", TvrVersion.MAX.toString());
        Assertions.assertEquals("12345", TvrVersion.of(12345L).toString());
        Assertions.assertEquals("0", TvrVersion.of(0L).toString());
        Assertions.assertEquals("-12345", TvrVersion.of(-12345L).toString());
    }

    @Test
    public void testCompareTo() {
        // Test MIN comparisons
        Assertions.assertEquals(0, TvrVersion.MIN.compareTo(TvrVersion.MIN));
        Assertions.assertEquals(-1, TvrVersion.MIN.compareTo(TvrVersion.of(0L)));
        Assertions.assertEquals(-1, TvrVersion.MIN.compareTo(TvrVersion.MAX));

        // Test MAX comparisons
        Assertions.assertEquals(0, TvrVersion.MAX.compareTo(TvrVersion.MAX));
        Assertions.assertEquals(1, TvrVersion.MAX.compareTo(TvrVersion.of(0L)));
        Assertions.assertEquals(1, TvrVersion.MAX.compareTo(TvrVersion.MIN));

        // Test regular version comparisons
        TvrVersion version1 = TvrVersion.of(100L);
        TvrVersion version2 = TvrVersion.of(200L);
        Assertions.assertEquals(-1, version1.compareTo(version2));
        Assertions.assertEquals(1, version2.compareTo(version1));
        Assertions.assertEquals(0, version1.compareTo(version1));

        // Test with MIN and MAX
        Assertions.assertEquals(1, version1.compareTo(TvrVersion.MIN));
        Assertions.assertEquals(-1, version1.compareTo(TvrVersion.MAX));
    }

    @Test
    public void testIsAfter() {
        TvrVersion version1 = TvrVersion.of(100L);
        TvrVersion version2 = TvrVersion.of(200L);

        Assertions.assertTrue(version2.isAfter(version1));
        Assertions.assertFalse(version1.isAfter(version2));
        Assertions.assertFalse(version1.isAfter(version1));

        // Test with MIN and MAX
        Assertions.assertTrue(version1.isAfter(TvrVersion.MIN));
        Assertions.assertFalse(version1.isAfter(TvrVersion.MAX));
        Assertions.assertTrue(TvrVersion.MAX.isAfter(version1));
        Assertions.assertFalse(TvrVersion.MIN.isAfter(version1));
    }

    @Test
    public void testIsBefore() {
        TvrVersion version1 = TvrVersion.of(100L);
        TvrVersion version2 = TvrVersion.of(200L);

        Assertions.assertTrue(version1.isBefore(version2));
        Assertions.assertFalse(version2.isBefore(version1));
        Assertions.assertFalse(version1.isBefore(version1));

        // Test with MIN and MAX
        Assertions.assertFalse(version1.isBefore(TvrVersion.MIN));
        Assertions.assertTrue(version1.isBefore(TvrVersion.MAX));
        Assertions.assertFalse(TvrVersion.MAX.isBefore(version1));
        Assertions.assertTrue(TvrVersion.MIN.isBefore(version1));
    }

    @Test
    public void testEquals() {
        TvrVersion version1 = TvrVersion.of(100L);
        TvrVersion version2 = TvrVersion.of(100L);
        TvrVersion version3 = TvrVersion.of(200L);

        // Test same instance
        Assertions.assertEquals(version1, version1);

        // Test equal versions
        Assertions.assertEquals(version1, version2);
        Assertions.assertEquals(version2, version1);

        // Test different versions
        Assertions.assertNotEquals(version1, version3);
        Assertions.assertNotEquals(version3, version1);

        // Test with MIN and MAX
        Assertions.assertEquals(TvrVersion.MIN, TvrVersion.MIN);
        Assertions.assertEquals(TvrVersion.MAX, TvrVersion.MAX);
        Assertions.assertNotEquals(TvrVersion.MIN, TvrVersion.MAX);

        // Test with null and different types
        Assertions.assertNotEquals(version1, null);
        Assertions.assertNotEquals(version1, "string");
        Assertions.assertNotEquals(version1, 100L);
    }

    @Test
    public void testHashCode() {
        TvrVersion version1 = TvrVersion.of(100L);
        TvrVersion version2 = TvrVersion.of(100L);
        TvrVersion version3 = TvrVersion.of(200L);

        // Equal objects should have equal hash codes
        Assertions.assertEquals(version1.hashCode(), version2.hashCode());

        // Different objects should have different hash codes (usually)
        Assertions.assertNotEquals(version1.hashCode(), version3.hashCode());

        // Hash code should be consistent
        Assertions.assertEquals(version1.hashCode(), version1.hashCode());
    }

    @Test
    public void testEdgeCases() {
        // Test zero version
        TvrVersion zeroVersion = TvrVersion.of(0L);
        Assertions.assertEquals(0L, zeroVersion.getVersion());
        Assertions.assertEquals("0", zeroVersion.toString());
        Assertions.assertFalse(zeroVersion.isMin());
        Assertions.assertFalse(zeroVersion.isMax());

        // Test negative version
        TvrVersion negativeVersion = TvrVersion.of(-100L);
        Assertions.assertEquals(-100L, negativeVersion.getVersion());
        Assertions.assertEquals("-100", negativeVersion.toString());
        Assertions.assertFalse(negativeVersion.isMin());
        Assertions.assertFalse(negativeVersion.isMax());

        // Test large positive version
        long largeValue = 999999999999L;
        TvrVersion largeVersion = TvrVersion.of(largeValue);
        Assertions.assertEquals(largeValue, largeVersion.getVersion());
        Assertions.assertEquals(String.valueOf(largeValue), largeVersion.toString());
    }

    @Test
    public void testComparisonConsistency() {
        TvrVersion[] versions = {
            TvrVersion.MIN,
            TvrVersion.of(-1000L),
            TvrVersion.of(-1L),
            TvrVersion.of(0L),
            TvrVersion.of(1L),
            TvrVersion.of(1000L),
            TvrVersion.MAX
        };

        // Test that comparison is consistent across all versions
        for (int i = 0; i < versions.length; i++) {
            for (int j = 0; j < versions.length; j++) {
                if (i < j) {
                    Assertions.assertTrue(versions[i].compareTo(versions[j]) < 0);
                    Assertions.assertTrue(versions[i].isBefore(versions[j]));
                    Assertions.assertFalse(versions[i].isAfter(versions[j]));
                } else if (i > j) {
                    Assertions.assertTrue(versions[i].compareTo(versions[j]) > 0);
                    Assertions.assertTrue(versions[i].isAfter(versions[j]));
                    Assertions.assertFalse(versions[i].isBefore(versions[j]));
                } else {
                    Assertions.assertEquals(0, versions[i].compareTo(versions[j]));
                    Assertions.assertFalse(versions[i].isBefore(versions[j]));
                    Assertions.assertFalse(versions[i].isAfter(versions[j]));
                }
            }
        }
    }
}