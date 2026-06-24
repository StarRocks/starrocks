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

package com.starrocks.lake.bookmark;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReferenceTest {

    @Test
    public void testValues() {
        Reference r = new Reference(123_456_789L, HolderInfo.EmptyInfo.INSTANCE, 5_000L);
        assertEquals(123_456_789L, r.getAcquiredAtMs());
        assertSame(HolderInfo.EmptyInfo.INSTANCE, r.getHolderInfo());
        assertEquals(5_000L, r.getTtlMs());
    }

    @Test
    public void testEffectiveTtl() {
        // "<= 0" means "no limit"; effective TTL is the smaller of the enabled bounds.
        assertEquals(-1L, new Reference(0L, HolderInfo.EmptyInfo.INSTANCE, -1L).effectiveTtlMs(-1L));
        assertEquals(100L, new Reference(0L, HolderInfo.EmptyInfo.INSTANCE, 100L).effectiveTtlMs(-1L));
        assertEquals(100L, new Reference(0L, HolderInfo.EmptyInfo.INSTANCE, -1L).effectiveTtlMs(100L));
        assertEquals(50L, new Reference(0L, HolderInfo.EmptyInfo.INSTANCE, 100L).effectiveTtlMs(50L));
        assertEquals(50L, new Reference(0L, HolderInfo.EmptyInfo.INSTANCE, 50L).effectiveTtlMs(100L));
    }

    @Test
    public void testIsExpired() {
        Reference r = new Reference(1_000L, HolderInfo.EmptyInfo.INSTANCE, 100L);
        assertFalse(r.isExpired(1_099L, -1L));   // 1000 + 100 = 1100 > 1099
        assertTrue(r.isExpired(1_100L, -1L));    // boundary: acquiredAt + eff <= now
        assertTrue(r.isExpired(2_000L, -1L));

        Reference disabled = new Reference(1_000L, HolderInfo.EmptyInfo.INSTANCE, -1L);
        assertFalse(disabled.isExpired(Long.MAX_VALUE, -1L));   // disabled, no ceiling
        assertTrue(disabled.isExpired(1_100L, 100L));           // ceiling forces expiry
    }

    @Test
    public void testLegacyDefaultsToDisabled() {
        // A reference deserialized before TTL existed has no "ttl" key; Gson leaves
        // the long at 0, which is <= 0 -> disabled, same as a -1 no-TTL reference.
        Reference legacy = new Reference(1_000L, HolderInfo.EmptyInfo.INSTANCE, 0L);
        assertEquals(-1L, legacy.effectiveTtlMs(-1L));
        assertFalse(legacy.isExpired(Long.MAX_VALUE, -1L));
    }

    @Test
    public void testView() {
        Reference.View v = new Reference.View("h1", 1_000L, 500L);
        assertEquals("h1", v.getHolderId());
        assertEquals(1_000L, v.getAcquiredAtMs());
        assertEquals(500L, v.getTtlMs());
    }
}
