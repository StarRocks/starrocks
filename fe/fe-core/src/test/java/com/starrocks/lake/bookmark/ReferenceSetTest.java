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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReferenceSetTest {

    @Test
    public void testPutGetRemove() {
        ReferenceSet rs = new ReferenceSet(1000L);
        assertEquals(1000L, rs.getReferencedSinceMs());
        assertTrue(rs.isEmpty());
        assertEquals(0, rs.size());

        HolderId h1 = new HolderId("h1");
        Reference ref1 = new Reference(1500L, HolderInfo.EmptyInfo.INSTANCE);
        rs.put(h1, ref1);
        assertSame(ref1, rs.get(h1));
        assertEquals(1, rs.size());
        assertFalse(rs.isEmpty());

        // Idempotent: a second put for the same holder leaves the existing ref untouched.
        Reference ref1b = new Reference(2000L, HolderInfo.EmptyInfo.INSTANCE);
        rs.put(h1, ref1b);
        assertSame(ref1, rs.get(h1));
        assertEquals(1, rs.size());

        HolderId h2 = new HolderId("h2");
        Reference ref2 = new Reference(2500L, HolderInfo.EmptyInfo.INSTANCE);
        rs.put(h2, ref2);
        assertEquals(2, rs.size());

        rs.remove(h1);
        assertNull(rs.get(h1));
        assertEquals(1, rs.size());

        rs.remove(h2);
        assertTrue(rs.isEmpty());
        assertEquals(1000L, rs.getReferencedSinceMs());
    }
}
