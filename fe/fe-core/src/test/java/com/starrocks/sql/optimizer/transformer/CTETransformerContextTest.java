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

package com.starrocks.sql.optimizer.transformer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CTETransformerContextTest {

    // save()/restore() must roll back every mutable structure so a discarded speculative "peek"
    // transform (RelationTransformer.buildCTEAnchorAndProducer) leaves no registrations behind --
    // otherwise cteRefIdMapping.size() would drift isForceInline() for later CTEs.
    @Test
    public void testSaveRestoreRollsBackRegistrations() {
        CTETransformerContext ctx = new CTETransformerContext(10);
        ctx.registerCte(1);

        CTETransformerContext.Memento memento = ctx.save();

        // Mutate everything a peek could touch.
        int id2 = ctx.registerCte(2);
        ctx.addForceCTE(id2);
        ctx.recordCteNodeCount(id2, 42);
        assertTrue(ctx.hasRegisteredCte(2));
        assertTrue(ctx.isForceCTE(id2));

        ctx.restore(memento);

        // Everything after the snapshot is rolled back; everything before it survives.
        assertTrue(ctx.hasRegisteredCte(1));
        assertFalse(ctx.hasRegisteredCte(2));
        assertFalse(ctx.isForceCTE(id2));
        assertNull(ctx.getCteNodeCount(id2));

        // uniqueId is restored too, so the next registration reuses the rolled-back id.
        int id3 = ctx.registerCte(3);
        assertEquals(id2, id3);
    }
}
