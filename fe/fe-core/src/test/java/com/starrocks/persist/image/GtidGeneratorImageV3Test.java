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

package com.starrocks.persist.image;

import com.starrocks.persist.proto.GtidGeneratorPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.transaction.GtidGenerator;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

class GtidGeneratorImageV3Test {
    @TempDir
    Path temporary;

    private static class Generator extends GtidGenerator {
        @Override
        protected long timeGen() {
            return 1_700_000_000_000L;
        }

        @Override
        protected void persistBatchEndGtid(long bound) {
            applyBatchEndGtid(bound);
        }
    }

    @Test
    void restoresReservedBoundAndSkipsUnusedIdsLikeV2() throws Exception {
        GlobalStateMgr global = LoadImageTestSupport.global();
        when(global.isLeader()).thenReturn(true);
        long bound = GtidGenerator.encode(1_700_000_005_000L, GtidGenerator.MAX_SEQUENCE);
        Generator source = new Generator();
        source.init(bound);
        source.setLastGtid(bound - 100);

        // Save V3 first, so a V2 serialization hook cannot prepare its state for V3.
        Path file = LoadImageTestSupport.save(temporary, source::save);
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        source.save(image.getImageWriter());
        Generator v2 = new Generator();
        v2.load(image.getMetaBlockReader());
        Generator v3 = new Generator();
        LoadImageTestSupport.load(file, v3.getImageV3Loaders());
        for (Generator restored : List.of(v2, v3)) {
            assertEquals(bound, restored.getBatchEndGtid());
            assertEquals(bound, restored.lastGtid());
            assertEquals(5_000, restored.getTimestampAheadOfClockMs());
            restored.init(bound - 1000);
            assertEquals(bound, restored.getBatchEndGtid());
            assertTrue(restored.nextGtid() > bound, "a restart must consume the entire reserved batch");
        }
        assertEquals(v2.lastGtid(), v3.lastGtid());
        assertEquals(v2.getBatchEndGtid(), v3.getBatchEndGtid());
    }

    @Test
    void missingOptionalBoundMatchesEmptyV2() throws Exception {
        Path file = LoadImageTestSupport.entries(temporary, SectionId.GTID_GENERATOR,
                GtidGeneratorPB.getDefaultInstance(), List.of());
        Generator v3 = new Generator();
        LoadImageTestSupport.load(file, v3.getImageV3Loaders());
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        new Generator().save(image.getImageWriter());
        Generator v2 = new Generator();
        v2.load(image.getMetaBlockReader());
        assertEquals(0, v3.getBatchEndGtid());
        assertEquals(v2.getLastTimestamp(), v3.getLastTimestamp());
        assertEquals(v2.getLastSequence(), v3.getLastSequence());
    }
}
