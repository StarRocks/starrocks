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

package com.starrocks.lake;

import com.starrocks.catalog.TabletRange;
import com.starrocks.persist.gson.GsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LakeTabletTest {

    @Test
    public void testSerialization() {
        LakeTablet tablet = new LakeTablet(10001L);
        tablet.setDataSize(100L);
        tablet.setRowCount(10L);

        String json = GsonUtils.GSON.toJson(tablet);
        LakeTablet deserializedTablet = GsonUtils.GSON.fromJson(json, LakeTablet.class);

        Assertions.assertEquals(tablet.getId(), deserializedTablet.getId());
        Assertions.assertEquals(tablet.getDataSize(true), deserializedTablet.getDataSize(true));
        Assertions.assertEquals(tablet.getRowCount(0), deserializedTablet.getRowCount(0));
        Assertions.assertNull(deserializedTablet.getRange());
    }

    @Test
    public void testDefaultConstructor() {
        LakeTablet tablet = new LakeTablet();
        Assertions.assertNull(tablet.getRange());
    }

    @Test
    public void testRangeSerialization() {
        LakeTablet tablet = new LakeTablet(10002L, new TabletRange());
        String json = GsonUtils.GSON.toJson(tablet);
        LakeTablet deserializedTablet = GsonUtils.GSON.fromJson(json, LakeTablet.class);
        Assertions.assertNotNull(deserializedTablet.getRange());
    }

    @Test
    public void testDefaultsToHoldingSharedFiles() {
        LakeTablet tablet = new LakeTablet(100L);
        Assertions.assertTrue(tablet.hasSharedFiles());
    }

    @Test
    public void testObserveSharedFiles() {
        LakeTablet tablet = new LakeTablet(101L);
        tablet.setHasSharedFiles(true);
        Assertions.assertTrue(tablet.hasSharedFiles());

        tablet.setHasSharedFiles(false);
        Assertions.assertFalse(tablet.hasSharedFiles());
    }

    @Test
    public void testLastObservationWins() {
        // The most recent observation replaces whatever this tablet held before, in either direction:
        // a later "true" un-proves an earlier "false", just as a later "false" proves a tablet clean.
        LakeTablet tablet = new LakeTablet(102L);
        tablet.setHasSharedFiles(false);
        tablet.setHasSharedFiles(true);
        Assertions.assertTrue(tablet.hasSharedFiles());
    }

    @Test
    public void testAbsentReportUnprovesCleanState() {
        // A boxed null means the peer did not report the field at all -- an old BE during a rolling
        // upgrade, for instance -- and that silence must not let an earlier proof survive.
        LakeTablet tablet = new LakeTablet(104L);
        tablet.observeSharedFiles(Boolean.FALSE);
        Assertions.assertFalse(tablet.hasSharedFiles());

        tablet.observeSharedFiles((Boolean) null);
        Assertions.assertTrue(tablet.hasSharedFiles());
    }

    @Test
    public void testSharedFileStateIsNotSerialized() {
        LakeTablet tablet = new LakeTablet(103L);
        tablet.setHasSharedFiles(false);
        LakeTablet restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(tablet), LakeTablet.class);
        Assertions.assertTrue(restored.hasSharedFiles());
    }
}
