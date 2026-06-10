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

package com.starrocks.alter.reshard;

import com.starrocks.catalog.TabletRange;
import com.starrocks.proto.SplittingTabletInfoPB;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.starrocks.persist.gson.GsonUtils.GSON;

public class SplittingTabletTest {

    @Test
    public void testJsonRoundTripPreservesEmptyRanges() {
        // Data-driven-path round-trip: empty newTabletRanges remains empty.
        SplittingTablet original = new SplittingTablet(2, new ArrayList<>(List.of(21L, 22L)));
        String json = GSON.toJson(original);
        SplittingTablet copy = GSON.fromJson(json, SplittingTablet.class);

        Assertions.assertNotNull(copy.getNewTabletRanges());
        Assertions.assertTrue(copy.getNewTabletRanges().isEmpty());
        Assertions.assertNull(copy.toProto().splittingTabletInfo.newTabletRanges);
    }

    @Test
    public void testConstructorRejectsNullList() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new SplittingTablet(2, List.of(21L, 22L), null));
    }

    @Test
    public void testConstructorRejectsNullElements() {
        // An internal external-boundaries caller passing a list with a null entry must
        // fail at construction with context, not later in the toProto() stream.
        List<TabletRange> ranges = Arrays.asList(new TabletRange(), null);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new SplittingTablet(2, List.of(21L, 22L), ranges));
    }

    @Test
    public void testConstructorRejectsSizeMismatch() {
        List<TabletRange> ranges = List.of(new TabletRange(), new TabletRange(), new TabletRange());
        Assertions.assertThrows(IllegalStateException.class,
                () -> new SplittingTablet(2, List.of(21L, 22L), ranges));
    }

    @Test
    public void testConstructorAcceptsEmptyRangesList() {
        SplittingTablet t = new SplittingTablet(2, new ArrayList<>(List.of(21L, 22L)),
                Collections.emptyList());
        Assertions.assertTrue(t.getNewTabletRanges().isEmpty());
    }

    @Test
    public void testFallbackClearsBothListsForExternalBoundariesPath() {
        // External-boundaries-shaped SplittingTablet with K=3 ranges. After
        // fallback, only the first new tablet survives and the K-entry ranges
        // list must be cleared to keep invariants consistent.
        List<TabletRange> ranges = new ArrayList<>(List.of(new TabletRange(), new TabletRange(), new TabletRange()));
        SplittingTablet t = new SplittingTablet(2, new ArrayList<>(List.of(21L, 22L, 23L)), ranges);

        Assertions.assertFalse(t.isIdenticalTablet());
        Assertions.assertEquals(3, t.getNewTabletRanges().size());

        t.fallbackToIdenticalTablet();

        Assertions.assertTrue(t.isIdenticalTablet());
        Assertions.assertEquals(1, t.getNewTabletIds().size());
        Assertions.assertTrue(t.getNewTabletRanges().isEmpty(),
                "fallback must clear stale FE-supplied ranges along with the trailing new tablet ids");
    }

    @Test
    public void testLegacyJsonWithoutNewTabletRangesDeserializesToEmptyList() {
        // Rolling-upgrade hazard: a SplittingTablet serialized before the
        // newTabletRanges field existed will lack the key entirely. Gson must
        // invoke the no-arg constructor and let the field initializer leave the
        // list non-null so toProto / fallbackToIdenticalTablet do not NPE.
        String legacyJson = "{\"oldTabletId\":2,\"newTabletIds\":[21,22]}";
        SplittingTablet copy = GSON.fromJson(legacyJson, SplittingTablet.class);

        Assertions.assertNotNull(copy.getNewTabletRanges(), "legacy record must yield a non-null ranges list");
        Assertions.assertTrue(copy.getNewTabletRanges().isEmpty());
        // The fallback path must work even on a legacy record carried through
        // a state transition post-upgrade.
        copy.fallbackToIdenticalTablet();
        Assertions.assertTrue(copy.isIdenticalTablet());
    }

    @Test
    public void testToProtoSerializesNonEmptyRanges() {
        List<TabletRange> ranges = List.of(new TabletRange(), new TabletRange());
        SplittingTablet t = new SplittingTablet(2, new ArrayList<>(List.of(21L, 22L)), ranges);

        SplittingTabletInfoPB pb = t.toProto().splittingTabletInfo;
        Assertions.assertEquals(Long.valueOf(2), pb.oldTabletId);
        Assertions.assertEquals(List.of(21L, 22L), pb.newTabletIds);
        Assertions.assertNotNull(pb.newTabletRanges);
        Assertions.assertEquals(2, pb.newTabletRanges.size());
    }
}
