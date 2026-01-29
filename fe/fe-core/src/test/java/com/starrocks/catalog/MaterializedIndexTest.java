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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/MaterializedIndexTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Range;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AggregateType;
import com.starrocks.type.IntegerType;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class MaterializedIndexTest {

    private MaterializedIndex index;
    private long indexId;

    private List<Column> columns;
    @Mocked
    private GlobalStateMgr globalStateMgr;

    private FakeGlobalStateMgr fakeGlobalStateMgr;

    @BeforeEach
    public void setUp() {
        indexId = 10000;

        columns = new LinkedList<Column>();
        columns.add(new Column("k1", IntegerType.TINYINT, true, null, "", ""));
        columns.add(new Column("k2", IntegerType.SMALLINT, true, null, "", ""));
        columns.add(new Column("v1", IntegerType.INT, false, AggregateType.REPLACE, "", ""));
        index = new MaterializedIndex(indexId, IndexState.NORMAL);

        fakeGlobalStateMgr = new FakeGlobalStateMgr();
        FakeGlobalStateMgr.setGlobalStateMgr(globalStateMgr);
        FakeGlobalStateMgr.setMetaVersion(FeConstants.META_VERSION);
    }

    @Test
    public void getMethodTest() {
        Assertions.assertEquals(indexId, index.getId());
    }

    @Test
    public void testVisibleForTransaction() {
        index = new MaterializedIndex(10);
        Assertions.assertEquals(IndexState.NORMAL, index.getState());
        Assertions.assertTrue(index.visibleForTransaction(0));
        Assertions.assertTrue(index.visibleForTransaction(10));

        index = new MaterializedIndex(10, IndexState.NORMAL, 10,
                PhysicalPartition.INVALID_SHARD_GROUP_ID);
        Assertions.assertTrue(index.visibleForTransaction(0));
        Assertions.assertTrue(index.visibleForTransaction(9));
        Assertions.assertTrue(index.visibleForTransaction(10));
        Assertions.assertTrue(index.visibleForTransaction(11));

        index = new MaterializedIndex(10, IndexState.SHADOW, 10,
                PhysicalPartition.INVALID_SHARD_GROUP_ID);
        Assertions.assertFalse(index.visibleForTransaction(0));
        Assertions.assertFalse(index.visibleForTransaction(9));
        Assertions.assertTrue(index.visibleForTransaction(10));
        Assertions.assertTrue(index.visibleForTransaction(11));
    }

    @Test
    public void testMaterializedIndexUpgrade() {
        MaterializedIndex index = new MaterializedIndex();
        Deencapsulation.setField(index, "id", 1L);
        Assertions.assertEquals(1L, index.getId());
        Assertions.assertEquals(0L, index.getMetaId());

        // Serialization/Deserialization
        String json = GsonUtils.GSON.toJson(index);
        MaterializedIndex newIndex = GsonUtils.GSON.fromJson(json, MaterializedIndex.class);

        Assertions.assertEquals(1L, newIndex.getMetaId());
    }

    // ==================== Share Adjacent Tablet Range Bounds Tests ====================

    private Tuple createTuple(int value) throws Exception {
        return new Tuple(Arrays.asList(Variant.of(IntegerType.INT, String.valueOf(value))));
    }

    private LocalTablet createTabletWithRange(long tabletId, Tuple lower, Tuple upper,
                                              boolean lowerIncluded, boolean upperIncluded) {
        LocalTablet tablet = new LocalTablet(tabletId);
        Range<Tuple> range = Range.of(lower, upper, lowerIncluded, upperIncluded);
        tablet.setRange(new TabletRange(range));
        return tablet;
    }

    @Test
    public void testShareAdjacentTabletRangeBounds() throws Exception {
        // Test that adjacent tablet range bounds are shared (same object reference)
        MaterializedIndex index = new MaterializedIndex(1, IndexState.NORMAL);

        Tuple t10 = createTuple(10);
        Tuple t20 = createTuple(20);
        Tuple t20Copy = createTuple(20);  // Different object, same value
        Tuple t30 = createTuple(30);

        // Create tablets with different Tuple objects for the shared bound
        LocalTablet tablet1 = createTabletWithRange(1001, t10, t20, true, false);
        LocalTablet tablet2 = createTabletWithRange(1002, t20Copy, t30, true, false);

        index.addTablet(tablet1, null, false);
        index.addTablet(tablet2, null, false);

        // Before sharing: bounds are equal but different objects
        Assertions.assertEquals(t20, tablet2.getRange().getRange().getLowerBound());
        Assertions.assertNotSame(t20, tablet2.getRange().getRange().getLowerBound());

        // Share adjacent bounds
        index.shareAdjacentTabletRangeBounds();

        // After sharing: tablet2's lower bound should be the same object as tablet1's upper bound
        Assertions.assertSame(
                tablet1.getRange().getRange().getUpperBound(),
                tablet2.getRange().getRange().getLowerBound());
    }

    @Test
    public void testShareAdjacentTabletRangeBoundsWithMultipleTablets() throws Exception {
        // Test sharing with multiple consecutive tablets
        MaterializedIndex index = new MaterializedIndex(1, IndexState.NORMAL);

        int numTablets = 5;
        Tuple[] bounds = new Tuple[numTablets + 1];
        for (int i = 0; i <= numTablets; i++) {
            bounds[i] = createTuple(i * 10);
        }

        // Create tablets with separate Tuple objects for each bound
        for (int i = 0; i < numTablets; i++) {
            Tuple lower = createTuple(i * 10);  // New Tuple object
            Tuple upper = createTuple((i + 1) * 10);  // New Tuple object
            LocalTablet tablet = createTabletWithRange(1000 + i, lower, upper, true, false);
            index.addTablet(tablet, null, false);
        }

        // Share adjacent bounds
        index.shareAdjacentTabletRangeBounds();

        // Verify all adjacent bounds are shared
        List<Tablet> tablets = index.getTablets();
        for (int i = 1; i < tablets.size(); i++) {
            Tablet prevTablet = tablets.get(i - 1);
            Tablet currTablet = tablets.get(i);

            Assertions.assertSame(
                    prevTablet.getRange().getRange().getUpperBound(),
                    currTablet.getRange().getRange().getLowerBound(),
                    "Tablets " + (i - 1) + " and " + i + " should share bound");
        }
    }

    @Test
    public void testShareAdjacentTabletRangeBoundsThrowsOnDiscontinuity() throws Exception {
        // Test that non-continuous ranges throw exception
        MaterializedIndex index = new MaterializedIndex(1, IndexState.NORMAL);

        Tuple t10 = createTuple(10);
        Tuple t20 = createTuple(20);
        Tuple t25 = createTuple(25);  // Gap: 20 != 25
        Tuple t30 = createTuple(30);

        LocalTablet tablet1 = createTabletWithRange(1001, t10, t20, true, false);
        LocalTablet tablet2 = createTabletWithRange(1002, t25, t30, true, false);

        index.addTablet(tablet1, null, false);
        index.addTablet(tablet2, null, false);

        // Should throw exception due to discontinuous ranges
        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class, () -> {
            index.shareAdjacentTabletRangeBounds();
        });

        // Verify error message contains relevant information
        Assertions.assertTrue(exception.getMessage().contains("not continuous"));
        Assertions.assertTrue(exception.getMessage().contains("1001"));
        Assertions.assertTrue(exception.getMessage().contains("1002"));
    }

    @Test
    public void testShareAdjacentTabletRangeBoundsThrowsOnNullBounds() throws Exception {
        // Test that null bounds throw exception
        MaterializedIndex index = new MaterializedIndex(1, IndexState.NORMAL);

        Tuple t10 = createTuple(10);
        Tuple t20 = createTuple(20);

        // First tablet with null upper bound (positive infinity)
        LocalTablet tablet1 = new LocalTablet(1001);
        Range<Tuple> range1 = Range.of(t10, null, true, false);  // [10, +∞)
        tablet1.setRange(new TabletRange(range1));

        // Second tablet with normal bounds
        LocalTablet tablet2 = createTabletWithRange(1002, t10, t20, true, false);

        index.addTablet(tablet1, null, false);
        index.addTablet(tablet2, null, false);

        // Should throw exception due to null bound
        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class, () -> {
            index.shareAdjacentTabletRangeBounds();
        });

        // Verify error message contains relevant information
        Assertions.assertTrue(exception.getMessage().contains("null"));
    }

    @Test
    public void testShareAdjacentTabletRangeBoundsSkipsNullTabletRange() throws Exception {
        // Test that null TabletRange is skipped (for non-range distribution tables)
        MaterializedIndex index = new MaterializedIndex(1, IndexState.NORMAL);

        Tuple t10 = createTuple(10);
        Tuple t20 = createTuple(20);
        Tuple t30 = createTuple(30);

        LocalTablet tablet1 = createTabletWithRange(1001, t10, t20, true, false);
        // tablet2 has null TabletRange (non-range distribution table)
        LocalTablet tablet2 = new LocalTablet(1002);
        tablet2.setRange(null);
        LocalTablet tablet3 = createTabletWithRange(1003, t20, t30, true, false);

        index.addTablet(tablet1, null, false);
        index.addTablet(tablet2, null, false);
        index.addTablet(tablet3, null, false);

        // Should not throw exception - null TabletRange is skipped
        index.shareAdjacentTabletRangeBounds();

        // Verify tablet1 and tablet3 ranges are unchanged (no sharing because tablet2 is in between)
        Assertions.assertNotNull(tablet1.getRange());
        Assertions.assertNull(tablet2.getRange());
        Assertions.assertNotNull(tablet3.getRange());
    }

    @Test
    public void testShareAdjacentTabletRangeBoundsWithEmptyList() throws Exception {
        // Test that empty tablets list doesn't cause issues
        MaterializedIndex index = new MaterializedIndex(1, IndexState.NORMAL);

        // Should not throw
        index.shareAdjacentTabletRangeBounds();

        Assertions.assertTrue(index.getTablets().isEmpty());
    }

    @Test
    public void testShareAdjacentTabletRangeBoundsWithSingleTablet() throws Exception {
        // Test that single tablet doesn't cause issues
        MaterializedIndex index = new MaterializedIndex(1, IndexState.NORMAL);

        Tuple t10 = createTuple(10);
        Tuple t20 = createTuple(20);

        LocalTablet tablet = createTabletWithRange(1001, t10, t20, true, false);
        index.addTablet(tablet, null, false);

        // Should not throw
        index.shareAdjacentTabletRangeBounds();

        // Verify tablet is unchanged
        Assertions.assertEquals(t10, tablet.getRange().getRange().getLowerBound());
        Assertions.assertEquals(t20, tablet.getRange().getRange().getUpperBound());
    }

    @Test
    public void testShareAdjacentTabletRangeBoundsPreservesInclusionFlags() throws Exception {
        // Test that inclusion flags are preserved during sharing
        MaterializedIndex index = new MaterializedIndex(1, IndexState.NORMAL);

        Tuple t10 = createTuple(10);
        Tuple t20 = createTuple(20);
        Tuple t20Copy = createTuple(20);
        Tuple t30 = createTuple(30);

        // tablet1: [10, 20] (closed interval)
        LocalTablet tablet1 = createTabletWithRange(1001, t10, t20, true, true);
        // tablet2: (20, 30) (open interval) - note: same bound value but different inclusion
        LocalTablet tablet2 = createTabletWithRange(1002, t20Copy, t30, false, false);

        index.addTablet(tablet1, null, false);
        index.addTablet(tablet2, null, false);

        // Share adjacent bounds
        index.shareAdjacentTabletRangeBounds();

        // Verify inclusion flags are preserved
        Range<Tuple> range1 = tablet1.getRange().getRange();
        Assertions.assertTrue(range1.isLowerBoundIncluded());
        Assertions.assertTrue(range1.isUpperBoundIncluded());

        Range<Tuple> range2 = tablet2.getRange().getRange();
        Assertions.assertFalse(range2.isLowerBoundIncluded());
        Assertions.assertFalse(range2.isUpperBoundIncluded());

        // Verify bounds are shared
        Assertions.assertSame(range1.getUpperBound(), range2.getLowerBound());
    }

    @Test
    public void testGsonPostProcessSharesAdjacentBounds() throws Exception {
        // Test that gsonPostProcess shares adjacent bounds after deserialization
        MaterializedIndex index = new MaterializedIndex(1, IndexState.NORMAL);

        Tuple t10 = createTuple(10);
        Tuple t20 = createTuple(20);
        Tuple t30 = createTuple(30);

        LocalTablet tablet1 = createTabletWithRange(1001, t10, t20, true, false);
        LocalTablet tablet2 = createTabletWithRange(1002, t20, t30, true, false);

        index.addTablet(tablet1, null, false);
        index.addTablet(tablet2, null, false);

        // Serialize and deserialize
        String json = GsonUtils.GSON.toJson(index);
        MaterializedIndex deserializedIndex = GsonUtils.GSON.fromJson(json, MaterializedIndex.class);

        // After deserialization, adjacent bounds should be shared
        List<Tablet> tablets = deserializedIndex.getTablets();
        Assertions.assertSame(
                tablets.get(0).getRange().getRange().getUpperBound(),
                tablets.get(1).getRange().getRange().getLowerBound());
    }

    @Test
    public void testSerializationRoundTripPreservesRanges() throws Exception {
        // Test complete serialization/deserialization round trip
        MaterializedIndex index = new MaterializedIndex(1, IndexState.NORMAL);

        Tuple t10 = createTuple(10);
        Tuple t20 = createTuple(20);
        Tuple t30 = createTuple(30);
        Tuple t40 = createTuple(40);

        // Create tablets: [10, 20), [20, 30), [30, 40)
        LocalTablet tablet1 = createTabletWithRange(1001, t10, t20, true, false);
        LocalTablet tablet2 = createTabletWithRange(1002, t20, t30, true, false);
        LocalTablet tablet3 = createTabletWithRange(1003, t30, t40, true, false);

        index.addTablet(tablet1, null, false);
        index.addTablet(tablet2, null, false);
        index.addTablet(tablet3, null, false);

        // Save original ranges for verification
        Range<Tuple> originalRange1 = tablet1.getRange().getRange();
        Range<Tuple> originalRange2 = tablet2.getRange().getRange();
        Range<Tuple> originalRange3 = tablet3.getRange().getRange();

        // Serialize and deserialize
        String json = GsonUtils.GSON.toJson(index);
        MaterializedIndex deserializedIndex = GsonUtils.GSON.fromJson(json, MaterializedIndex.class);

        // Verify all tablets have correct ranges after deserialization
        List<Tablet> tablets = deserializedIndex.getTablets();
        Assertions.assertEquals(3, tablets.size());

        // Verify ranges are equal
        Range<Tuple> range1 = tablets.get(0).getRange().getRange();
        Assertions.assertEquals(originalRange1.getLowerBound(), range1.getLowerBound());
        Assertions.assertEquals(originalRange1.getUpperBound(), range1.getUpperBound());

        Range<Tuple> range2 = tablets.get(1).getRange().getRange();
        Assertions.assertEquals(originalRange2.getLowerBound(), range2.getLowerBound());
        Assertions.assertEquals(originalRange2.getUpperBound(), range2.getUpperBound());

        Range<Tuple> range3 = tablets.get(2).getRange().getRange();
        Assertions.assertEquals(originalRange3.getLowerBound(), range3.getLowerBound());
        Assertions.assertEquals(originalRange3.getUpperBound(), range3.getUpperBound());
    }
}
