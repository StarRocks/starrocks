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

package com.starrocks.planner;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletScanKeyConstraint;
import com.starrocks.thrift.TTabletScanKeyConstraintType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TabletScanKeyConstraintBuilderTest {

    private static final long INDEX_META_ID = 1L;

    private static Column key(String name) {
        Column column = new Column(name, VarcharType.VARCHAR);
        column.setIsKey(true);
        return column;
    }

    private static Column intKey(String name) {
        Column column = new Column(name, IntegerType.INT);
        column.setIsKey(true);
        return column;
    }

    private static OlapTable table(List<Column> schema, List<Column> distributionColumns, int bucketNum) {
        OlapTable olapTable = new OlapTable(Table.TableType.OLAP);
        olapTable.maySetDatabaseId(1L);
        olapTable.setBaseIndexMetaId(INDEX_META_ID);
        olapTable.setIndexMeta(INDEX_META_ID, "base", schema, 0, 0, (short) schema.size(),
                TStorageType.COLUMN, KeysType.DUP_KEYS);
        olapTable.setDefaultDistributionInfo(new HashDistributionInfo(bucketNum, distributionColumns));
        return olapTable;
    }

    private static MaterializedIndex indexWithTablets(long... tabletIds) {
        MaterializedIndex index = new MaterializedIndex(INDEX_META_ID);
        for (long tabletId : tabletIds) {
            index.addTablet(new LocalTablet(tabletId), null, false);
        }
        return index;
    }

    // The bucket ordinal must be the tablet's position in the full tablet list, because that is the
    // list HashDistributionPruner indexes with `hash % size` when it selects tablets.
    @Test
    public void bucketOrdinalAndCountComeFromFullTabletList() {
        Column docId = key("doc_id");
        OlapTable olapTable = table(Collections.singletonList(docId), Collections.singletonList(docId), 4);
        TabletScanKeyConstraintBuilder builder = TabletScanKeyConstraintBuilder.create(
                olapTable, INDEX_META_ID, olapTable.getDefaultDistributionInfo());
        assertNotNull(builder);

        MaterializedIndex index = indexWithTablets(1001L, 1002L, 1003L, 1004L);
        for (int ordinal = 0; ordinal < 4; ordinal++) {
            long tabletId = 1001L + ordinal;
            TTabletScanKeyConstraint constraint = builder.build(index, tabletId, 2);
            assertNotNull(constraint);
            assertEquals(TTabletScanKeyConstraintType.HASH_BUCKET, constraint.getType());
            assertEquals(ordinal, constraint.getBucket_id());
            // Bucket count is the full tablet list, never the number of tablets the query selected.
            assertEquals(4, constraint.getBucket_num());
            assertEquals(Collections.singletonList(0), constraint.getDistribution_key_positions());
            assertEquals(1, constraint.getHash_version());
        }
    }

    @Test
    public void unknownTabletYieldsNoConstraint() {
        Column docId = key("doc_id");
        OlapTable olapTable = table(Collections.singletonList(docId), Collections.singletonList(docId), 4);
        TabletScanKeyConstraintBuilder builder = TabletScanKeyConstraintBuilder.create(
                olapTable, INDEX_META_ID, olapTable.getDefaultDistributionInfo());
        assertNotNull(builder);
        assertNull(builder.build(indexWithTablets(1001L, 1002L), 9999L, 1));
    }

    // pruning_was_exact only claims "FE narrowed the scan"; it must be false when every tablet is read,
    // otherwise the BE would treat a legitimately empty prune result as a hash-contract violation.
    @Test
    public void pruningWasExactOnlyWhenScanIsNarrowed() {
        Column docId = key("doc_id");
        OlapTable olapTable = table(Collections.singletonList(docId), Collections.singletonList(docId), 4);
        TabletScanKeyConstraintBuilder builder = TabletScanKeyConstraintBuilder.create(
                olapTable, INDEX_META_ID, olapTable.getDefaultDistributionInfo());
        assertNotNull(builder);
        MaterializedIndex index = indexWithTablets(1L, 2L, 3L, 4L);

        assertTrue(builder.build(index, 1L, 2).isPruning_was_exact());
        // All four tablets selected: nothing was ruled out.
        assertFalse(builder.build(index, 1L, 4).isPruning_was_exact());
        // Unknown selected count (re-schedule path) stays conservative.
        assertFalse(builder.build(index, 1L, 0).isPruning_was_exact());
    }

    // Positions are indexes into the sort-key tuple, in DDL declaration order of the distribution key.
    @Test
    public void distributionKeyPositionsFollowSortKeyOrder() {
        Column k1 = intKey("k1");
        Column k2 = key("k2");
        Column k3 = key("k3");
        // Sort key (k1, k2, k3); distribution by (k3, k1) -- deliberately out of sort-key order.
        OlapTable olapTable = table(Arrays.asList(k1, k2, k3), Arrays.asList(k3, k1), 8);
        TabletScanKeyConstraintBuilder builder = TabletScanKeyConstraintBuilder.create(
                olapTable, INDEX_META_ID, olapTable.getDefaultDistributionInfo());
        assertNotNull(builder);

        TTabletScanKeyConstraint constraint = builder.build(indexWithTablets(1L, 2L), 1L, 1);
        assertNotNull(constraint);
        // DDL order is (k3, k1) -> sort-key positions (2, 0).
        assertEquals(Arrays.asList(2, 0), constraint.getDistribution_key_positions());
    }

    // A distribution column outside the sort key never shows up in a scan key, so nothing can be routed.
    @Test
    public void noConstraintWhenDistributionColumnIsNotInSortKey() {
        Column k1 = key("k1");
        Column v1 = new Column("v1", IntegerType.INT);
        OlapTable olapTable = table(Arrays.asList(k1, v1), Collections.singletonList(v1), 8);
        assertNull(TabletScanKeyConstraintBuilder.create(olapTable, INDEX_META_ID,
                olapTable.getDefaultDistributionInfo()));
    }

    @Test
    public void noConstraintForNonHashDistribution() {
        Column k1 = key("k1");
        OlapTable olapTable = table(Collections.singletonList(k1), Collections.singletonList(k1), 8);
        assertNull(TabletScanKeyConstraintBuilder.create(olapTable, INDEX_META_ID,
                new RandomDistributionInfo(8)));
        assertNull(TabletScanKeyConstraintBuilder.create(olapTable, INDEX_META_ID, null));
    }

    @Test
    public void noConstraintForUnknownIndexMeta() {
        Column k1 = key("k1");
        OlapTable olapTable = table(Collections.singletonList(k1), Collections.singletonList(k1), 8);
        assertNull(TabletScanKeyConstraintBuilder.create(olapTable, -1L,
                olapTable.getDefaultDistributionInfo()));
        assertNull(TabletScanKeyConstraintBuilder.create(olapTable, 99999L,
                olapTable.getDefaultDistributionInfo()));
    }
}
