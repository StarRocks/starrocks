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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.type.ArrayType;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link PreSplitTargets#findEligibleTable}, the table-level
 * structural eligibility gate shared by both load hooks and the multi-partition
 * coordinator's defensive re-check.
 *
 * <p>Each test stubs an {@link OlapTable} that passes every check up to the one
 * under test, then asserts the gate returns exactly that branch's
 * {@link SkipReason}. The {@code null} (eligible) path is also pinned as a
 * positive control.
 */
public class PreSplitTargetsTest {

    private static OlapTable tableThatPassesUpTo() {
        OlapTable table = mock(OlapTable.class);
        when(table.isCloudNativeTableOrMaterializedView()).thenReturn(true);
        when(table.isRangeDistribution()).thenReturn(true);
        when(table.getState()).thenReturn(OlapTable.OlapTableState.NORMAL);
        when(table.getVisibleIndexMetas()).thenReturn(List.of(mock(MaterializedIndexMeta.class)));
        return table;
    }

    @Test
    public void rejectsNonCloudNativeTable() {
        OlapTable table = mock(OlapTable.class);
        when(table.isCloudNativeTableOrMaterializedView()).thenReturn(false);
        when(table.isRangeDistribution()).thenReturn(true);
        Assertions.assertEquals(SkipReason.NOT_CLOUD_NATIVE,
                PreSplitTargets.findEligibleTable(mock(Database.class), table));
    }

    @Test
    public void rejectsNonRangeDistribution() {
        OlapTable table = mock(OlapTable.class);
        when(table.isCloudNativeTableOrMaterializedView()).thenReturn(true);
        when(table.isRangeDistribution()).thenReturn(false);
        Assertions.assertEquals(SkipReason.NOT_RANGE_DISTRIBUTION,
                PreSplitTargets.findEligibleTable(mock(Database.class), table));
    }

    @Test
    public void rejectsNonNormalTableState() {
        OlapTable table = mock(OlapTable.class);
        when(table.isCloudNativeTableOrMaterializedView()).thenReturn(true);
        when(table.isRangeDistribution()).thenReturn(true);
        when(table.getState()).thenReturn(OlapTable.OlapTableState.SCHEMA_CHANGE);
        Assertions.assertEquals(SkipReason.TABLE_NOT_NORMAL,
                PreSplitTargets.findEligibleTable(mock(Database.class), table));
    }

    @Test
    public void rejectsTableWithMaterializedViewOrRollup() {
        // More than one visible index meta means an MV / rollup is attached.
        OlapTable table = mock(OlapTable.class);
        when(table.isCloudNativeTableOrMaterializedView()).thenReturn(true);
        when(table.isRangeDistribution()).thenReturn(true);
        when(table.getState()).thenReturn(OlapTable.OlapTableState.NORMAL);
        when(table.getVisibleIndexMetas()).thenReturn(List.of(
                mock(MaterializedIndexMeta.class), mock(MaterializedIndexMeta.class)));
        Assertions.assertEquals(SkipReason.HAS_MATERIALIZED_VIEW_OR_ROLLUP,
                PreSplitTargets.findEligibleTable(mock(Database.class), table));
    }

    @Test
    public void rejectsEmptySortKey() {
        OlapTable table = tableThatPassesUpTo();
        try (MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class)) {
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table)).thenReturn(List.of());
            Assertions.assertEquals(SkipReason.UNSUPPORTED_SORT_KEY,
                    PreSplitTargets.findEligibleTable(mock(Database.class), table));
        }
    }

    @Test
    public void rejectsNonScalarSortKeyColumn() {
        OlapTable table = tableThatPassesUpTo();
        Column arrayColumn = new Column("arr", new ArrayType(IntegerType.INT));
        try (MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class)) {
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table))
                    .thenReturn(List.of(arrayColumn));
            Assertions.assertEquals(SkipReason.UNSUPPORTED_SORT_KEY,
                    PreSplitTargets.findEligibleTable(mock(Database.class), table));
        }
    }

    @Test
    public void acceptsEligibleTable() {
        // Positive control: range-distribution, NORMAL, single index, scalar sort key -> eligible (null).
        OlapTable table = tableThatPassesUpTo();
        Column scalarColumn = new Column("k", IntegerType.BIGINT);
        try (MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class)) {
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(table))
                    .thenReturn(List.of(scalarColumn));
            Assertions.assertNull(PreSplitTargets.findEligibleTable(mock(Database.class), table),
                    "fully eligible table must return null (no skip reason)");
        }
    }
}
