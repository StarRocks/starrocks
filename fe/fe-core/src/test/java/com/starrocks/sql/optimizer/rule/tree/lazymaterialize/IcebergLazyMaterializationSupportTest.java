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

package com.starrocks.sql.optimizer.rule.tree.lazymaterialize;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class IcebergLazyMaterializationSupportTest {
    private final IcebergLazyMaterializationSupport support = new IcebergLazyMaterializationSupport();

    private PhysicalIcebergScanOperator scan(IcebergTable table, Map<ColumnRefOperator, Column> columns) {
        PhysicalIcebergScanOperator scan = mock(PhysicalIcebergScanOperator.class);
        when(scan.getTable()).thenReturn(table);
        when(scan.getColRefToColumnMetaMap()).thenReturn(columns);
        return scan;
    }

    @Test
    void keepMetadataAndPredicateColumnsInScan() {
        Map<ColumnRefOperator, Column> columns = new LinkedHashMap<>();
        int id = 1;
        for (String name : List.of("_file", "$spec_id", "$data_sequence_number", "_pos", "_FILE", "payload")) {
            columns.put(new ColumnRefOperator(id++, IntegerType.BIGINT, name, true),
                    new Column(name, IntegerType.BIGINT, true));
        }
        PhysicalIcebergScanOperator scan = scan(mock(IcebergTable.class), columns);
        ScanOperatorPredicates predicates = mock(ScanOperatorPredicates.class);
        ColumnRefSet predicateColumns = new ColumnRefSet(100);
        when(predicates.getUsedColumns()).thenReturn(predicateColumns);
        when(scan.getScanOperatorPredicates()).thenReturn(predicates);
        ColumnRefSet earlyColumns = support.predicateUsedColumns(scan);
        assertEquals(ColumnRefSet.createByIds(List.of(1, 2, 3, 4, 5, 100)), earlyColumns);
        assertEquals(new ColumnRefSet(100), predicateColumns);
    }

    @Test
    void reuseExplicitRowPositionAsLocator() {
        ColumnRefFactory factory = new ColumnRefFactory();
        ColumnRefOperator pos = factory.create("_pos", IntegerType.BIGINT, true);
        IcebergTable table = mock(IcebergTable.class);
        PhysicalIcebergScanOperator scan = scan(table, Map.of(pos, new Column("_pos", IntegerType.BIGINT, true)));
        assertSame(pos, support.addRowIdColumns(scan, factory).get(2));
    }

    @Test
    void supportParquetV2AndV3ButExcludeLineage() {
        IcebergTable table = mock(IcebergTable.class);
        when(table.isParquetFormat()).thenReturn(true);
        PhysicalIcebergScanOperator scan = scan(table, Map.of());
        when(table.getFormatVersion()).thenReturn(1);
        assertFalse(support.supports(scan));
        when(table.getFormatVersion()).thenReturn(2);
        assertTrue(support.supports(scan));
        when(table.getFormatVersion()).thenReturn(3);
        assertTrue(support.supports(scan));
        for (String name : List.of("_row_id", "_last_updated_sequence_number")) {
            ColumnRefOperator ref = new ColumnRefOperator(1, IntegerType.BIGINT, name, true);
            assertFalse(support.supports(scan(table, Map.of(ref, new Column(name, IntegerType.BIGINT, true)))));
        }
        when(table.isParquetFormat()).thenReturn(false);
        assertFalse(support.supports(scan));
    }
}
