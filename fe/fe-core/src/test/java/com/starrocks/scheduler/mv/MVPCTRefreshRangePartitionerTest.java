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

package com.starrocks.scheduler.mv;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.lake.LakeTable;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.mv.pct.MVPCTRefreshPartitioner;
import com.starrocks.scheduler.mv.pct.MVPCTRefreshRangePartitioner;
import com.starrocks.scheduler.mv.pct.PCTPartitionTopology;
import com.starrocks.sql.common.PCellNone;
import com.starrocks.sql.common.PCellSetMapping;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.common.PCellWithName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MVPCTRefreshRangePartitionerTest {

    @Test
    public void testGetAdaptivePartitionRefreshNumber() {
        MvTaskRunContext mvContext = mock(MvTaskRunContext.class);
        MaterializedView mv = mock(MaterializedView.class);
        when(mv.getTableProperty()).thenReturn(mock(TableProperty.class));
        when(mv.getPartitionInfo()).thenReturn(mock(PartitionInfo.class));
        when(mv.isPartitionedTable()).thenReturn(true);

        OlapTable refTable1 = Mockito.mock(OlapTable.class);
        Mockito.when(refTable1.isNativeTableOrMaterializedView()).thenReturn(true);
        PCellWithName pCellWithName1 = PCellWithName.of("partition1", new PCellNone());
        PCellWithName pCellWithName2 = PCellWithName.of("partition2", new PCellNone());
        PCellSortedSet refTablePartition1 = PCellSortedSet.of(Set.of(pCellWithName1, pCellWithName2));
        Map<Table, PCellSortedSet> ref1 = new HashMap<>();
        ref1.put(refTable1, refTablePartition1);

        IcebergTable refTable2 = Mockito.mock(IcebergTable.class);
        PCellSortedSet refTablePartition2 = PCellSortedSet.of(Set.of(pCellWithName1, pCellWithName2));
        Map<Table, PCellSortedSet> ref2 = new HashMap<>();
        ref2.put(refTable2, refTablePartition2);

        Map<String, Map<Table, PCellSortedSet>> mvToBaseNameRefs = Maps.newHashMap();
        mvToBaseNameRefs.put("mv_p1", ref1);
        mvToBaseNameRefs.put("mv_p2", ref2);

        Mockito.when(mvContext.getPartitionTopology()).thenReturn(new PCTPartitionTopology(
                PCellSortedSet.of(),
                Maps.newHashMap(),
                Maps.<Table, PCellSetMapping>newHashMap(),
                mvToBaseNameRefs));
        // TODO: make range cells
        List<PCellWithName> partitions = Arrays.asList(PCellWithName.of("mv_p1", new PCellNone()),
                PCellWithName.of("mv_p2", new PCellNone()));
        MVRefreshParams mvRefreshParams = new MVRefreshParams(mv, new HashMap<>());
        MVPCTRefreshRangePartitioner partitioner = new MVPCTRefreshRangePartitioner(mvContext, null,
                null, mv, mvRefreshParams);
        MVAdaptiveRefreshException exception = Assertions.assertThrows(MVAdaptiveRefreshException.class,
                () -> partitioner.getAdaptivePartitionRefreshNumber(PCellSortedSet.of(partitions)));
        Assertions.assertTrue(exception.getMessage().contains("Missing too many partition stats"));
    }

    @Test
    public void testIsAdaptiveRefreshSupported() {
        // internal tables and materialized views in both shared-nothing and shared-data mode
        Assertions.assertTrue(MVPCTRefreshPartitioner.isAdaptiveRefreshSupported(Table.TableType.OLAP));
        Assertions.assertTrue(MVPCTRefreshPartitioner.isAdaptiveRefreshSupported(Table.TableType.CLOUD_NATIVE));
        Assertions.assertTrue(MVPCTRefreshPartitioner.isAdaptiveRefreshSupported(
                Table.TableType.MATERIALIZED_VIEW));
        Assertions.assertTrue(MVPCTRefreshPartitioner.isAdaptiveRefreshSupported(
                Table.TableType.CLOUD_NATIVE_MATERIALIZED_VIEW));
        // external tables with partition statistics
        Assertions.assertTrue(MVPCTRefreshPartitioner.isAdaptiveRefreshSupported(Table.TableType.HIVE));
        Assertions.assertTrue(MVPCTRefreshPartitioner.isAdaptiveRefreshSupported(Table.TableType.ICEBERG));
        Assertions.assertTrue(MVPCTRefreshPartitioner.isAdaptiveRefreshSupported(Table.TableType.HUDI));
        Assertions.assertTrue(MVPCTRefreshPartitioner.isAdaptiveRefreshSupported(Table.TableType.DELTALAKE));
        // external tables without partition statistics
        Assertions.assertFalse(MVPCTRefreshPartitioner.isAdaptiveRefreshSupported(Table.TableType.JDBC));
        Assertions.assertFalse(MVPCTRefreshPartitioner.isAdaptiveRefreshSupported(Table.TableType.PAIMON));
    }

    @Test
    public void testGetAdaptivePartitionRefreshNumberWithCloudNativeTable() throws Exception {
        MvTaskRunContext mvContext = mock(MvTaskRunContext.class);
        MaterializedView mv = mock(MaterializedView.class);
        when(mv.getTableProperty()).thenReturn(mock(TableProperty.class));
        when(mv.getPartitionInfo()).thenReturn(mock(PartitionInfo.class));
        when(mv.isPartitionedTable()).thenReturn(true);

        // shared-data base table: each partition has 60M rows, so only one partition fits
        // into mv_max_rows_per_refresh (100M by default) per task run.
        LakeTable refTable = mock(LakeTable.class);
        when(refTable.getType()).thenReturn(Table.TableType.CLOUD_NATIVE);
        when(refTable.isNativeTableOrMaterializedView()).thenReturn(true);
        Map<String, Map<Table, PCellSortedSet>> mvToBaseNameRefs = Maps.newHashMap();
        List<PCellWithName> mvPartitions = Lists.newArrayList();
        for (int i = 1; i <= 3; i++) {
            String name = "p" + i;
            Partition partition = mock(Partition.class);
            when(partition.getRowCount()).thenReturn(60_000_000L);
            when(partition.getDataSize()).thenReturn(1024L);
            when(refTable.getPartition(name)).thenReturn(partition);

            Map<Table, PCellSortedSet> refs = new HashMap<>();
            refs.put(refTable, PCellSortedSet.of(Set.of(PCellWithName.of(name, new PCellNone()))));
            mvToBaseNameRefs.put("mv_" + name, refs);
            mvPartitions.add(PCellWithName.of("mv_" + name, new PCellNone()));
        }
        when(mvContext.getPartitionTopology()).thenReturn(new PCTPartitionTopology(
                PCellSortedSet.of(),
                Maps.newHashMap(),
                Maps.<Table, PCellSetMapping>newHashMap(),
                mvToBaseNameRefs));

        Assertions.assertTrue(MVPCTRefreshPartitioner.isAdaptiveRefreshSupported(refTable.getType()));
        MVRefreshParams mvRefreshParams = new MVRefreshParams(mv, new HashMap<>());
        MVPCTRefreshRangePartitioner partitioner = new MVPCTRefreshRangePartitioner(mvContext, null,
                null, mv, mvRefreshParams);
        Assertions.assertEquals(1, partitioner.getAdaptivePartitionRefreshNumber(PCellSortedSet.of(mvPartitions)));
    }

    @Test
    public void testFilterPartitionsByTTL() {
        MvTaskRunContext mvContext = mock(MvTaskRunContext.class);
        when(mvContext.getPartitionTTLNumber()).thenReturn(2);

        MaterializedView mv = mock(MaterializedView.class);
        when(mv.getTableProperty()).thenReturn(mock(TableProperty.class));
        when(mv.getPartitionInfo()).thenReturn(mock(PartitionInfo.class));
        when(mv.getTableProperty().getPartitionTTLNumber()).thenReturn(2);

        MVRefreshParams mvRefreshParams = new MVRefreshParams(mv, new HashMap<>());
        MVPCTRefreshRangePartitioner partitioner = new MVPCTRefreshRangePartitioner(mvContext, null, null, mv,
                mvRefreshParams);

        PCellSortedSet toRefreshPartitions = PCellSortedSet.of();
        toRefreshPartitions.add(PCellWithName.of("partition1", new PCellNone()));
        toRefreshPartitions.add(PCellWithName.of("partition2", new PCellNone()));
        toRefreshPartitions.add(PCellWithName.of("partition3", new PCellNone()));

        partitioner.filterPartitionsByTTL(toRefreshPartitions, true);

        Assertions.assertEquals(2, toRefreshPartitions.size());
    }
}
