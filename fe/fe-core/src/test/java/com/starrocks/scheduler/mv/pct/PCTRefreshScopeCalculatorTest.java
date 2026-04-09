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

package com.starrocks.scheduler.mv.pct;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Table;
import com.starrocks.scheduler.mv.BaseTableSnapshotInfo;
import com.starrocks.sql.common.PCellNone;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.common.PCellWithName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.LinkedHashMap;
import java.util.Map;

public class PCTRefreshScopeCalculatorTest {

    private final PCTRefreshScopeCalculator calculator = new PCTRefreshScopeCalculator();

    @Test
    public void testProjectRefTableRefreshPartitionsUsesTopologyMappings() {
        Table tableA = Mockito.mock(Table.class);
        Table tableB = Mockito.mock(Table.class);
        Table tableC = Mockito.mock(Table.class);

        BaseTableSnapshotInfo snapshotInfoA = Mockito.mock(BaseTableSnapshotInfo.class);
        Mockito.when(snapshotInfoA.getBaseTable()).thenReturn(tableA);
        Mockito.when(snapshotInfoA.getName()).thenReturn("table_a");

        BaseTableSnapshotInfo snapshotInfoB = Mockito.mock(BaseTableSnapshotInfo.class);
        Mockito.when(snapshotInfoB.getBaseTable()).thenReturn(tableB);
        Mockito.when(snapshotInfoB.getName()).thenReturn("table_b");

        BaseTableSnapshotInfo snapshotInfoC = Mockito.mock(BaseTableSnapshotInfo.class);
        Mockito.when(snapshotInfoC.getBaseTable()).thenReturn(tableC);
        Mockito.when(snapshotInfoC.getName()).thenReturn("table_c");

        PCellSortedSet mvPartitionsToRefresh = PCellSortedSet.of();
        mvPartitionsToRefresh.add(PCellWithName.of("mv_p1", new PCellNone()));
        mvPartitionsToRefresh.add(PCellWithName.of("mv_p2", new PCellNone()));

        PCellSortedSet tableAPartitionsForMvP1 = PCellSortedSet.of();
        tableAPartitionsForMvP1.add(PCellWithName.of("base_a_p1", new PCellNone()));
        PCellSortedSet tableAPartitionsForMvP2 = PCellSortedSet.of();
        tableAPartitionsForMvP2.add(PCellWithName.of("base_a_p2", new PCellNone()));

        Map<String, Map<Table, PCellSortedSet>> mvToBasePartitions = Maps.newHashMap();
        Map<Table, PCellSortedSet> mvP1Refs = Maps.newHashMap();
        mvP1Refs.put(tableA, tableAPartitionsForMvP1);
        mvP1Refs.put(tableB, PCellSortedSet.of());
        mvToBasePartitions.put("mv_p1", mvP1Refs);

        Map<Table, PCellSortedSet> mvP2Refs = Maps.newHashMap();
        mvP2Refs.put(tableA, tableAPartitionsForMvP2);
        mvToBasePartitions.put("mv_p2", mvP2Refs);

        PCTPartitionTopology topology = new PCTPartitionTopology(
                PCellSortedSet.of(),
                Maps.newHashMap(),
                Maps.newHashMap(),
                mvToBasePartitions,
                Maps.newHashMap());

        Map<Long, BaseTableSnapshotInfo> snapshotBaseTables = new LinkedHashMap<>();
        snapshotBaseTables.put(1L, snapshotInfoA);
        snapshotBaseTables.put(2L, snapshotInfoB);
        snapshotBaseTables.put(3L, snapshotInfoC);

        Map<BaseTableSnapshotInfo, PCellSortedSet> projected = calculator.projectRefTableRefreshPartitions(
                topology, snapshotBaseTables, mvPartitionsToRefresh);

        Assertions.assertEquals(2, projected.size());
        Assertions.assertTrue(projected.get(snapshotInfoA).containsName("base_a_p1"));
        Assertions.assertTrue(projected.get(snapshotInfoA).containsName("base_a_p2"));
        Assertions.assertTrue(projected.containsKey(snapshotInfoB));
        Assertions.assertTrue(projected.get(snapshotInfoB).isEmpty());
        Assertions.assertFalse(projected.containsKey(snapshotInfoC));
    }

    @Test
    public void testBuildScopePackagesProjectedState() {
        Table table = Mockito.mock(Table.class);
        BaseTableSnapshotInfo snapshotInfo = Mockito.mock(BaseTableSnapshotInfo.class);
        Mockito.when(snapshotInfo.getBaseTable()).thenReturn(table);
        Mockito.when(snapshotInfo.getName()).thenReturn("table_a");

        PCellSortedSet mvPartitionsToRefresh = PCellSortedSet.of();
        mvPartitionsToRefresh.add(PCellWithName.of("mv_p1", new PCellNone()));

        PCellSortedSet refPartitions = PCellSortedSet.of();
        refPartitions.add(PCellWithName.of("base_a_p1", new PCellNone()));

        Map<String, Map<Table, PCellSortedSet>> mvToBasePartitions = Maps.newHashMap();
        Map<Table, PCellSortedSet> mvP1Refs = Maps.newHashMap();
        mvP1Refs.put(table, refPartitions);
        mvToBasePartitions.put("mv_p1", mvP1Refs);

        PCTPartitionTopology topology = new PCTPartitionTopology(
                PCellSortedSet.of(),
                Maps.newHashMap(),
                Maps.newHashMap(),
                mvToBasePartitions,
                Maps.newHashMap());

        Map<Long, BaseTableSnapshotInfo> snapshotBaseTables = Map.of(1L, snapshotInfo);

        PCTRefreshScope scope = calculator.buildScope(topology, snapshotBaseTables, mvPartitionsToRefresh, true, true);

        Assertions.assertFalse(scope.isEmpty());
        Assertions.assertTrue(scope.isCompleteRefresh());
        Assertions.assertTrue(scope.hasPotentialPartitions());
        Assertions.assertEquals(mvPartitionsToRefresh.getPartitionNames(), scope.getMvPartitionsToRefresh().getPartitionNames());
        Assertions.assertEquals(
                Map.of("table_a", refPartitions.getPartitionNames()),
                scope.getRefTablePartitionNames().getRefTablePartitionNames());
        Assertions.assertEquals(refPartitions.getPartitionNames(),
                scope.getRefTableRefreshPartitions().get(snapshotInfo).getPartitionNames());
    }

    @Test
    public void testBuildScopeFailsWhenSnapshotTablesShareTheSameName() {
        Table tableA = Mockito.mock(Table.class);
        Table tableB = Mockito.mock(Table.class);

        BaseTableSnapshotInfo snapshotInfoA = Mockito.mock(BaseTableSnapshotInfo.class);
        Mockito.when(snapshotInfoA.getBaseTable()).thenReturn(tableA);
        Mockito.when(snapshotInfoA.getName()).thenReturn("dup_table");

        BaseTableSnapshotInfo snapshotInfoB = Mockito.mock(BaseTableSnapshotInfo.class);
        Mockito.when(snapshotInfoB.getBaseTable()).thenReturn(tableB);
        Mockito.when(snapshotInfoB.getName()).thenReturn("dup_table");

        PCellSortedSet mvPartitionsToRefresh = PCellSortedSet.of();
        mvPartitionsToRefresh.add(PCellWithName.of("mv_p1", new PCellNone()));

        PCellSortedSet refPartitionsA = PCellSortedSet.of();
        refPartitionsA.add(PCellWithName.of("base_a_p1", new PCellNone()));
        PCellSortedSet refPartitionsB = PCellSortedSet.of();
        refPartitionsB.add(PCellWithName.of("base_b_p1", new PCellNone()));

        Map<String, Map<Table, PCellSortedSet>> mvToBasePartitions = Maps.newHashMap();
        Map<Table, PCellSortedSet> mvP1Refs = Maps.newHashMap();
        mvP1Refs.put(tableA, refPartitionsA);
        mvP1Refs.put(tableB, refPartitionsB);
        mvToBasePartitions.put("mv_p1", mvP1Refs);

        PCTPartitionTopology topology = new PCTPartitionTopology(
                PCellSortedSet.of(),
                Maps.newHashMap(),
                Maps.newHashMap(),
                mvToBasePartitions,
                Maps.newHashMap());

        Map<Long, BaseTableSnapshotInfo> snapshotBaseTables = new LinkedHashMap<>();
        snapshotBaseTables.put(1L, snapshotInfoA);
        snapshotBaseTables.put(2L, snapshotInfoB);

        Assertions.assertThrows(IllegalStateException.class,
                () -> calculator.buildScope(topology, snapshotBaseTables, mvPartitionsToRefresh, false, false));
    }
}
