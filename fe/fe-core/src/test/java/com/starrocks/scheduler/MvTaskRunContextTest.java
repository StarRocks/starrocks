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

package com.starrocks.scheduler;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Table;
import com.starrocks.scheduler.mv.pct.PCTPartitionTopology;
import com.starrocks.scheduler.mv.pct.PCTRefreshScope;
import com.starrocks.sql.common.PCellNone;
import com.starrocks.sql.common.PCellSetMapping;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.common.PCellWithName;
import com.starrocks.sql.common.PartitionNameSetMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Set;

public class MvTaskRunContextTest {

    @Test
    public void testPartitionTopologyAndRefreshScopeDefaultToNullBeforeSet() {
        MvTaskRunContext context = new MvTaskRunContext(new TaskRunContext());

        Assertions.assertNull(context.getPartitionTopology());
        Assertions.assertNull(context.getRefreshScope());
    }

    @Test
    public void testSetPartitionTopologyStoresTopologyAndSupportsPartitionLookups() {
        MvTaskRunContext context = new MvTaskRunContext(new TaskRunContext());

        PCellSortedSet mvToCellMap = PCellSortedSet.of();
        mvToCellMap.add(PCellWithName.of("mv_p1", new PCellNone()));

        Table refBaseTable = Mockito.mock(Table.class);
        Mockito.when(refBaseTable.isNativeTableOrMaterializedView()).thenReturn(false);

        PCellSortedSet refBaseTableToCell = PCellSortedSet.of();
        refBaseTableToCell.add(PCellWithName.of("base_p1", new PCellNone()));
        Map<Table, PCellSortedSet> refBaseTableToCellMap = Maps.newHashMap();
        refBaseTableToCellMap.put(refBaseTable, refBaseTableToCell);

        PCellSetMapping refBaseTableMVIntersectedPartitions = PCellSetMapping.of();
        refBaseTableMVIntersectedPartitions.put("base_p1", PCellWithName.of("mv_p1", new PCellNone()));
        Map<Table, PCellSetMapping> baseToMvNameRef = Maps.newHashMap();
        baseToMvNameRef.put(refBaseTable, refBaseTableMVIntersectedPartitions);

        Map<String, Map<Table, PCellSortedSet>> mvToBaseNameRef = Maps.newHashMap();
        mvToBaseNameRef.put("mv_p1", refBaseTableToCellMap);

        PartitionNameSetMap externalRefBaseTableMVPartitionMap = PartitionNameSetMap.of();
        externalRefBaseTableMVPartitionMap.put("mv_p1", "base_p1");
        Map<Table, PartitionNameSetMap> externalMap = Maps.newHashMap();
        externalMap.put(refBaseTable, externalRefBaseTableMVPartitionMap);

        PCTPartitionTopology topology = new PCTPartitionTopology(mvToCellMap, refBaseTableToCellMap,
                baseToMvNameRef, mvToBaseNameRef, externalMap);

        context.setPartitionTopology(topology);

        Assertions.assertSame(topology, context.getPartitionTopology());
        Assertions.assertEquals(Set.of("base_p1"), context.getExternalTableRealPartitionName(refBaseTable, "mv_p1"));

        Table nativeTable = Mockito.mock(Table.class);
        Mockito.when(nativeTable.isNativeTableOrMaterializedView()).thenReturn(true);
        Assertions.assertEquals(Set.of("mv_p1"), context.getExternalTableRealPartitionName(nativeTable, "mv_p1"));
    }

    @Test
    public void testSetRefreshScopeStoresScope() {
        MvTaskRunContext context = new MvTaskRunContext(new TaskRunContext());

        PCellSortedSet mvPartitionsToRefresh = PCellSortedSet.of();
        mvPartitionsToRefresh.add(PCellWithName.of("mv_p1", new PCellNone()));

        PCellSetMapping refTablePartitionNames = PCellSetMapping.of();
        refTablePartitionNames.put("tbl1", PCellWithName.of("base_p1", new PCellNone()));

        PCTRefreshScope refreshScope = new PCTRefreshScope(
                mvPartitionsToRefresh,
                Maps.newHashMap(),
                refTablePartitionNames,
                false,
                false);

        context.setRefreshScope(refreshScope);

        Assertions.assertSame(refreshScope, context.getRefreshScope());
    }
}
