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

package com.starrocks.common.mv;

import com.google.common.collect.Lists;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MaterializedViewDependencyGraphTest {

    public MaterializedViewDependencyGraphTest() {
        super();
    }

    /**
     * Helper method to create a MaterializedView with given id and dependencies.
     * The dependencies are expressed as base table infos of the MV, like a persisted MV would carry them.
     */
    private MaterializedView createMockMV(long id, Long... dependencies) {
        List<BaseTableInfo> baseTableInfos = new ArrayList<>();
        for (Long depId : dependencies) {
            baseTableInfos.add(new BaseTableInfo(1L, "db", "mv_" + depId, depId));
        }
        return createMV(id, 1L, baseTableInfos);
    }

    /**
     * Helper method to create a MaterializedView in the given database with the given base table infos.
     */
    private MaterializedView createMV(long id, long dbId, List<BaseTableInfo> baseTableInfos) {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("c1", IntegerType.INT));

        MaterializedView mv = new MaterializedView(id, dbId, "mv_" + id, columns, KeysType.DUP_KEYS,
                null, null, null);
        mv.setBaseTableInfos(baseTableInfos);
        return mv;
    }

    /**
     * Helper method to turn layers of MVs into layers of MV ids, to compare them with an expected layering.
     */
    private static List<List<Long>> toLayerIds(List<List<MaterializedView>> layers) {
        List<List<Long>> layerIds = new ArrayList<>();
        for (List<MaterializedView> layer : layers) {
            List<Long> ids = new ArrayList<>();
            for (MaterializedView mv : layer) {
                ids.add(mv.getId());
            }
            layerIds.add(ids);
        }
        return layerIds;
    }

    @Test
    public void testEmptyList() {
        List<MaterializedView> allMVs = new ArrayList<>();
        List<MaterializedView> result = MaterializedViewDependencyGraph.buildTopologicalOrder(allMVs);
        
        Assertions.assertNotNull(result);
        Assertions.assertEquals(0, result.size());
    }

    @Test
    public void testSingleMVNoDependencies() {
        MaterializedView mv1 = createMockMV(1L);
        List<MaterializedView> allMVs = Lists.newArrayList(mv1);
        
        List<MaterializedView> result = MaterializedViewDependencyGraph.buildTopologicalOrder(allMVs);
        
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(1L, result.get(0).getId());
    }

    @Test
    public void testMultipleMVsNoDependencies() {
        MaterializedView mv1 = createMockMV(1L);
        MaterializedView mv2 = createMockMV(2L);
        MaterializedView mv3 = createMockMV(3L);
        List<MaterializedView> allMVs = Lists.newArrayList(mv1, mv2, mv3);
        
        List<MaterializedView> result = MaterializedViewDependencyGraph.buildTopologicalOrder(allMVs);
        
        Assertions.assertNotNull(result);
        Assertions.assertEquals(3, result.size());
        // All MVs should be in result (order doesn't matter without dependencies)
        Set<Long> ids = new HashSet<>();
        for (MaterializedView mv : result) {
            ids.add(mv.getId());
        }
        Assertions.assertTrue(ids.contains(1L));
        Assertions.assertTrue(ids.contains(2L));
        Assertions.assertTrue(ids.contains(3L));
    }

    @Test
    public void testLinearDependencyChain() {
        // mv3 depends on mv2, mv2 depends on mv1
        MaterializedView mv1 = createMockMV(1L);
        MaterializedView mv2 = createMockMV(2L, 1L);
        MaterializedView mv3 = createMockMV(3L, 2L);
        List<MaterializedView> allMVs = Lists.newArrayList(mv3, mv2, mv1);
        
        List<MaterializedView> result = MaterializedViewDependencyGraph.buildTopologicalOrder(allMVs);
        
        Assertions.assertNotNull(result);
        Assertions.assertEquals(3, result.size());
        
        // mv1 should come before mv2, mv2 should come before mv3
        int idx1 = -1;
        int idx2 = -1;
        int idx3 = -1;
        for (int i = 0; i < result.size(); i++) {
            long id = result.get(i).getId();
            if (id == 1L) {
                idx1 = i;
            }
            if (id == 2L) {
                idx2 = i;
            }
            if (id == 3L) {
                idx3 = i;
            }
        }
        Assertions.assertTrue(idx1 < idx2, "mv1 should come before mv2");
        Assertions.assertTrue(idx2 < idx3, "mv2 should come before mv3");
    }

    @Test
    public void testDiamondDependency() {
        // mv4 depends on mv2 and mv3
        // mv2 depends on mv1
        // mv3 depends on mv1
        MaterializedView mv1 = createMockMV(1L);
        MaterializedView mv2 = createMockMV(2L, 1L);
        MaterializedView mv3 = createMockMV(3L, 1L);
        MaterializedView mv4 = createMockMV(4L, 2L, 3L);
        List<MaterializedView> allMVs = Lists.newArrayList(mv4, mv3, mv2, mv1);
        
        List<MaterializedView> result = MaterializedViewDependencyGraph.buildTopologicalOrder(allMVs);
        
        Assertions.assertNotNull(result);
        Assertions.assertEquals(4, result.size());
        
        // mv1 should come before mv2 and mv3
        // mv2 and mv3 should come before mv4
        int idx1 = -1;
        int idx2 = -1;
        int idx3 = -1;
        int idx4 = -1;
        for (int i = 0; i < result.size(); i++) {
            long id = result.get(i).getId();
            if (id == 1L) {
                idx1 = i;
            }
            if (id == 2L) {
                idx2 = i;
            }
            if (id == 3L) {
                idx3 = i;
            }
            if (id == 4L) {
                idx4 = i;
            }
        }
        Assertions.assertTrue(idx1 < idx2, "mv1 should come before mv2");
        Assertions.assertTrue(idx1 < idx3, "mv1 should come before mv3");
        Assertions.assertTrue(idx2 < idx4, "mv2 should come before mv4");
        Assertions.assertTrue(idx3 < idx4, "mv3 should come before mv4");
    }

    @Test
    public void testCircularDependency() {
        // mv2 depends on mv1
        // mv1 depends on mv2 (circular)
        MaterializedView mv1 = createMockMV(1L, 2L);
        MaterializedView mv2 = createMockMV(2L, 1L);
        List<MaterializedView> allMVs = Lists.newArrayList(mv1, mv2);
        
        List<MaterializedView> result = MaterializedViewDependencyGraph.buildTopologicalOrder(allMVs);
        
        // Should return original order when cycle is detected
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(allMVs, result);
    }

    @Test
    public void testSelfDependency() {
        // mv1 depends on itself
        MaterializedView mv1 = createMockMV(1L, 1L);
        List<MaterializedView> allMVs = Lists.newArrayList(mv1);
        
        List<MaterializedView> result = MaterializedViewDependencyGraph.buildTopologicalOrder(allMVs);
        
        // Should return original order when cycle is detected
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(allMVs, result);
    }

    @Test
    public void testComplexGraph() {
        // Complex dependency graph:
        // mv1, mv2 are roots (no dependencies)
        // mv3 depends on mv1
        // mv4 depends on mv1
        // mv5 depends on mv2
        // mv6 depends on mv3 and mv4
        // mv7 depends on mv5 and mv6
        MaterializedView mv1 = createMockMV(1L);
        MaterializedView mv2 = createMockMV(2L);
        MaterializedView mv3 = createMockMV(3L, 1L);
        MaterializedView mv4 = createMockMV(4L, 1L);
        MaterializedView mv5 = createMockMV(5L, 2L);
        MaterializedView mv6 = createMockMV(6L, 3L, 4L);
        MaterializedView mv7 = createMockMV(7L, 5L, 6L);
        List<MaterializedView> allMVs = Lists.newArrayList(mv7, mv6, mv5, mv4, mv3, mv2, mv1);
        
        List<MaterializedView> result = MaterializedViewDependencyGraph.buildTopologicalOrder(allMVs);
        
        Assertions.assertNotNull(result);
        Assertions.assertEquals(7, result.size());
        
        // Get indices
        int idx1 = -1;
        int idx2 = -1;
        int idx3 = -1;
        int idx4 = -1;
        int idx5 = -1;
        int idx6 = -1;
        int idx7 = -1;
        for (int i = 0; i < result.size(); i++) {
            long id = result.get(i).getId();
            if (id == 1L) {
                idx1 = i;
            }
            if (id == 2L) {
                idx2 = i;
            }
            if (id == 3L) {
                idx3 = i;
            }
            if (id == 4L) {
                idx4 = i;
            }
            if (id == 5L) {
                idx5 = i;
            }
            if (id == 6L) {
                idx6 = i;
            }
            if (id == 7L) {
                idx7 = i;
            }
        }
        
        // Verify dependencies are respected
        Assertions.assertTrue(idx1 < idx3, "mv1 should come before mv3");
        Assertions.assertTrue(idx1 < idx4, "mv1 should come before mv4");
        Assertions.assertTrue(idx2 < idx5, "mv2 should come before mv5");
        Assertions.assertTrue(idx3 < idx6, "mv3 should come before mv6");
        Assertions.assertTrue(idx4 < idx6, "mv4 should come before mv6");
        Assertions.assertTrue(idx5 < idx7, "mv5 should come before mv7");
        Assertions.assertTrue(idx6 < idx7, "mv6 should come before mv7");
    }

    @Test
    public void testPartialCircularDependency() {
        // mv1, mv2, mv3 form a cycle
        // mv4 depends on mv1
        MaterializedView mv1 = createMockMV(1L, 3L);
        MaterializedView mv2 = createMockMV(2L, 1L);
        MaterializedView mv3 = createMockMV(3L, 2L);
        MaterializedView mv4 = createMockMV(4L, 1L);
        List<MaterializedView> allMVs = Lists.newArrayList(mv1, mv2, mv3, mv4);
        
        List<MaterializedView> result = MaterializedViewDependencyGraph.buildTopologicalOrder(allMVs);
        
        // Should return original order when cycle is detected
        Assertions.assertNotNull(result);
        Assertions.assertEquals(4, result.size());
        Assertions.assertEquals(allMVs, result);
    }

    @Test
    public void testMVDependsOnNonExistentMV() {
        // mv2 depends on mv999 which doesn't exist in the list
        MaterializedView mv1 = createMockMV(1L);
        MaterializedView mv2 = createMockMV(2L, 999L);
        MaterializedView mv3 = createMockMV(3L, 1L);
        List<MaterializedView> allMVs = Lists.newArrayList(mv1, mv2, mv3);
        
        List<MaterializedView> result = MaterializedViewDependencyGraph.buildTopologicalOrder(allMVs);
        
        Assertions.assertNotNull(result);
        Assertions.assertEquals(3, result.size());
        
        // mv2's dependency on non-existent mv999 is ignored
        // So mv2 should be treated as having no dependencies
        int idx1 = -1;
        int idx3 = -1;
        for (int i = 0; i < result.size(); i++) {
            long id = result.get(i).getId();
            if (id == 1L) {
                idx1 = i;
            }
            if (id == 3L) {
                idx3 = i;
            }
        }
        Assertions.assertTrue(idx1 < idx3, "mv1 should come before mv3");
    }

    @Test
    public void testMultipleDependenciesOnSameMV() {
        // mv3 and mv4 both depend on mv1
        // mv5 depends on mv2
        // mv6 depends on mv3, mv4, and mv5
        MaterializedView mv1 = createMockMV(1L);
        MaterializedView mv2 = createMockMV(2L);
        MaterializedView mv3 = createMockMV(3L, 1L);
        MaterializedView mv4 = createMockMV(4L, 1L);
        MaterializedView mv5 = createMockMV(5L, 2L);
        MaterializedView mv6 = createMockMV(6L, 3L, 4L, 5L);
        List<MaterializedView> allMVs = Lists.newArrayList(mv6, mv5, mv4, mv3, mv2, mv1);
        
        List<MaterializedView> result = MaterializedViewDependencyGraph.buildTopologicalOrder(allMVs);
        
        Assertions.assertNotNull(result);
        Assertions.assertEquals(6, result.size());
        
        // Get indices
        int idx1 = -1;
        int idx2 = -1;
        int idx3 = -1;
        int idx4 = -1;
        int idx5 = -1;
        int idx6 = -1;
        for (int i = 0; i < result.size(); i++) {
            long id = result.get(i).getId();
            if (id == 1L) {
                idx1 = i;
            }
            if (id == 2L) {
                idx2 = i;
            }
            if (id == 3L) {
                idx3 = i;
            }
            if (id == 4L) {
                idx4 = i;
            }
            if (id == 5L) {
                idx5 = i;
            }
            if (id == 6L) {
                idx6 = i;
            }
        }
        
        // Verify all dependencies are respected
        Assertions.assertTrue(idx1 < idx3, "mv1 should come before mv3");
        Assertions.assertTrue(idx1 < idx4, "mv1 should come before mv4");
        Assertions.assertTrue(idx2 < idx5, "mv2 should come before mv5");
        Assertions.assertTrue(idx3 < idx6, "mv3 should come before mv6");
        Assertions.assertTrue(idx4 < idx6, "mv4 should come before mv6");
        Assertions.assertTrue(idx5 < idx6, "mv5 should come before mv6");
    }

    @Test
    public void testLongDependencyChain() {
        // Create a chain: mv1 <- mv2 <- mv3 <- mv4 <- mv5
        MaterializedView mv1 = createMockMV(1L);
        MaterializedView mv2 = createMockMV(2L, 1L);
        MaterializedView mv3 = createMockMV(3L, 2L);
        MaterializedView mv4 = createMockMV(4L, 3L);
        MaterializedView mv5 = createMockMV(5L, 4L);
        List<MaterializedView> allMVs = Lists.newArrayList(mv5, mv4, mv3, mv2, mv1);
        
        List<MaterializedView> result = MaterializedViewDependencyGraph.buildTopologicalOrder(allMVs);
        
        Assertions.assertNotNull(result);
        Assertions.assertEquals(5, result.size());
        
        // Verify the chain order
        List<Long> ids = new ArrayList<>();
        for (MaterializedView mv : result) {
            ids.add(mv.getId());
        }
        Assertions.assertTrue(ids.indexOf(1L) < ids.indexOf(2L));
        Assertions.assertTrue(ids.indexOf(2L) < ids.indexOf(3L));
        Assertions.assertTrue(ids.indexOf(3L) < ids.indexOf(4L));
        Assertions.assertTrue(ids.indexOf(4L) < ids.indexOf(5L));
    }

    @Test
    public void testDisconnectedComponents() {
        // Two separate chains:
        // Chain 1: mv1 <- mv2 <- mv3
        // Chain 2: mv4 <- mv5 <- mv6
        MaterializedView mv1 = createMockMV(1L);
        MaterializedView mv2 = createMockMV(2L, 1L);
        MaterializedView mv3 = createMockMV(3L, 2L);
        MaterializedView mv4 = createMockMV(4L);
        MaterializedView mv5 = createMockMV(5L, 4L);
        MaterializedView mv6 = createMockMV(6L, 5L);
        List<MaterializedView> allMVs = Lists.newArrayList(mv3, mv6, mv2, mv5, mv1, mv4);
        
        List<MaterializedView> result = MaterializedViewDependencyGraph.buildTopologicalOrder(allMVs);
        
        Assertions.assertNotNull(result);
        Assertions.assertEquals(6, result.size());
        
        // Get indices
        int idx1 = -1;
        int idx2 = -1;
        int idx3 = -1;
        int idx4 = -1;
        int idx5 = -1;
        int idx6 = -1;
        for (int i = 0; i < result.size(); i++) {
            long id = result.get(i).getId();
            if (id == 1L) {
                idx1 = i;
            }
            if (id == 2L) {
                idx2 = i;
            }
            if (id == 3L) {
                idx3 = i;
            }
            if (id == 4L) {
                idx4 = i;
            }
            if (id == 5L) {
                idx5 = i;
            }
            if (id == 6L) {
                idx6 = i;
            }
        }
        
        // Verify both chains are ordered correctly
        Assertions.assertTrue(idx1 < idx2 && idx2 < idx3, "Chain 1 should be ordered");
        Assertions.assertTrue(idx4 < idx5 && idx5 < idx6, "Chain 2 should be ordered");
    }

    @Test
    public void testNullBaseTableInfos() {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("c1", IntegerType.INT));

        MaterializedView mv1 = new MaterializedView(1L, 1L, "mv_1", columns, KeysType.DUP_KEYS,
                null, null, null);
        MaterializedView mv2 = new MaterializedView(2L, 1L, "mv_2", columns, KeysType.DUP_KEYS,
                null, null, null);

        // Set mv1's base table infos to null to test null handling
        mv1.setBaseTableInfos(null);

        List<MaterializedView> allMVs = Lists.newArrayList(mv1, mv2);
        
        // Should handle null/empty gracefully
        List<MaterializedView> result = MaterializedViewDependencyGraph.buildTopologicalOrder(allMVs);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2, result.size());
    }

    @Test
    public void testEmptyBaseTableInfos() {
        MaterializedView mv1 = createMockMV(1L);
        MaterializedView mv2 = createMockMV(2L);
        List<MaterializedView> allMVs = Lists.newArrayList(mv1, mv2);
        
        List<MaterializedView> result = MaterializedViewDependencyGraph.buildTopologicalOrder(allMVs);
        
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2, result.size());
    }

    @Test
    public void testComplexCycleDetection() {
        // Create a more complex cycle: mv1 -> mv2 -> mv3 -> mv4 -> mv2
        MaterializedView mv1 = createMockMV(1L, 2L);
        MaterializedView mv2 = createMockMV(2L, 3L);
        MaterializedView mv3 = createMockMV(3L, 4L);
        MaterializedView mv4 = createMockMV(4L, 2L);
        MaterializedView mv5 = createMockMV(5L, 1L);
        List<MaterializedView> allMVs = Lists.newArrayList(mv1, mv2, mv3, mv4, mv5);
        
        List<MaterializedView> result = MaterializedViewDependencyGraph.buildTopologicalOrder(allMVs);
        
        // Should return original order when cycle is detected
        Assertions.assertNotNull(result);
        Assertions.assertEquals(5, result.size());
        Assertions.assertEquals(allMVs, result);
    }

    @Test
    public void testLayersEmptyList() {
        List<List<MaterializedView>> layers = MaterializedViewDependencyGraph.buildTopologicalLayers(new ArrayList<>());

        Assertions.assertNotNull(layers);
        Assertions.assertTrue(layers.isEmpty());
    }

    @Test
    public void testLayersSingleChain() {
        // mv3 depends on mv2, mv2 depends on mv1
        MaterializedView mv1 = createMockMV(1L);
        MaterializedView mv2 = createMockMV(2L, 1L);
        MaterializedView mv3 = createMockMV(3L, 2L);
        List<MaterializedView> allMVs = Lists.newArrayList(mv3, mv2, mv1);

        List<List<MaterializedView>> layers = MaterializedViewDependencyGraph.buildTopologicalLayers(allMVs);

        Assertions.assertEquals(List.of(List.of(1L), List.of(2L), List.of(3L)), toLayerIds(layers));
    }

    @Test
    public void testLayersDiamond() {
        // mv4 depends on mv2 and mv3, both of which depend on mv1
        MaterializedView mv1 = createMockMV(1L);
        MaterializedView mv2 = createMockMV(2L, 1L);
        MaterializedView mv3 = createMockMV(3L, 1L);
        MaterializedView mv4 = createMockMV(4L, 2L, 3L);
        List<MaterializedView> allMVs = Lists.newArrayList(mv4, mv3, mv2, mv1);

        List<List<MaterializedView>> layers = MaterializedViewDependencyGraph.buildTopologicalLayers(allMVs);

        // mv3 precedes mv2 in the input, so it also precedes it inside the layer
        Assertions.assertEquals(List.of(List.of(1L), List.of(3L, 2L), List.of(4L)), toLayerIds(layers));
    }

    @Test
    public void testLayersCrossDatabase() {
        // mv1001 lives in db 10, mv2001 in db 20 is built on mv1001, mv3001 in db 30 is built on mv2001.
        // Table ids are globally unique, so the edges hold across databases.
        MaterializedView mv1001 = createMV(1001L, 10L, new ArrayList<>());
        MaterializedView mv2001 = createMV(2001L, 20L,
                Lists.newArrayList(new BaseTableInfo(10L, "db1", "mv_1001", 1001L)));
        MaterializedView mv3001 = createMV(3001L, 30L,
                Lists.newArrayList(new BaseTableInfo(20L, "db2", "mv_2001", 2001L)));
        List<MaterializedView> allMVs = Lists.newArrayList(mv3001, mv2001, mv1001);

        List<List<MaterializedView>> layers = MaterializedViewDependencyGraph.buildTopologicalLayers(allMVs);

        Assertions.assertEquals(List.of(List.of(1001L), List.of(2001L), List.of(3001L)), toLayerIds(layers));
    }

    @Test
    public void testLayersIgnoreExternalTable() {
        // An external base table carries no internal table id, so it never produces an edge
        BaseTableInfo externalTable = new BaseTableInfo("hive_catalog", "hive_db", "hive_tbl", "hive_tbl:uuid");
        Assertions.assertEquals(-1L, externalTable.getTableId());

        // mv1 is built on the external table only, mv2 on the external table and mv1, mv3 has no base table
        MaterializedView mv1 = createMV(1L, 1L, Lists.newArrayList(externalTable));
        MaterializedView mv2 = createMV(2L, 1L,
                Lists.newArrayList(externalTable, new BaseTableInfo(1L, "db", "mv_1", 1L)));
        MaterializedView mv3 = createMockMV(3L);
        List<MaterializedView> allMVs = Lists.newArrayList(mv2, mv1, mv3);

        List<List<MaterializedView>> layers = MaterializedViewDependencyGraph.buildTopologicalLayers(allMVs);

        Assertions.assertEquals(List.of(List.of(1L, 3L), List.of(2L)), toLayerIds(layers));
    }

    @Test
    public void testLayersIgnoreViewInBetween() {
        // mv2 is built on view_100, which is defined on mv1. The view id is not an MV id, so there is no edge
        // from mv1 to mv2 and both stay in the first layer, in input order.
        MaterializedView mv1 = createMockMV(1L);
        MaterializedView mv2 = createMV(2L, 1L, Lists.newArrayList(new BaseTableInfo(1L, "db", "view_100", 100L)));
        List<MaterializedView> allMVs = Lists.newArrayList(mv2, mv1);

        List<List<MaterializedView>> layers = MaterializedViewDependencyGraph.buildTopologicalLayers(allMVs);

        Assertions.assertEquals(List.of(List.of(2L, 1L)), toLayerIds(layers));
    }

    @Test
    public void testLayersNullBaseTableInfos() {
        // An MV without base table infos has no predecessor and lands in the first layer
        MaterializedView mv1 = createMV(1L, 1L, null);
        MaterializedView mv2 = createMockMV(2L, 1L);
        MaterializedView mv3 = createMV(3L, 1L, null);
        List<MaterializedView> allMVs = Lists.newArrayList(mv2, mv3, mv1);

        List<List<MaterializedView>> layers = MaterializedViewDependencyGraph.buildTopologicalLayers(allMVs);

        Assertions.assertEquals(List.of(List.of(3L, 1L), List.of(2L)), toLayerIds(layers));
    }

    @Test
    public void testLayersCycleReturnsSingleLayer() {
        // mv1, mv2, mv3 form a cycle, mv4 depends on mv1 and mv5 is independent
        MaterializedView mv1 = createMockMV(1L, 3L);
        MaterializedView mv2 = createMockMV(2L, 1L);
        MaterializedView mv3 = createMockMV(3L, 2L);
        MaterializedView mv4 = createMockMV(4L, 1L);
        MaterializedView mv5 = createMockMV(5L);
        List<MaterializedView> allMVs = Lists.newArrayList(mv4, mv1, mv5, mv2, mv3);

        List<List<MaterializedView>> layers = MaterializedViewDependencyGraph.buildTopologicalLayers(allMVs);

        // a cycle cannot be layered: everything falls back to one layer in input order
        Assertions.assertEquals(1, layers.size());
        Assertions.assertEquals(allMVs, layers.get(0));
        Assertions.assertEquals(allMVs, MaterializedViewDependencyGraph.buildTopologicalOrder(allMVs));
    }

    @Test
    public void testLayersDanglingIdIsNotCycle() {
        // mv999 is not part of the input (dropped, or not taking part in the activation): it is ignored
        // instead of being taken for a cycle, and the rest is layered as usual.
        MaterializedView mv1 = createMockMV(1L);
        MaterializedView mv2 = createMockMV(2L, 999L);
        MaterializedView mv3 = createMockMV(3L, 2L);
        MaterializedView mv4 = createMockMV(4L, 999L, 1L);
        List<MaterializedView> allMVs = Lists.newArrayList(mv3, mv4, mv2, mv1);

        List<List<MaterializedView>> layers = MaterializedViewDependencyGraph.buildTopologicalLayers(allMVs);

        Assertions.assertEquals(List.of(List.of(2L, 1L), List.of(3L, 4L)), toLayerIds(layers));
    }

    @Test
    public void testLayersKeepInputOrderInsideLayer() {
        // The second layer is released in the order mv10, mv11, mv12 (following the first layer mv3, mv2, mv1),
        // but it must come out in input order: mv12, mv11, mv10.
        MaterializedView mv1 = createMockMV(1L);
        MaterializedView mv2 = createMockMV(2L);
        MaterializedView mv3 = createMockMV(3L);
        MaterializedView mv10 = createMockMV(10L, 3L);
        MaterializedView mv11 = createMockMV(11L, 2L);
        MaterializedView mv12 = createMockMV(12L, 1L);
        List<MaterializedView> allMVs = Lists.newArrayList(mv12, mv11, mv10, mv3, mv2, mv1);

        List<List<MaterializedView>> layers = MaterializedViewDependencyGraph.buildTopologicalLayers(allMVs);

        Assertions.assertEquals(List.of(List.of(3L, 2L, 1L), List.of(12L, 11L, 10L)), toLayerIds(layers));
    }

    @Test
    public void testTopologicalOrderIsFlattenedLayers() {
        // Same graph as testComplexGraph:
        // mv3, mv4 depend on mv1; mv5 depends on mv2; mv6 depends on mv3 and mv4; mv7 depends on mv5 and mv6
        MaterializedView mv1 = createMockMV(1L);
        MaterializedView mv2 = createMockMV(2L);
        MaterializedView mv3 = createMockMV(3L, 1L);
        MaterializedView mv4 = createMockMV(4L, 1L);
        MaterializedView mv5 = createMockMV(5L, 2L);
        MaterializedView mv6 = createMockMV(6L, 3L, 4L);
        MaterializedView mv7 = createMockMV(7L, 5L, 6L);
        List<MaterializedView> allMVs = Lists.newArrayList(mv7, mv6, mv5, mv4, mv3, mv2, mv1);

        List<List<MaterializedView>> layers = MaterializedViewDependencyGraph.buildTopologicalLayers(allMVs);
        List<MaterializedView> order = MaterializedViewDependencyGraph.buildTopologicalOrder(allMVs);

        Assertions.assertEquals(List.of(List.of(2L, 1L), List.of(5L, 4L, 3L), List.of(6L), List.of(7L)),
                toLayerIds(layers));
        List<MaterializedView> flattened = new ArrayList<>();
        for (List<MaterializedView> layer : layers) {
            flattened.addAll(layer);
        }
        Assertions.assertEquals(flattened, order);
    }
}
