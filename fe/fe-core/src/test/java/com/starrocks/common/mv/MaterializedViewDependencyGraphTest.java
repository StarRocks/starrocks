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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.Type;
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
     * Helper method to create a MaterializedView with given id and dependencies
     */
    private MaterializedView createMockMV(long id, Long... dependencies) {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("c1", Type.INT));
        
        MaterializedView mv = new MaterializedView(id, 1L, "mv_" + id, columns, KeysType.DUP_KEYS,
                null, null, null);
        
        // Set dependencies
        Set<MvId> mvIds = new HashSet<>();
        for (Long depId : dependencies) {
            mvIds.add(new MvId(1L, depId));
        }
        mv.getRelatedMaterializedViews().addAll(mvIds);
        
        return mv;
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
    public void testNullRelatedMaterializedViews() {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("c1", Type.INT));
        
        MaterializedView mv1 = new MaterializedView(1L, 1L, "mv_1", columns, KeysType.DUP_KEYS,
                null, null, null);
        MaterializedView mv2 = new MaterializedView(2L, 1L, "mv_2", columns, KeysType.DUP_KEYS,
                null, null, null);
        
        // Set mv1's related MVs to null to test null handling
        mv1.getRelatedMaterializedViews().clear();
        
        List<MaterializedView> allMVs = Lists.newArrayList(mv1, mv2);
        
        // Should handle null/empty gracefully
        List<MaterializedView> result = MaterializedViewDependencyGraph.buildTopologicalOrder(allMVs);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2, result.size());
    }

    @Test
    public void testEmptyRelatedMaterializedViews() {
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
}
