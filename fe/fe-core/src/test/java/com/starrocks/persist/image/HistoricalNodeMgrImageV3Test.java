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

package com.starrocks.persist.image;

import com.starrocks.common.Pair;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.proto.HistoricalNodePB;
import com.starrocks.persist.proto.HistoricalNodeSetPB;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.persist.proto.Int64ListPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.HistoricalNodeMgr;
import com.starrocks.system.HistoricalNodeSet;
import com.starrocks.warehouse.Warehouse;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class HistoricalNodeMgrImageV3Test {
    @TempDir
    Path tempDir;
    private GlobalStateMgr global;

    @BeforeEach
    public void setUp() {
        global = LoadImageTestSupport.global();
        WarehouseManager warehouses = mock(WarehouseManager.class);
        Warehouse legacy = mock(Warehouse.class);
        when(legacy.getId()).thenReturn(17L);
        when(warehouses.getWarehouseAllowNull("legacyWarehouse")).thenReturn(legacy);
        when(global.getWarehouseMgr()).thenReturn(warehouses);
    }

    private void allowResourcesExcept(String excluded) {
        new MockUp<HistoricalNodeMgr>() {
            @Mock
            public boolean isResourceAvailable(String key) {
                return !key.equals(excluded);
            }
        };
    }

    static HistoricalNodePB entry(String key, long time) {
        return HistoricalNodePB.newBuilder().setComputeResourceKey(key)
                .setNodeSet(HistoricalNodeSetPB.newBuilder().setLastUpdateTime(time)
                        .setBackendIds(Int64ListPB.newBuilder().addAllValues(List.of(9L, 3L, 9L)))
                        .setComputeNodeIds(Int64ListPB.newBuilder().addAllValues(List.of(20L, 11L)))).build();
    }

    @Test
    public void testAllNodeSetFieldsAndListOrder() {
        HistoricalNodeSet source = HistoricalNodeSet.fromProto(entry("1-2", 123456789L).getNodeSet());
        FinalManagersImageTestSupport.assertAllFieldsPresent(source);
        HistoricalNodeSet restored = HistoricalNodeSet.fromProto(source.toProto());
        GsonEquivalence.assertEquivalent(source, restored);
        assertEquals(List.of(9L, 3L, 9L), restored.getHistoricalBackendIds());
        assertEquals(List.of(20L, 11L), restored.getHistoricalComputeNodeIds());
        assertEquals(123456789L, restored.getLastUpdateTime());
        restored.updateHistoricalBackendIds(List.of(101L), 200L);
        assertEquals(List.of(101L), restored.getHistoricalBackendIds());
        assertEquals(List.of(20L, 11L), restored.getHistoricalComputeNodeIds());
        assertEquals(200L, restored.getLastUpdateTime());
        assertEquals(List.of(9L, 3L, 9L), source.getHistoricalBackendIds());
        restored.updateHistoricalComputeNodeIds(List.of(202L), 300L);
        assertEquals(List.of(202L), restored.getHistoricalComputeNodeIds());
        assertEquals(300L, restored.getLastUpdateTime());
    }

    @Test
    public void testNodeSetNullAndEmptyCollections() {
        HistoricalNodeSet absent = HistoricalNodeSet.fromProto(HistoricalNodeSetPB.getDefaultInstance());
        assertNull(getField(absent, "backendIds"));
        assertNull(getField(absent, "computeNodeIds"));
        assertFalse(absent.toProto().hasBackendIds());
        assertEquals(0L, absent.getLastUpdateTime());
        HistoricalNodeSet empty = HistoricalNodeSet.fromProto(HistoricalNodeSetPB.newBuilder()
                .setBackendIds(Int64ListPB.getDefaultInstance()).setComputeNodeIds(Int64ListPB.getDefaultInstance()).build());
        assertTrue(empty.getHistoricalBackendIds().isEmpty());
        assertTrue(empty.getHistoricalComputeNodeIds().isEmpty());
        assertTrue(empty.toProto().hasBackendIds());
        assertTrue(empty.toProto().hasComputeNodeIds());
    }

    @Test
    public void testV2AndV3ApplySameSaveFilter() throws Exception {
        allowResourcesExcept("3-4");
        HistoricalNodeMgr source = new HistoricalNodeMgr();
        source.updateHistoricalBackendIds(1, 2, List.of(1L, 3L), 500);
        source.updateHistoricalComputeNodeIds(1, 2, List.of(8L), 600);
        source.updateHistoricalBackendIds(3, 4, List.of(9L), 700);
        HistoricalNodeMgr expected = new HistoricalNodeMgr();
        FinalManagersImageTestSupport.v2(source::save, expected::load);
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        HistoricalNodeMgr restored = new HistoricalNodeMgr();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertEquals(350, sections.get(0).getType());
        assertEquals(1, sections.get(0).getNumEntries());
        assertEquals(0, sections.get(0).getManagerLength());
        assertEquals(expected.getAllHistoricalNodeSet().keySet(), restored.getAllHistoricalNodeSet().keySet());
        GsonEquivalence.assertEquivalent(expected.getAllHistoricalNodeSet().get(Pair.create(1L, 2L)),
                restored.getAllHistoricalNodeSet().get(Pair.create(1L, 2L)));
        assertTrue(restored.getHistoricalBackendIds(3, 4).isEmpty());
        assertEquals(600L, restored.getLastUpdateTime(1, 2));
        assertEquals(0L, restored.getLastUpdateTime(3, 4));

        verifyNoInteractions(global.getEditLog());
    }

    @Test
    public void testLegacyWarehouseNameAndModernResourceKey() throws Exception {
        Path path = FinalManagersImageTestSupport.entries(tempDir, SectionId.HISTORICAL_NODE_MGR,
                List.of(entry("legacyWarehouse", 100), entry("19-29", 200)));
        HistoricalNodeMgr restored = new HistoricalNodeMgr();
        LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertEquals(2, restored.getAllHistoricalNodeSet().size());
        assertEquals(100, restored.getLastUpdateTime(17, StarOSAgent.DEFAULT_WORKER_GROUP_ID));
        assertEquals(200, restored.getLastUpdateTime(19, 29));
        ConcurrentHashMap<String, HistoricalNodeSet> map = getField(restored, "computeResourceToNodeSet");
        assertFalse(map.containsKey("legacyWarehouse"));
        assertTrue(map.containsKey("17-" + StarOSAgent.DEFAULT_WORKER_GROUP_ID));

    }

    @Test
    public void testInvalidAndMissingWarehouseKeysDoNotAffectFollowingEntries() throws Exception {
        Path path = FinalManagersImageTestSupport.entries(tempDir, SectionId.HISTORICAL_NODE_MGR,
                List.of(entry("missingWarehouse", 10), entry("invalid-id", 20), entry("9-bad", 30),
                        entry("8-9", 40), entry("", 50), entry("10-11", 60)));
        HistoricalNodeMgr restored = new HistoricalNodeMgr();
        LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertEquals(2, restored.getAllHistoricalNodeSet().size());
        assertEquals(40, restored.getLastUpdateTime(8, 9));
        assertEquals(60, restored.getLastUpdateTime(10, 11));

    }

    @Test
    public void testDuplicateNormalizedKeyReplacesValueInFileOrder() throws Exception {
        String modern = "17-" + StarOSAgent.DEFAULT_WORKER_GROUP_ID;
        Path path = FinalManagersImageTestSupport.entries(tempDir, SectionId.HISTORICAL_NODE_MGR,
                List.of(entry("legacyWarehouse", 100), entry(modern, 200), entry("legacyWarehouse", 300)));
        HistoricalNodeMgr restored = new HistoricalNodeMgr();
        LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertEquals(1, restored.getAllHistoricalNodeSet().size());
        assertEquals(300, restored.getLastUpdateTime(17, StarOSAgent.DEFAULT_WORKER_GROUP_ID));

    }

    @Test
    public void testLargeResourceListIsFlattenedAndMutableAfterLoad() throws Exception {
        allowResourcesExcept("excluded");
        HistoricalNodeMgr source = new HistoricalNodeMgr();
        for (int i = 1; i <= 1024; i++) {
            source.updateHistoricalBackendIds(i, i + 1, List.of((long) i), i + 10);
            source.updateHistoricalComputeNodeIds(i, i + 1, List.of(i + 1000L), i + 20);
        }
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        HistoricalNodeMgr restored = new HistoricalNodeMgr();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertEquals(1024, sections.get(0).getNumEntries());
        assertEquals(source.getAllHistoricalNodeSet().keySet(), restored.getAllHistoricalNodeSet().keySet());
        assertEquals(List.of(1024L), restored.getHistoricalBackendIds(1024, 1025));
        restored.updateHistoricalBackendIds(1024, 1025, List.of(9999L), 7777);
        assertEquals(List.of(9999L), restored.getHistoricalBackendIds(1024, 1025));
        assertEquals(7777, restored.getLastUpdateTime(1024, 1025));
        restored.clear();
        assertTrue(restored.getAllHistoricalNodeSet().isEmpty());

    }

    @Test
    public void testEmptyImageKeepsExistingNodeSetsLikeV2() throws Exception {
        allowResourcesExcept("excluded");
        HistoricalNodeMgr restored = new HistoricalNodeMgr();
        restored.updateHistoricalBackendIds(1, 2, List.of(3L), 4);
        Path path = LoadImageTestSupport.save(tempDir, new HistoricalNodeMgr()::save);
        LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertEquals(List.of(3L), restored.getHistoricalBackendIds(1, 2));
        assertEquals(4, restored.getLastUpdateTime(1, 2));

    }
}
