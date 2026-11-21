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

import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.ResourceGroupMgr;
import com.starrocks.persist.ResourceGroupOpEntry;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.persist.proto.ResourceGroupMgrPB;
import com.starrocks.persist.proto.ResourceGroupPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.thrift.TWorkGroupOp;
import com.starrocks.thrift.TWorkGroupOpType;
import com.starrocks.thrift.TWorkGroupType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ResourceGroupMgrImageV3Test {
    @TempDir
    Path dir;

    @BeforeEach
    public void setUp() {
        GlobalStateMgr global = LoadImageTestSupport.global();
        NodeMgr nodes = Mockito.mock(NodeMgr.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(global.getNodeMgr()).thenReturn(nodes);
        Mockito.when(nodes.getClusterInfo().getBackendOrComputeNode(Mockito.anyLong())).thenReturn(null);
    }

    static ResourceGroupMgr populated() {
        ResourceGroupMgr source = new ResourceGroupMgr();
        ResourceGroup normal = ResourceGroupProtoTest.group(11, 300);
        ResourceGroup shortQuery = ResourceGroupProtoTest.group(12, 100);
        shortQuery.setResourceGroupType(TWorkGroupType.WG_SHORT_QUERY);
        ResourceGroup defaultGroup = ResourceGroupProtoTest.group(13, 200);
        defaultGroup.setName(ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME);
        source.replayResourceGroupOp(new ResourceGroupOpEntry(TWorkGroupOpType.WORKGROUP_OP_CREATE, normal));
        source.replayResourceGroupOp(new ResourceGroupOpEntry(TWorkGroupOpType.WORKGROUP_OP_CREATE, shortQuery));
        source.replayResourceGroupOp(new ResourceGroupOpEntry(TWorkGroupOpType.WORKGROUP_OP_CREATE, defaultGroup));
        return source;
    }

    @Test
    public void testIndexesAndBackendOperationsMatchV2() throws Exception {
        ResourceGroupMgr source = populated();
        ResourceGroupMgr v2 = new ResourceGroupMgr();
        StatisticsSecurityTestSupport.v2(source::save, v2::load);
        ResourceGroupMgr loaded = new ResourceGroupMgr();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(
                LoadImageTestSupport.save(dir, source::save), loaded.getImageV3Loaders());
        assertEquals(3, sections.get(0).getNumEntries());
        StatisticsSecurityTestSupport.assertMaps(v2, loaded,
                "resourceGroupMap", "id2ResourceGroupMap", "classifierMap");
        GsonEquivalence.assertEquivalent(getField(v2, "shortQueryResourceGroup"),
                getField(loaded, "shortQueryResourceGroup"));
        assertTrue((boolean) getField(loaded, "hasCreatedDefaultResourceGroups"));
        List<TWorkGroupOp> operations = loaded.getResourceGroupsNeedToDeliver(123L);
        assertEquals(v2.getResourceGroupsNeedToDeliver(123L), operations);
        assertEquals(List.of(100L, 200L, 300L), operations.stream()
                .map(op -> op.getWorkgroup().getVersion()).collect(Collectors.toList()));
        assertTrue(operations.stream().allMatch(op -> op.getOp_type() == TWorkGroupOpType.WORKGROUP_OP_CREATE));
        loaded.saveActiveResourceGroupsForBe(123L,
                operations.stream().map(TWorkGroupOp::getWorkgroup).collect(Collectors.toList()));
        assertTrue(loaded.getResourceGroupsNeedToDeliver(123L).isEmpty());

    }

    @Test
    public void testVersionSortIsRetainedForUnsortedEntries() throws Exception {
        Path file = LoadImageTestSupport.entries(dir, SectionId.RESOURCE_GROUP_MGR,
                ResourceGroupMgrPB.getDefaultInstance(), List.of(ResourceGroupProtoTest.group(1, 9).toProto(),
                        ResourceGroupProtoTest.group(2, 1).toProto(), ResourceGroupProtoTest.group(3, 5).toProto()));
        ResourceGroupMgr loaded = new ResourceGroupMgr();
        LoadImageTestSupport.load(file, loaded.getImageV3Loaders());
        assertEquals(List.of(1L, 5L, 9L), loaded.getResourceGroupsNeedToDeliver(123L).stream()
                .map(op -> op.getWorkgroup().getVersion()).collect(Collectors.toList()));
    }

    @Test
    public void testEmptySection() throws Exception {
        ResourceGroupMgr source = new ResourceGroupMgr();
        ResourceGroupMgr loaded = new ResourceGroupMgr();
        List<IndexEntryPB> sections = LoadImageTestSupport.load(
                LoadImageTestSupport.save(dir, source::save), loaded.getImageV3Loaders());
        assertEquals(0, sections.get(0).getNumEntries());
        assertTrue(loaded.getResourceGroupsNeedToDeliver(123L).isEmpty());
        assertFalse((boolean) getField(loaded, "hasCreatedDefaultResourceGroups"));
        assertNull(getField(loaded, "shortQueryResourceGroup"));

    }

    @Test
    public void testConversionFailureDoesNotInstallEarlierEntries() throws Exception {
        Path file = LoadImageTestSupport.entries(dir, SectionId.RESOURCE_GROUP_MGR, ResourceGroupMgrPB.getDefaultInstance(),
                List.of(ResourceGroupProtoTest.group(1, 1).toProto(),
                        ResourceGroupPB.newBuilder().setResourceGroupTypeValue(999).build()));
        ResourceGroupMgr loaded = new ResourceGroupMgr();
        assertThrows(ImageFormatException.class, () -> LoadImageTestSupport.load(file, loaded.getImageV3Loaders()));
        assertTrue(loaded.getResourceGroupsNeedToDeliver(123L).isEmpty());
    }
}
