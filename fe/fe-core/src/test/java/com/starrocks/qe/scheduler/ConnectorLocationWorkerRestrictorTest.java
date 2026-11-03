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

package com.starrocks.qe.scheduler;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.Config;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ConnectorLocationWorkerRestrictorTest {

    @Test
    public void testNativeRequiredBackendIdsFromOlapScan(@Mocked OlapScanNode olapScanNode) {
        TScanRangeLocations range = new TScanRangeLocations();
        TScanRangeLocation loc1 = new TScanRangeLocation();
        loc1.setBackend_id(11L);
        TScanRangeLocation loc2 = new TScanRangeLocation();
        loc2.setBackend_id(22L);
        range.setLocations(Lists.newArrayList(loc1, loc2));

        new Expectations() {
            {
                olapScanNode.getScanRangeLocations(0);
                result = Lists.newArrayList(range);
            }
        };

        Set<Long> ids = ConnectorLocationWorkerRestrictor.computeNativeRequiredBackendIds(
                Lists.newArrayList(olapScanNode));
        Assertions.assertEquals(Set.of(11L, 22L), ids);
    }

    @Test
    public void testResolveUnrestrictedWhenNoConnectorLocation(
            @Mocked OlapScanNode olapScanNode,
            @Mocked SystemInfoService clusterInfo) {
        new Expectations() {
            {
                olapScanNode.isConnectorScanNode();
                result = false;
                minTimes = 0;
            }
        };
        ConnectorLocationWorkerRestrictor.Restriction restriction =
                ConnectorLocationWorkerRestrictor.resolve(Lists.newArrayList(olapScanNode), clusterInfo, null);
        Assertions.assertFalse(restriction.isRestricted());
    }

    @Test
    public void testResolveDisabledByConfig(
            @Mocked IcebergScanNode icebergScanNode,
            @Mocked SystemInfoService clusterInfo) {
        boolean saved = Config.enable_connector_location_worker_restriction;
        Config.enable_connector_location_worker_restriction = false;
        try {
            ConnectorLocationWorkerRestrictor.Restriction restriction =
                    ConnectorLocationWorkerRestrictor.resolve(
                            Lists.newArrayList(icebergScanNode), clusterInfo, null);
            Assertions.assertFalse(restriction.isRestricted());
        } finally {
            Config.enable_connector_location_worker_restriction = saved;
        }
    }

    @Test
    public void testResolveFallbackWhenNoAliveMatch(
            @Mocked IcebergScanNode icebergScanNode,
            @Mocked TupleDescriptor tupleDescriptor,
            @Mocked IcebergTable icebergTable,
            @Mocked CatalogMgr catalogMgr,
            @Mocked SystemInfoService clusterInfo) {
        Map<String, String> config = new HashMap<>();
        config.put("type", "iceberg");
        config.put(PropertyAnalyzer.PROPERTIES_LABELS_LOCATION, "rack:r1");
        Catalog catalog = new Catalog(1, "ice_cat", config, "");

        new Expectations() {
            {
                icebergScanNode.isConnectorScanNode();
                result = true;
                icebergScanNode.getDesc();
                result = tupleDescriptor;
                tupleDescriptor.getTable();
                result = icebergTable;
                icebergTable.getCatalogName();
                result = "ice_cat";
                // @Mocked CatalogMgr stubs statics; must stub isExternalCatalog explicitly.
                CatalogMgr.isExternalCatalog("ice_cat");
                result = true;
                catalogMgr.getCatalogByName("ice_cat");
                result = catalog;
                clusterInfo.getLocationMatchedBackendIds((Multimap<String, String>) any);
                result = Collections.emptyList();
            }
        };

        ConnectorLocationWorkerRestrictor.Restriction restriction =
                ConnectorLocationWorkerRestrictor.resolve(
                        Lists.newArrayList(icebergScanNode), clusterInfo, catalogMgr);
        Assertions.assertFalse(restriction.isRestricted());
    }

    @Test
    public void testResolveRestrictedWithNativeUnion(
            @Mocked IcebergScanNode icebergScanNode,
            @Mocked TupleDescriptor tupleDescriptor,
            @Mocked IcebergTable icebergTable,
            @Mocked OlapScanNode olapScanNode,
            @Mocked CatalogMgr catalogMgr,
            @Mocked SystemInfoService clusterInfo) {
        Map<String, String> config = new HashMap<>();
        config.put("type", "iceberg");
        config.put(PropertyAnalyzer.PROPERTIES_LABELS_LOCATION, "rack:r1");
        Catalog catalog = new Catalog(1, "ice_cat", config, "");

        Backend labelled = new Backend(1, "host1", 9050);
        labelled.setAlive(true);
        labelled.setLocation(Map.of("rack", "r1"));

        TScanRangeLocations range = new TScanRangeLocations();
        TScanRangeLocation loc = new TScanRangeLocation();
        loc.setBackend_id(99L);
        range.setLocations(Lists.newArrayList(loc));

        new Expectations() {
            {
                icebergScanNode.isConnectorScanNode();
                result = true;
                icebergScanNode.getDesc();
                result = tupleDescriptor;
                tupleDescriptor.getTable();
                result = icebergTable;
                icebergTable.getCatalogName();
                result = "ice_cat";
                CatalogMgr.isExternalCatalog("ice_cat");
                result = true;
                catalogMgr.getCatalogByName("ice_cat");
                result = catalog;
                clusterInfo.getLocationMatchedBackendIds((Multimap<String, String>) any);
                result = Lists.newArrayList(1L);
                clusterInfo.getBackend(1L);
                result = labelled;
                minTimes = 0;
                olapScanNode.isConnectorScanNode();
                result = false;
                minTimes = 0;
                olapScanNode.getScanRangeLocations(0);
                result = Lists.newArrayList(range);
            }
        };

        ConnectorLocationWorkerRestrictor.Restriction restriction =
                ConnectorLocationWorkerRestrictor.resolve(
                        Lists.newArrayList(icebergScanNode, olapScanNode), clusterInfo, catalogMgr);
        Assertions.assertTrue(restriction.isRestricted());
        Assertions.assertEquals(Set.of(1L), restriction.getPreferredConnectorWorkerIds());
        Assertions.assertEquals(Set.of(1L, 99L), restriction.getAllowedWorkerIds());
    }

    @Test
    public void testLocationRestrictedWorkerProviderPreferredSubset() throws Exception {
        Map<Long, ComputeNode> nodes = new HashMap<>();
        for (long i = 0; i < 3; i++) {
            ComputeNode node = new ComputeNode(i, "192.168.1." + i, 9030);
            node.setAlive(true);
            nodes.put(i, node);
        }
        ImmutableMap<Long, ComputeNode> map = ImmutableMap.copyOf(nodes);
        DefaultWorkerProvider base = new DefaultWorkerProvider(
                map, ImmutableMap.of(), map, ImmutableMap.of(), false, WarehouseManager.DEFAULT_RESOURCE);
        WorkerProvider restricted = new LocationRestrictedWorkerProvider(base, Set.of(0L, 1L, 2L), Set.of(0L, 1L));

        Assertions.assertEquals(3, restricted.getAllWorkers().size());
        Assertions.assertEquals(2, restricted.getPreferredConnectorWorkers().size());
        Set<Long> preferredIds = restricted.getPreferredConnectorWorkers().stream()
                .map(ComputeNode::getId)
                .collect(Collectors.toSet());
        Assertions.assertEquals(Set.of(0L, 1L), preferredIds);
        Assertions.assertNull(restricted.getWorkerById(5L));
        Assertions.assertNotNull(restricted.getWorkerById(2L));
    }
}
