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
import com.starrocks.planner.KuduScanNode;
import com.starrocks.planner.MetaScanNode;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.SchemaScanNode;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class ConnectorLocationWorkerRestrictorTest {

    @Test
    void testFixedLocationRequiredBackendIdsFromOlapAndMetaScan(
            @Mocked OlapScanNode olapScanNode,
            @Mocked MetaScanNode metaScanNode) {
        TScanRangeLocations olapRange = new TScanRangeLocations();
        TScanRangeLocation loc1 = new TScanRangeLocation();
        loc1.setBackend_id(11L);
        TScanRangeLocation loc2 = new TScanRangeLocation();
        loc2.setBackend_id(22L);
        olapRange.setLocations(Lists.newArrayList(loc1, loc2));

        TScanRangeLocations metaRange = new TScanRangeLocations();
        TScanRangeLocation metaLoc = new TScanRangeLocation();
        metaLoc.setBackend_id(33L);
        metaRange.setLocations(Lists.newArrayList(metaLoc));

        new Expectations() {
            {
                olapScanNode.isConnectorScanNode();
                result = false;
                minTimes = 0;
                olapScanNode.getScanRangeLocations(0);
                result = Lists.newArrayList(olapRange);
                metaScanNode.isConnectorScanNode();
                result = false;
                minTimes = 0;
                metaScanNode.getScanRangeLocations(0);
                result = Lists.newArrayList(metaRange);
            }
        };

        Set<Long> ids = ConnectorLocationWorkerRestrictor.computeFixedLocationRequiredBackendIds(
                Lists.newArrayList(olapScanNode, metaScanNode));
        Assertions.assertEquals(Set.of(11L, 22L, 33L), ids);
    }

    @Test
    void testFixedLocationRequiredBackendIdsIncludesSchemaAndKudu(
            @Mocked SchemaScanNode schemaScanNode,
            @Mocked KuduScanNode kuduScanNode,
            @Mocked IcebergScanNode icebergScanNode) {
        TScanRangeLocations schemaRange = new TScanRangeLocations();
        TScanRangeLocation schemaLoc = new TScanRangeLocation();
        schemaLoc.setBackend_id(44L);
        schemaRange.setLocations(Lists.newArrayList(schemaLoc));

        TScanRangeLocations kuduRange = new TScanRangeLocations();
        TScanRangeLocation kuduLoc = new TScanRangeLocation();
        kuduLoc.setBackend_id(55L);
        kuduRange.setLocations(Lists.newArrayList(kuduLoc));

        new Expectations() {
            {
                schemaScanNode.isConnectorScanNode();
                result = false;
                minTimes = 0;
                schemaScanNode.getScanRangeLocations(0);
                result = Lists.newArrayList(schemaRange);
                kuduScanNode.isConnectorScanNode();
                result = false;
                minTimes = 0;
                kuduScanNode.getScanRangeLocations(0);
                result = Lists.newArrayList(kuduRange);
                // Connector scans must not contribute to N.
                icebergScanNode.isConnectorScanNode();
                result = true;
                minTimes = 0;
            }
        };

        Set<Long> ids = ConnectorLocationWorkerRestrictor.computeFixedLocationRequiredBackendIds(
                Lists.newArrayList(schemaScanNode, kuduScanNode, icebergScanNode));
        Assertions.assertEquals(Set.of(44L, 55L), ids);
    }

    @Test
    void testResolveUnrestrictedWhenNoConnectorLocation(
            @Mocked OlapScanNode olapScanNode,
            @Mocked SystemInfoService clusterInfo) {
        new Expectations() {
            {
                olapScanNode.isConnectorScanNode();
                result = false;
                minTimes = 0;
            }
        };
        ConnectorLocationWorkerRestrictor.Restriction restriction = ConnectorLocationWorkerRestrictor
                .resolve(Lists.newArrayList(olapScanNode), clusterInfo, null);
        Assertions.assertFalse(restriction.isRestricted());
    }

    @Test
    void testResolveUnrestrictedForNullOrEmptyScanNodes(@Mocked SystemInfoService clusterInfo) {
        Assertions.assertFalse(
                ConnectorLocationWorkerRestrictor.resolve(null, clusterInfo, null).isRestricted());
        Assertions.assertFalse(
                ConnectorLocationWorkerRestrictor.resolve(Collections.emptyList(), clusterInfo, null).isRestricted());
    }

    @Test
    void testCollectIgnoresBlankInvalidAndMissingCatalog(
            @Mocked IcebergScanNode blankLocScan,
            @Mocked IcebergScanNode invalidLocScan,
            @Mocked IcebergScanNode missingCatalogScan,
            @Mocked IcebergScanNode nullTableScan,
            @Mocked TupleDescriptor blankDesc,
            @Mocked TupleDescriptor invalidDesc,
            @Mocked TupleDescriptor missingDesc,
            @Mocked TupleDescriptor nullTableDesc,
            @Mocked IcebergTable blankTable,
            @Mocked IcebergTable invalidTable,
            @Mocked IcebergTable missingTable,
            @Mocked CatalogMgr catalogMgr) {
        Map<String, String> blankCfg = new HashMap<>();
        blankCfg.put("type", "iceberg");
        blankCfg.put(PropertyAnalyzer.PROPERTIES_LABELS_LOCATION, "  ");
        Catalog blankCatalog = new Catalog(1, "blank_cat", blankCfg, "");

        Map<String, String> invalidCfg = new HashMap<>();
        invalidCfg.put("type", "iceberg");
        invalidCfg.put(PropertyAnalyzer.PROPERTIES_LABELS_LOCATION, "not-a-valid-location");
        Catalog invalidCatalog = new Catalog(2, "bad_cat", invalidCfg, "");

        new Expectations() {
            {
                nullTableScan.isConnectorScanNode();
                result = true;
                nullTableScan.getDesc();
                result = nullTableDesc;
                nullTableDesc.getTable();
                result = null;

                blankLocScan.isConnectorScanNode();
                result = true;
                blankLocScan.getDesc();
                result = blankDesc;
                blankDesc.getTable();
                result = blankTable;
                blankTable.getCatalogName();
                result = "blank_cat";
                CatalogMgr.isExternalCatalog("blank_cat");
                result = true;
                catalogMgr.getCatalogByName("blank_cat");
                result = blankCatalog;

                invalidLocScan.isConnectorScanNode();
                result = true;
                invalidLocScan.getDesc();
                result = invalidDesc;
                invalidDesc.getTable();
                result = invalidTable;
                invalidTable.getCatalogName();
                result = "bad_cat";
                CatalogMgr.isExternalCatalog("bad_cat");
                result = true;
                catalogMgr.getCatalogByName("bad_cat");
                result = invalidCatalog;

                missingCatalogScan.isConnectorScanNode();
                result = true;
                missingCatalogScan.getDesc();
                result = missingDesc;
                missingDesc.getTable();
                result = missingTable;
                missingTable.getCatalogName();
                result = "missing_cat";
                CatalogMgr.isExternalCatalog("missing_cat");
                result = true;
                catalogMgr.getCatalogByName("missing_cat");
                result = null;
            }
        };

        Multimap<String, String> locs = ConnectorLocationWorkerRestrictor.collectConnectorCatalogLocations(
                Lists.newArrayList(nullTableScan, blankLocScan, invalidLocScan, missingCatalogScan), catalogMgr);
        Assertions.assertTrue(locs.isEmpty());
    }

    @Test
    void testFixedLocationSkipsNullRanges(@Mocked OlapScanNode olapScanNode) {
        new Expectations() {
            {
                olapScanNode.isConnectorScanNode();
                result = false;
                minTimes = 0;
                olapScanNode.getScanRangeLocations(0);
                result = null;
            }
        };
        Assertions.assertTrue(ConnectorLocationWorkerRestrictor.computeFixedLocationRequiredBackendIds(
                Lists.newArrayList(olapScanNode)).isEmpty());
    }

    @Test
    void testResolveDisabledByConfig(
            @Mocked IcebergScanNode icebergScanNode,
            @Mocked SystemInfoService clusterInfo) {
        boolean saved = Config.enable_connector_location_worker_restriction;
        Config.enable_connector_location_worker_restriction = false;
        try {
            ConnectorLocationWorkerRestrictor.Restriction restriction = ConnectorLocationWorkerRestrictor.resolve(
                    Lists.newArrayList(icebergScanNode), clusterInfo, null);
            Assertions.assertFalse(restriction.isRestricted());
        } finally {
            Config.enable_connector_location_worker_restriction = saved;
        }
    }

    @Test
    void testResolveFallbackWhenNoAliveMatch(
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

        ConnectorLocationWorkerRestrictor.Restriction restriction = ConnectorLocationWorkerRestrictor.resolve(
                Lists.newArrayList(icebergScanNode), clusterInfo, catalogMgr);
        Assertions.assertFalse(restriction.isRestricted());
    }

    @Test
    void testResolveFallbackWhenLabelMatchedButNotSchedulableOnProvider(
            @Mocked IcebergScanNode icebergScanNode,
            @Mocked TupleDescriptor tupleDescriptor,
            @Mocked IcebergTable icebergTable,
            @Mocked CatalogMgr catalogMgr,
            @Mocked SystemInfoService clusterInfo,
            @Mocked WorkerProvider workerProvider) {
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
                CatalogMgr.isExternalCatalog("ice_cat");
                result = true;
                catalogMgr.getCatalogByName("ice_cat");
                result = catalog;
                clusterInfo.getLocationMatchedBackendIds((Multimap<String, String>) any);
                result = Lists.newArrayList(1L);
                // Label-matched but excluded from captured provider (e.g. blacklisted).
                workerProvider.getWorkerById(1L);
                result = null;
                minTimes = 0;
            }
        };

        ConnectorLocationWorkerRestrictor.Restriction restriction = ConnectorLocationWorkerRestrictor.resolve(
                Lists.newArrayList(icebergScanNode), clusterInfo, catalogMgr, workerProvider);
        Assertions.assertFalse(restriction.isRestricted());
    }

    @Test
    void testResolveRestrictedWhenProviderHasLabelMatched(
            @Mocked IcebergScanNode icebergScanNode,
            @Mocked TupleDescriptor tupleDescriptor,
            @Mocked IcebergTable icebergTable,
            @Mocked CatalogMgr catalogMgr,
            @Mocked SystemInfoService clusterInfo,
            @Mocked WorkerProvider workerProvider) {
        Map<String, String> config = new HashMap<>();
        config.put("type", "iceberg");
        config.put(PropertyAnalyzer.PROPERTIES_LABELS_LOCATION, "rack:r1");
        Catalog catalog = new Catalog(1, "ice_cat", config, "");
        ComputeNode schedulable = new ComputeNode(1L, "host1", 9050);
        schedulable.setAlive(true);

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
                workerProvider.getWorkerById(1L);
                result = schedulable;
                minTimes = 0;
            }
        };

        ConnectorLocationWorkerRestrictor.Restriction restriction = ConnectorLocationWorkerRestrictor.resolve(
                Lists.newArrayList(icebergScanNode), clusterInfo, catalogMgr, workerProvider);
        Assertions.assertTrue(restriction.isRestricted());
        Assertions.assertEquals(Set.of(1L), restriction.getPreferredConnectorWorkerIds());
    }

    @Test
    void testResolveRestrictedWithNativeUnion(
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

        ConnectorLocationWorkerRestrictor.Restriction restriction = ConnectorLocationWorkerRestrictor.resolve(
                Lists.newArrayList(icebergScanNode, olapScanNode), clusterInfo, catalogMgr);
        Assertions.assertTrue(restriction.isRestricted());
        Assertions.assertEquals(Set.of(1L), restriction.getPreferredConnectorWorkerIds());
        Assertions.assertEquals(Set.of(1L, 99L), restriction.getAllowedWorkerIds());
    }

    @Test
    void testLocationRestrictedWorkerProviderPreferredSubset() {
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

    @Test
    void testLocationRestrictedWorkerProviderRotatesAcrossInstances() throws Exception {
        Map<Long, ComputeNode> nodes = new HashMap<>();
        for (long i = 0; i < 3; i++) {
            ComputeNode node = new ComputeNode(i, "192.168.1." + i, 9030);
            node.setAlive(true);
            nodes.put(i, node);
        }
        ImmutableMap<Long, ComputeNode> map = ImmutableMap.copyOf(nodes);
        Set<Long> allowed = Set.of(0L, 1L, 2L);
        Set<Long> preferred = Set.of(0L, 1L);

        List<Long> firstPicks = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            DefaultWorkerProvider base = new DefaultWorkerProvider(
                    map, ImmutableMap.of(), map, ImmutableMap.of(), false, WarehouseManager.DEFAULT_RESOURCE);
            WorkerProvider restricted = new LocationRestrictedWorkerProvider(base, allowed, preferred);
            firstPicks.add(restricted.selectNextWorker());
        }
        // Process-wide cursor must advance across fresh wrappers; not always
        // allowedWorkers[0].
        Assertions.assertEquals(3, Set.copyOf(firstPicks).size());
    }
}
