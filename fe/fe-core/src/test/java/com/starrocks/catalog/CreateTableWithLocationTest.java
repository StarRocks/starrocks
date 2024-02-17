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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/CreateTableTest.java

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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.pseudocluster.PseudoBackend;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AlterSystemStmtAnalyzer;
import com.starrocks.system.Backend;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Test create table with location property and check whether
 * the replica is placed on the right backends with matching location label.
 */
public class CreateTableWithLocationTest {
    @BeforeClass
    public static void setUp() throws Exception {
        PseudoCluster.getOrCreateWithRandomPort(true, 7);
        GlobalStateMgr.getCurrentState().getTabletChecker().setInterval(1000);
        PseudoCluster cluster = PseudoCluster.getInstance();
        cluster.runSql(null, "create database test");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown(true);
    }

    private void clearBackendLocationProp() throws SQLException {
        PseudoCluster cluster = PseudoCluster.getInstance();
        for (PseudoBackend backend : cluster.getBackends()) {
            String stmtStr = "alter system modify backend '" + backend.getHostHeartbeatPort() + "' set ('" +
                    AlterSystemStmtAnalyzer.PROP_KEY_LOCATION + "' = '')";
            System.out.println(stmtStr);
            cluster.runSql(null, stmtStr);
        }
    }

    private void setBackendLocationProp(List<String> locations) throws SQLException {
        PseudoCluster cluster = PseudoCluster.getInstance();
        int i = 0;
        for (PseudoBackend backend : cluster.getBackends()) {
            String stmtStr = "alter system modify backend '" + backend.getHostHeartbeatPort() + "' set ('" +
                    AlterSystemStmtAnalyzer.PROP_KEY_LOCATION + "' = '" + locations.get(i++) +  "')";
            System.out.println(stmtStr);
            cluster.runSql(null, stmtStr);
        }
    }


    @Test
    public void testCreateTableAndBackendNoLocationProp() throws SQLException {
        clearBackendLocationProp();
        PseudoCluster cluster = PseudoCluster.getInstance();
        String sql = "CREATE TABLE test.`test_table_backend_no_loc` (\n" +
                "    k1 int,\n" +
                "    k2 VARCHAR NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 2\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"3\",\n" +
                "    \"in_memory\" = \"false\"\n" +
                ");";
        cluster.runSql("test", sql);

        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable table = (OlapTable) db.getTable("test_table_backend_no_loc");
        Assert.assertNull(table.getLocation());
    }

    private Set<Long> getBackendIdsWithLocProp() {
        PseudoCluster cluster = PseudoCluster.getInstance();
        Set<Long> backendIds = Sets.newHashSet();
        for (PseudoBackend pseudoBackend : cluster.getBackends()) {
            Backend backend = GlobalStateMgr.getCurrentState().getNodeMgr()
                    .getClusterInfo().getBackend(pseudoBackend.getId());
            if (!backend.getLocation().isEmpty()) {
                backendIds.add(backend.getId());
            }
        }
        return backendIds;
    }

    private Set<Long> getBackendIdsWithLocProp(String locationKey) {
        PseudoCluster cluster = PseudoCluster.getInstance();
        Set<Long> backendIds = Sets.newHashSet();
        for (PseudoBackend pseudoBackend : cluster.getBackends()) {
            Backend backend = GlobalStateMgr.getCurrentState().getNodeMgr()
                    .getClusterInfo().getBackend(pseudoBackend.getId());
            if (backend.getLocation().containsKey(locationKey)) {
                backendIds.add(backend.getId());
            }
        }
        return backendIds;
    }

    private Set<Long> getBackendIdsWithLocProp(String locationKey, String locationVal) {
        PseudoCluster cluster = PseudoCluster.getInstance();
        Set<Long> backendIds = Sets.newHashSet();
        for (PseudoBackend pseudoBackend : cluster.getBackends()) {
            Backend backend = GlobalStateMgr.getCurrentState().getNodeMgr()
                    .getClusterInfo().getBackend(pseudoBackend.getId());
            if (backend.getLocation().containsKey(locationKey) &&
                    Objects.equals(backend.getLocation().get(locationKey), locationVal)) {
                backendIds.add(backend.getId());
            }
        }
        return backendIds;
    }

    private Set<Long> getBackendIdsWithoutLocProp() {
        PseudoCluster cluster = PseudoCluster.getInstance();
        Set<Long> backendIds = Sets.newHashSet();
        for (PseudoBackend pseudoBackend : cluster.getBackends()) {
            Backend backend = GlobalStateMgr.getCurrentState().getNodeMgr()
                    .getClusterInfo().getBackend(pseudoBackend.getId());
            if (backend.getLocation().isEmpty()) {
                backendIds.add(backend.getId());
            }
        }
        return backendIds;
    }

    @Test
    public void testCreateTableNoLocPropBackendWithLocProp() throws SQLException {
        // last backend doesn't contain location property
        List<String> locations = Lists.newArrayList("rack:r1", "rack:rack2", "rack:rack3",
                "region:r1", "region:r2", "region:r3", "");
        setBackendLocationProp(locations);
        PseudoCluster cluster = PseudoCluster.getInstance();
        String sql = "CREATE TABLE test.`test_table_no_loc_backend_with_loc` (\n" +
                "    k1 int,\n" +
                "    k2 VARCHAR NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"3\",\n" +
                "    \"in_memory\" = \"false\"\n" +
                ");";
        cluster.runSql("test", sql);

        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable table = (OlapTable) db.getTable("test_table_no_loc_backend_with_loc");
        Assert.assertTrue(table.getLocation().keySet().contains("*"));

        List<Partition> partitions = new ArrayList<>(table.getAllPartitions());
        List<Tablet> tablets = partitions.get(0).getBaseIndex().getTablets();
        Set<Long> backendIdsWithLocProp = getBackendIdsWithLocProp();
        Set<Long> backendIdsWithoutLocProp = getBackendIdsWithoutLocProp();
        System.out.println(backendIdsWithLocProp);
        for (Tablet tablet : tablets) {
            List<Replica> replicas = tablet.getAllReplicas();
            Set<Long> replicaBackendIds = Sets.newHashSet();
            System.out.println(tablet.getId());
            for (Replica replica : replicas) {
                replicaBackendIds.add(replica.getBackendId());
            }
            System.out.println(replicaBackendIds);
            Assert.assertEquals(3, replicaBackendIds.size());
            Assert.assertEquals(3, Sets.intersection(replicaBackendIds, backendIdsWithLocProp).size());
            Assert.assertEquals(0, Sets.intersection(replicaBackendIds, backendIdsWithoutLocProp).size());
        }
    }

    @Test
    public void testCreateTableWithExplicitLocPropBackendWithLocProp() throws SQLException {
        // last backend doesn't contain location property
        List<String> locations = Lists.newArrayList("rack:r1", "rack:rack2", "rack:rack3",
                "region:r1", "region:r2", "region:r3", "");
        setBackendLocationProp(locations);
        PseudoCluster cluster = PseudoCluster.getInstance();

        // Test: rack:*
        String sql = "CREATE TABLE test.`test_table_explicit_loc_backend_with_loc1` (\n" +
                "    k1 int,\n" +
                "    k2 VARCHAR NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"3\",\n" +
                "    \"" + PropertyAnalyzer.PROPERTIES_LABELS_LOCATION +  "\" = \"rack:*\",\n" +
                "    \"in_memory\" = \"false\"\n" +
                ");";
        System.out.println(sql);
        cluster.runSql("test", sql);
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable table = (OlapTable) db.getTable("test_table_explicit_loc_backend_with_loc1");
        Assert.assertTrue(table.getLocation().keySet().contains("rack"));
        List<Partition> partitions = new ArrayList<>(table.getAllPartitions());
        List<Tablet> tablets = partitions.get(0).getBaseIndex().getTablets();
        Set<Long> backendIdsWithLocProp = getBackendIdsWithLocProp("rack");
        Set<Long> backendIdsWithoutLocProp = getBackendIdsWithoutLocProp();
        // test_table_explicit_loc_backend_with_loc1's replicas should only distribute on backends with rack location
        backendIdsWithoutLocProp.addAll(getBackendIdsWithLocProp("region"));
        System.out.println(backendIdsWithLocProp);
        for (Tablet tablet : tablets) {
            List<Replica> replicas = tablet.getAllReplicas();
            Set<Long> replicaBackendIds = Sets.newHashSet();
            System.out.println(tablet.getId());
            for (Replica replica : replicas) {
                replicaBackendIds.add(replica.getBackendId());
            }
            System.out.println(replicaBackendIds);
            Assert.assertEquals(3, replicaBackendIds.size());
            Assert.assertEquals(3, Sets.intersection(replicaBackendIds, backendIdsWithLocProp).size());
            Assert.assertEquals(0, Sets.intersection(replicaBackendIds, backendIdsWithoutLocProp).size());
        }

        // Test: rack:*, region:*
        sql = "CREATE TABLE test.`test_table_explicit_loc_backend_with_loc2` (\n" +
                "    k1 int,\n" +
                "    k2 VARCHAR NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"3\",\n" +
                "    \"" + PropertyAnalyzer.PROPERTIES_LABELS_LOCATION +  "\" = \"rack:*, region:*\",\n" +
                "    \"in_memory\" = \"false\"\n" +
                ");";
        cluster.runSql("test", sql);
        boolean hasReplicaOnRegionBackend = false;
        boolean hasReplicaOnRackBackend = false;
        table = (OlapTable) db.getTable("test_table_explicit_loc_backend_with_loc2");
        Assert.assertTrue(table.getLocation().keySet().contains("rack"));
        Assert.assertTrue(table.getLocation().keySet().contains("region"));
        partitions = new ArrayList<>(table.getAllPartitions());
        tablets = partitions.get(0).getBaseIndex().getTablets();
        for (Tablet tablet : tablets) {
            List<Replica> replicas = tablet.getAllReplicas();
            Set<Long> replicaBackendIds = Sets.newHashSet();
            System.out.println(tablet.getId());
            for (Replica replica : replicas) {
                replicaBackendIds.add(replica.getBackendId());
            }
            System.out.println(replicaBackendIds);
            Assert.assertEquals(3, replicaBackendIds.size());
            if (!Sets.intersection(replicaBackendIds, getBackendIdsWithLocProp("rack")).isEmpty()) {
                hasReplicaOnRackBackend = true;
            }
            if (!Sets.intersection(replicaBackendIds, getBackendIdsWithLocProp("region")).isEmpty()) {
                hasReplicaOnRegionBackend = true;
            }
        }
        Assert.assertTrue(hasReplicaOnRackBackend);
        Assert.assertTrue(hasReplicaOnRegionBackend);

        // Test: rack:r1, rack:rack2, rack:rack3
        sql = "CREATE TABLE test.`test_table_explicit_loc_backend_with_loc3` (\n" +
                "    k1 int,\n" +
                "    k2 VARCHAR NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"3\",\n" +
                "    \"" + PropertyAnalyzer.PROPERTIES_LABELS_LOCATION +  "\" = \"rack:r1, rack:rack2, rack:rack3\",\n" +
                "    \"in_memory\" = \"false\"\n" +
                ");";
        cluster.runSql("test", sql);
        table = (OlapTable) db.getTable("test_table_explicit_loc_backend_with_loc3");
        Assert.assertTrue(table.getLocation().keySet().contains("rack"));
        Assert.assertEquals(1, table.getLocation().keySet().size());
        partitions = new ArrayList<>(table.getAllPartitions());
        tablets = partitions.get(0).getBaseIndex().getTablets();
        backendIdsWithLocProp = getBackendIdsWithLocProp("rack", "r1");
        backendIdsWithLocProp.addAll(getBackendIdsWithLocProp("rack", "rack2"));
        backendIdsWithLocProp.addAll(getBackendIdsWithLocProp("rack", "rack3"));
        for (Tablet tablet : tablets) {
            List<Replica> replicas = tablet.getAllReplicas();
            Set<Long> replicaBackendIds = Sets.newHashSet();
            System.out.println(tablet.getId());
            for (Replica replica : replicas) {
                replicaBackendIds.add(replica.getBackendId());
            }
            System.out.println(replicaBackendIds);
            Assert.assertEquals(3, replicaBackendIds.size());
            Assert.assertEquals(3, Sets.intersection(replicaBackendIds, backendIdsWithLocProp).size());
        }

        // Test: rack:r1, region:*
        sql = "CREATE TABLE test.`test_table_explicit_loc_backend_with_loc4` (\n" +
                "    k1 int,\n" +
                "    k2 VARCHAR NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"3\",\n" +
                "    \"" + PropertyAnalyzer.PROPERTIES_LABELS_LOCATION +  "\" = \"rack:r1, region:*\",\n" +
                "    \"in_memory\" = \"false\"\n" +
                ");";
        cluster.runSql("test", sql);
        table = (OlapTable) db.getTable("test_table_explicit_loc_backend_with_loc4");
        Assert.assertTrue(table.getLocation().keySet().contains("rack"));
        Assert.assertTrue(table.getLocation().keySet().contains("region"));
        partitions = new ArrayList<>(table.getAllPartitions());
        tablets = partitions.get(0).getBaseIndex().getTablets();
        Set<Long> allReplicasBackendIds = Sets.newHashSet();
        for (Tablet tablet : tablets) {
            List<Replica> replicas = tablet.getAllReplicas();
            Set<Long> replicaBackendIds = Sets.newHashSet();
            System.out.println(tablet.getId());
            for (Replica replica : replicas) {
                replicaBackendIds.add(replica.getBackendId());
            }
            System.out.println(replicaBackendIds);
            Assert.assertEquals(3, replicaBackendIds.size());
            allReplicasBackendIds.addAll(replicaBackendIds);
        }
        backendIdsWithLocProp = getBackendIdsWithLocProp("rack", "r1");
        backendIdsWithLocProp.addAll(getBackendIdsWithLocProp("region"));
        System.out.println(backendIdsWithLocProp);
        System.out.println(allReplicasBackendIds);
        int size = Sets.intersection(allReplicasBackendIds, backendIdsWithLocProp).size();
        Assert.assertTrue(size >= 3);
        Assert.assertTrue(size <= 4);

        // Test: rack:r1, rack:rack2, not enough hosts, fallback to ignore location prop
        sql = "CREATE TABLE test.`test_table_explicit_loc_backend_with_loc5` (\n" +
                "    k1 int,\n" +
                "    k2 VARCHAR NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"3\",\n" +
                "    \"" + PropertyAnalyzer.PROPERTIES_LABELS_LOCATION +  "\" = \"rack:r1, rack:rack2\",\n" +
                "    \"in_memory\" = \"false\"\n" +
                ");";
        cluster.runSql("test", sql);
        table = (OlapTable) db.getTable("test_table_explicit_loc_backend_with_loc5");
        Assert.assertTrue(table.getLocation().keySet().contains("rack"));
        partitions = new ArrayList<>(table.getAllPartitions());
        tablets = partitions.get(0).getBaseIndex().getTablets();
        allReplicasBackendIds = Sets.newHashSet();
        for (Tablet tablet : tablets) {
            List<Replica> replicas = tablet.getAllReplicas();
            Set<Long> replicaBackendIds = Sets.newHashSet();
            System.out.println(tablet.getId());
            for (Replica replica : replicas) {
                replicaBackendIds.add(replica.getBackendId());
            }
            System.out.println(replicaBackendIds);
            Assert.assertEquals(3, replicaBackendIds.size());
            allReplicasBackendIds.addAll(replicaBackendIds);
        }
        backendIdsWithLocProp = getBackendIdsWithLocProp();
        backendIdsWithLocProp.addAll(getBackendIdsWithoutLocProp());
        Assert.assertEquals(backendIdsWithLocProp.size(),
                Sets.intersection(allReplicasBackendIds, backendIdsWithLocProp).size());

        // Test: rack:r1, rack:rack2, not enough hosts, fallback to ignore location prop
        sql = "CREATE TABLE test.`test_table_explicit_loc_backend_with_loc6` (\n" +
                "    k1 int,\n" +
                "    k2 VARCHAR NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"10\",\n" +
                "    \"" + PropertyAnalyzer.PROPERTIES_LABELS_LOCATION +  "\" = \"rack:r1, rack:rack2\",\n" +
                "    \"in_memory\" = \"false\"\n" +
                ");";

        try {
            cluster.runSql("test", sql);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(
                    "Table replication num should be less than of equal to the number of available BE nodes"));
        }

        // clean up
        clearBackendLocationProp();
    }

    @Test
    public void testBestEffortMatchLocationProp() throws SQLException {
        // last backend has the same location prop with the first one
        List<String> locations = Lists.newArrayList("rack:r1", "rack:rack2", "rack:rack3",
                "region:r1", "region:r2", "region:r3", "rack:r1");
        setBackendLocationProp(locations);
        PseudoCluster cluster = PseudoCluster.getInstance();

        // Test: rack:r1, rack:rack2, 3 hosts, but only 2 racks
        String sql = "CREATE TABLE test.`test_table_explicit_loc_backend_with_loc_best_effort` (\n" +
                "    k1 int,\n" +
                "    k2 VARCHAR NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"3\",\n" +
                "    \"" + PropertyAnalyzer.PROPERTIES_LABELS_LOCATION +  "\" = \"rack:r1, rack:rack2\",\n" +
                "    \"in_memory\" = \"false\"\n" +
                ");";
        System.out.println(sql);
        cluster.runSql("test", sql);
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable table = (OlapTable) db.getTable("test_table_explicit_loc_backend_with_loc_best_effort");
        Assert.assertTrue(table.getLocation().keySet().contains("rack"));
        List<Partition> partitions = new ArrayList<>(table.getAllPartitions());
        List<Tablet> tablets = partitions.get(0).getBaseIndex().getTablets();
        Set<Long> backendIdsWithLocProp = getBackendIdsWithLocProp("rack", "r1");
        backendIdsWithLocProp.addAll(getBackendIdsWithLocProp("rack", "rack2"));
        for (Tablet tablet : tablets) {
            List<Replica> replicas = tablet.getAllReplicas();
            Set<Long> replicaBackendIds = Sets.newHashSet();
            for (Replica replica : replicas) {
                replicaBackendIds.add(replica.getBackendId());
            }
            System.out.println(replicaBackendIds);
            Assert.assertEquals(3, replicaBackendIds.size());
        }

        // clean up
        clearBackendLocationProp();
    }
}