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

package com.starrocks.pseudocluster;

import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.clone.TabletScheduler;
import com.starrocks.clone.TabletSchedulerStat;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AlterSystemStmtAnalyzer;
import com.starrocks.system.Backend;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class LocationMismatchRepairTest {
    @BeforeClass
    public static void setUp() throws Exception {
        Config.tablet_sched_checker_interval_seconds = 1;
        Config.tablet_sched_repair_delay_factor_second = 1;
        Config.tablet_checker_partition_batch_num = 1;
        Config.drop_backend_after_decommission = false;
        Config.sys_log_verbose_modules = new String[] {"com.starrocks.clone"};
        Config.tablet_sched_slot_num_per_path = 32;
        Config.tablet_sched_consecutive_full_clone_delay_sec = 1;
        // Disable balance
        Config.tablet_sched_disable_balance = true;
        TabletScheduler.stateUpdateIntervalMs = 1000;
        PseudoBackend.reportIntervalMs = 1000;
        PseudoCluster.getOrCreateWithRandomPort(true, 10);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown(true);
    }

    @Before
    public void before() throws SQLException {
        PseudoCluster cluster = PseudoCluster.getInstance();
        cluster.runSql(null, "create database test");
    }

    @After
    public void after() throws SQLException {
        PseudoCluster cluster = PseudoCluster.getInstance();
        cluster.runSql(null, "drop database test FORCE");
    }

    private void setBackendLocationProp() throws SQLException {
        PseudoCluster cluster = PseudoCluster.getInstance();
        int i = 0;
        int j = 0;
        for (PseudoBackend backend : cluster.getBackends()) {
            String stmtStr = "alter system modify backend '" + backend.getHostHeartbeatPort() + "' set ('" +
                    AlterSystemStmtAnalyzer.PROP_KEY_LOCATION + "' = 'rack:r" + i + "')";
            if (j % 2 == 1) {
                i++;
            }
            j++;
            System.out.println(stmtStr);
            cluster.runSql(null, stmtStr);
        }
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

    private void printTabletReplicaInfo(OlapTable table) {
        table.getPartitions().forEach(partition -> {
            partition.getBaseIndex().getTablets().forEach(tablet -> {
                StringBuffer stringBuffer = new StringBuffer();
                stringBuffer.append("tablet ").append(tablet.getId()).append(": ");
                for (Replica replica : tablet.getAllReplicas()) {
                    stringBuffer.append(replica.getBackendId()).append(" ")
                            .append(GlobalStateMgr.getCurrentState().getNodeMgr()
                                    .getClusterInfo().getBackend(
                            replica.getBackendId()).getLocation()).append(", ");
                }
                System.out.println(stringBuffer);
            });
        });
    }

    private Set<Long> getTabletReplicasBackendIds(long tabletId) {
        TabletInvertedIndex tabletInvertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        List<Replica> replicas = tabletInvertedIndex.getReplicasByTabletId(tabletId);
        Set<Long> replicaBackendIds = new HashSet<>();
        replicas.forEach(replica -> replicaBackendIds.add(replica.getBackendId()));

        return replicaBackendIds;
    }

    @Test
    public void testRepairAfterChangeTableLocation() throws Exception {
        setBackendLocationProp();

        PseudoCluster cluster = PseudoCluster.getInstance();
        String sql = "CREATE TABLE test.`test_table_backend_no_loc` (\n" +
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
        OlapTable table = (OlapTable) db.getTable("test_table_backend_no_loc");
        Assert.assertNotNull(table.getLocation());
        Assert.assertTrue(table.getLocation().keySet().contains("*"));

        printTabletReplicaInfo(table);

        // check: replica of table `test_table_backend_no_loc` will move tablet to rack r1, r2 and r3
        Set<Long> backendIds = getBackendIdsWithLocProp("rack", "r0");
        backendIds.addAll(getBackendIdsWithLocProp("rack", "r4"));
        System.out.println("move from backends: " + backendIds);
        List<Long> tabletIds = cluster.listTablets("test", "test_table_backend_no_loc");
        int locationMismatchFullCloneNeeded = 0;
        for (long tabletId : tabletIds) {
            Set<Long> replicaBackendIds = getTabletReplicasBackendIds(tabletId);
            Set<Long> intersection = Sets.intersection(backendIds, replicaBackendIds);
            if (!intersection.isEmpty()) {
                locationMismatchFullCloneNeeded += intersection.size();
            }
        }

        TabletSchedulerStat stat = GlobalStateMgr.getCurrentState().getTabletScheduler().getStat();
        long oldCloneFinishedCnt = stat.counterCloneTaskSucceeded.get();
        long oldLocationMismatchErr = stat.counterReplicaLocMismatchErr.get();
        System.out.println("old clone finished: " + oldCloneFinishedCnt + ", old location mismatch: " +
                oldLocationMismatchErr);
        sql = "ALTER TABLE test.`test_table_backend_no_loc` SET ('" +
                PropertyAnalyzer.PROPERTIES_LABELS_LOCATION + "' = 'rack:r1,rack:r2,rack:r3');";
        cluster.runSql("test", sql);
        System.out.println("=========================");
        long start = System.currentTimeMillis();
        while (true) {
            if (stat.counterCloneTaskSucceeded.get() - oldCloneFinishedCnt >= locationMismatchFullCloneNeeded
                    && stat.counterReplicaLocMismatchErr.get() - oldLocationMismatchErr >=
                    locationMismatchFullCloneNeeded) {
                break;
            }
            System.out.println("wait for enough clone tasks for LOCATION_MISMATCH finished, " +
                    "current clone finished: " + stat.counterCloneTaskSucceeded.get() +
                    ", current location mismatch: " + stat.counterReplicaLocMismatchErr.get() +
                    ", expected clone finished: " + locationMismatchFullCloneNeeded);
            Thread.sleep(1000);
            if (System.currentTimeMillis() - start > 180000) {
                Assert.fail("wait for enough clone tasks for LOCATION_MISMATCH finished timeout");
            }
        }

        // sleep to wait for redundant replicas cleaned
        Thread.sleep(5000);
        printTabletReplicaInfo(table);
        Set<Long> stayedBackendIds = getBackendIdsWithLocProp("rack", "r1");
        stayedBackendIds.addAll(getBackendIdsWithLocProp("rack", "r2"));
        stayedBackendIds.addAll(getBackendIdsWithLocProp("rack", "r3"));
        for (long tabletId : tabletIds) {
            Set<Long> replicaBackendIds = getTabletReplicasBackendIds(tabletId);
            Set<Long> intersection = Sets.intersection(stayedBackendIds, replicaBackendIds);
            // check replicas scattered on 3 different racks after repair
            Set<Pair<String, String>> racks = new HashSet<>();
            for (long backendId : intersection) {
                Backend backend = GlobalStateMgr.getCurrentState().getNodeMgr()
                        .getClusterInfo().getBackend(backendId);
                racks.add(backend.getSingleLevelLocationKV());
            }
            Assert.assertEquals(3, racks.size());
        }
    }

    @Test
    public void testRepairLocationAfterDecommissionBackend() throws Exception {
        setBackendLocationProp();

        PseudoCluster cluster = PseudoCluster.getInstance();
        String sql = "CREATE TABLE test.`test_table_backend_no_loc` (\n" +
                "    k1 int,\n" +
                "    k2 VARCHAR NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"3\",\n" +
                "    \"" + PropertyAnalyzer.PROPERTIES_LABELS_LOCATION + "\" = \"rack:r0,rack:r1,rack:r2,rack:r4\",\n" +
                "    \"in_memory\" = \"false\"\n" +
                ");";
        cluster.runSql("test", sql);

        // decommission backends with 'rack:r4' location
        Set<Long> backendIds = getBackendIdsWithLocProp("rack", "r4");
        List<Pair<PseudoBackend, Integer>> decommissionedBackends = new ArrayList<>();
        for (Long backendId : backendIds) {
            final PseudoBackend decommissionBE = cluster.getBackend(backendId);
            int oldTabletNum = decommissionBE.getTabletManager().getNumTablet();
            decommissionedBackends.add(new Pair<>(decommissionBE, oldTabletNum));
            sql = "ALTER SYSTEM DECOMMISSION BACKEND '" + decommissionBE.getHostHeartbeatPort() + "';";
            cluster.runSql(null, sql);
        }

        // wait for decommission finished
        for (Pair<PseudoBackend, Integer> pair : decommissionedBackends) {
            PseudoBackend decommissionBE = pair.first;
            while (true) {
                int curTabletNum = decommissionBE.getTabletManager().getNumTablet();
                System.out.printf("#tablets: %d/%d: fullClone: %d wait...\n", curTabletNum, pair.second,
                        Tablet.getTotalFullClone());
                if (curTabletNum == 0) {
                    break;
                }
                Thread.sleep(1000);
            }
        }

        // sleep to wait for redundant replicas cleaned
        Thread.sleep(5000);
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable table = (OlapTable) db.getTable("test_table_backend_no_loc");
        printTabletReplicaInfo(table);
        Set<Long> stayedBackendIds = getBackendIdsWithLocProp("rack", "r0");
        stayedBackendIds.addAll(getBackendIdsWithLocProp("rack", "r1"));
        stayedBackendIds.addAll(getBackendIdsWithLocProp("rack", "r2"));
        List<Long> tabletIds = cluster.listTablets("test", "test_table_backend_no_loc");
        // Check after decommission, the tablets of `test_table_backend_no_loc` should only stay on backends with
        // location label 'rack:r0', 'rack:r1' and 'rack:r2'.
        for (long tabletId : tabletIds) {
            Set<Long> replicaBackendIds = getTabletReplicasBackendIds(tabletId);
            Set<Long> intersection = Sets.intersection(stayedBackendIds, replicaBackendIds);
            // check replicas scattered on 3 different racks after decommission
            Set<Pair<String, String>> racks = new HashSet<>();
            for (long backendId : intersection) {
                Backend backend = GlobalStateMgr.getCurrentState().getNodeMgr()
                        .getClusterInfo().getBackend(backendId);
                racks.add(backend.getSingleLevelLocationKV());
            }
            Assert.assertEquals(3, racks.size());
        }
    }
}
