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
import com.starrocks.clone.TabletSchedulerStat;
import com.starrocks.common.Config;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AlterSystemStmtAnalyzer;
import com.starrocks.system.Backend;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Using fixed order test, because the test case is related to the order of the test case.
 * First we test best-effort balance, i.e. if we cannot find enough backends to match the
 * location requirement of the table, we will ignore the location requirement.
 * Second we test fully matched balance, i.e. we add enough backends to match the location
 * requirement of the table, and see whether the balance works as expected.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class LocationLabeledTableBalanceTest {
    private static final long WAIT_FOR_CLONE_TIMEOUT = 180000;
    @BeforeClass
    public static void setUp() throws Exception {
        Config.tablet_sched_checker_interval_seconds = 1;
        Config.tablet_sched_repair_delay_factor_second = 1;
        Config.tablet_checker_partition_batch_num = 1;
        Config.drop_backend_after_decommission = false;
        Config.sys_log_verbose_modules = new String[] {"com.starrocks.clone"};
        Config.tablet_sched_slot_num_per_path = 32;
        Config.tablet_sched_consecutive_full_clone_delay_sec = 1;
        PseudoBackend.reportIntervalMs = 1000;
        PseudoCluster.getOrCreateWithRandomPort(true, 3);
        PseudoCluster cluster = PseudoCluster.getInstance();
        cluster.runSql(null, "create database test");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown();
    }

    private List<Tablet> getBackendTabletsByTable(PseudoBackend backend, long tableId) {
        return backend.getTabletManager().getTabletsByTable(tableId);
    }

    @Test
    public void test1BestEffortBalance() throws SQLException, InterruptedException {
        // Initialize 3 backends with location: rack:r1, rack:r1, rack:r2
        PseudoCluster cluster = PseudoCluster.getInstance();
        List<PseudoBackend> backendList = new ArrayList<>(cluster.getBackends());
        String sql = "alter system modify backend '" + backendList.get(0).getHostHeartbeatPort() + "' set ('" +
                AlterSystemStmtAnalyzer.PROP_KEY_LOCATION + "' = 'rack:r1')";
        cluster.runSql(null, sql);
        sql = "alter system modify backend '" + backendList.get(1).getHostHeartbeatPort() + "' set ('" +
                AlterSystemStmtAnalyzer.PROP_KEY_LOCATION + "' = 'rack:r1')";
        cluster.runSql(null, sql);
        sql = "alter system modify backend '" + backendList.get(2).getHostHeartbeatPort() + "' set ('" +
                AlterSystemStmtAnalyzer.PROP_KEY_LOCATION + "' = 'rack:r2')";
        cluster.runSql(null, sql);

        // create table, replicas will be placed on rack:r1 and rack:r2
        sql = "CREATE TABLE test.`test_table_best_effort_balance` (\n" +
                "    k1 int,\n" +
                "    k2 VARCHAR NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"3\",\n" +
                "    \"" + PropertyAnalyzer.PROPERTIES_LABELS_LOCATION + "\" = \"rack:*\",\n" +
                "    \"in_memory\" = \"false\"\n" +
                ");";
        cluster.runSql("test", sql);

        Database test = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable olapTable = (OlapTable) test.getTable("test_table_best_effort_balance");

        // Add another backend without location property, with best-effort balance strategy,
        // There should be replicas moved to this backend
        List<Long> newBackends = cluster.addBackends(1);
        long firstNewBackend = newBackends.get(0);
        PseudoBackend newBackend = cluster.getBackend(firstNewBackend);
        long start = System.currentTimeMillis();
        TabletSchedulerStat stat = GlobalStateMgr.getCurrentState().getTabletScheduler().getStat();
        while (getBackendTabletsByTable(newBackend, olapTable.getId()).size() < 3) {
            System.out.println("new backend tablets(no loc): " +
                    getBackendTabletsByTable(newBackend, olapTable.getId()) +
                    ", clone tasks finished: " + stat.counterCloneTaskSucceeded.get());
            Thread.sleep(1000);
            if (System.currentTimeMillis() - start > WAIT_FOR_CLONE_TIMEOUT) {
                Assert.fail("wait for enough clone tasks for location balance finished timeout");
            }
        }
        System.out.println("new backend tablets: " + getBackendTabletsByTable(newBackend, olapTable.getId()) +
                ", clone tasks finished: " + stat.counterCloneTaskSucceeded.get());
        Assert.assertEquals(3, getBackendTabletsByTable(newBackend, olapTable.getId()).size());

        // Add another backend with location property rack:r3, check location mismatch repair
        // Replicas will be moved from last added backend(with no location) to this backend.
        printTabletReplicaInfo(olapTable);
        newBackend = addPseudoBackendWithLocation("rack:r3");
        while (getBackendTabletsByTable(newBackend, olapTable.getId()).size() < 5) {
            System.out.println("new backend tablets(rack:r3): " +
                    getBackendTabletsByTable(newBackend, olapTable.getId()) +
                    ", clone tasks finished: " + stat.counterCloneTaskSucceeded.get());
            Thread.sleep(1000);
            if (System.currentTimeMillis() - start > WAIT_FOR_CLONE_TIMEOUT) {
                Assert.fail("wait for enough clone tasks for location balance finished timeout");
            }
        }
        Assert.assertEquals(5, getBackendTabletsByTable(newBackend, olapTable.getId()).size());

        // Wait for redundant replicas deleted.
        Thread.sleep(10000);
        Assert.assertEquals(0, getBackendTabletsByTable(cluster.getBackend(firstNewBackend),
                olapTable.getId()).size());
    }

    private void printTabletReplicaInfo(OlapTable table) {
        table.getPartitions().forEach(partition -> {
            partition.getBaseIndex().getTablets().forEach(tablet -> {
                StringBuffer stringBuffer = new StringBuffer();
                stringBuffer.append("tablet ").append(tablet.getId()).append(": ");
                for (Replica replica : tablet.getAllReplicas()) {
                    stringBuffer.append(replica.getBackendId()).append(" ")
                            .append(GlobalStateMgr.getCurrentSystemInfo().getBackend(
                                    replica.getBackendId()).getLocation()).append(", ");
                }
                System.out.println(stringBuffer);
            });
        });
    }

    @Test
    public void test2LocationMatchedBalance() throws InterruptedException, SQLException {
        Database test = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable olapTable = (OlapTable) test.getTable("test_table_best_effort_balance");

        long start = System.currentTimeMillis();
        TabletSchedulerStat stat = GlobalStateMgr.getCurrentState().getTabletScheduler().getStat();
        printTabletReplicaInfo(olapTable);
        // Add a new backend with location rack:r4, and replicas will be balanced to it.
        PseudoBackend newBackend = addPseudoBackendWithLocation("rack:r4");
        while (getBackendTabletsByTable(newBackend, olapTable.getId()).size() < 3) {
            System.out.println("new backend tablets(rack:r4): " +
                    getBackendTabletsByTable(newBackend, olapTable.getId()) +
                    ", clone tasks finished: " + stat.counterCloneTaskSucceeded.get());
            printTabletReplicaInfo(olapTable);
            Thread.sleep(1000);
            if (System.currentTimeMillis() - start > WAIT_FOR_CLONE_TIMEOUT) {
                Assert.fail("wait for enough clone tasks for location balance finished timeout");
            }
        }

        // Wait for redundant replicas deleted.
        Thread.sleep(10000);
        System.out.println("new backend tablets(rack:r4): " +
                getBackendTabletsByTable(newBackend, olapTable.getId()) +
                ", clone tasks finished: " + stat.counterCloneTaskSucceeded.get());
        Assert.assertTrue(getBackendTabletsByTable(newBackend, olapTable.getId()).size() >= 3);
        Assert.assertTrue(getBackendTabletsByTable(newBackend, olapTable.getId()).size() <= 4);
        Set<Long> backendIds = getBackendIdsWithLocProp("rack", "r3");
        Assert.assertTrue(getBackendTabletsByTable(
                PseudoCluster.getInstance().getBackend(backendIds.iterator().next()), olapTable.getId()).size() <= 4);
        printTabletReplicaInfo(olapTable);
    }

    private PseudoBackend addPseudoBackendWithLocation(String location) throws SQLException {
        PseudoCluster cluster = PseudoCluster.getInstance();
        List<Long> newBackends = cluster.addBackends(1);
        long newBackendId = newBackends.get(0);
        String sql = "alter system modify backend '" +
                cluster.getBackend(newBackendId).getHostHeartbeatPort() + "' set ('" +
                PropertyAnalyzer.PROPERTIES_LABELS_LOCATION + "' = '" + location + "')";
        cluster.runSql(null, sql);

        return cluster.getBackend(newBackendId);
    }

    private Set<Long> getBackendIdsWithLocProp(String locationKey, String locationVal) {
        PseudoCluster cluster = PseudoCluster.getInstance();
        Set<Long> backendIds = Sets.newHashSet();
        for (PseudoBackend pseudoBackend : cluster.getBackends()) {
            Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(pseudoBackend.getId());
            if (backend.getLocation().containsKey(locationKey) &&
                    Objects.equals(backend.getLocation().get(locationKey), locationVal)) {
                backendIds.add(backend.getId());
            }
        }
        return backendIds;
    }
}
