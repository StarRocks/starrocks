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

package com.starrocks.sql.plan;

import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The TABLET clause of a table reference on an OLAP table. Each tablet the clause names
 * must be a tablet of the partitions the reference reads, or the statement fails. A named tablet whose partition
 * pruning removes, because the WHERE clause rules the partition out or because the partition holds no data, is not
 * read and raises no error. The cases run in a shared-nothing cluster here, and in a shared-data one in a subclass.
 *
 * <p>Pruning decides these outcomes, so two switches that unit tests usually set apart from production are back at
 * their production values: FeConstants.runningUnitTest, which counts every partition as loaded, and
 * FeConstants.enablePruneEmptyOutputScan, which plan tests usually turn off and which is what turns a scan left with
 * nothing to read into an empty set.
 */
public class TabletClausePlanTest {
    protected static final String DB = "tablet_clause";
    // By month: p1 and p2 are loaded, p3 never is.
    private static final String PARTITIONS = "PARTITION BY RANGE(dt) (\n" +
            "  PARTITION p1 VALUES LESS THAN ('2025-02-01'),\n" +
            "  PARTITION p2 VALUES LESS THAN ('2025-03-01'),\n" +
            "  PARTITION p3 VALUES LESS THAN ('2025-04-01')\n" +
            ")\n";
    private static final String HASH_DISTRIBUTION = "DISTRIBUTED BY HASH(k) BUCKETS 2\n";
    private static final String PROPERTIES = "PROPERTIES (\"replication_num\" = \"1\")";

    protected static ConnectContext connectContext;
    protected static StarRocksAssert starRocksAssert;
    // The duplicate key tables a SELECT reads, partitioned as above.
    protected static final List<String> SELECT_TABLES = new ArrayList<>();
    private static boolean savedRunningUnitTest;

    @BeforeAll
    public static void beforeClass() throws Exception {
        startCluster(RunMode.SHARED_NOTHING);
        createTables();
        useProductionPruning();
    }

    @AfterAll
    public static void afterClass() {
        FeConstants.runningUnitTest = savedRunningUnitTest;
    }

    protected static void startCluster(RunMode runMode) throws Exception {
        UtFrameUtils.createMinStarRocksCluster(runMode);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB).useDatabase(DB);
        SELECT_TABLES.clear();
    }

    protected static void createTables() throws Exception {
        createPartitionedTables("t_dup", "t_pk", HASH_DISTRIBUTION);
        starRocksAssert.withTable("CREATE TABLE t_unloaded_pk (k bigint NOT NULL, v bigint NULL) PRIMARY KEY(k)\n" +
                HASH_DISTRIBUTION + PROPERTIES);
        starRocksAssert.alterTable("ALTER TABLE t_dup ADD TEMPORARY PARTITION tp1 VALUES LESS THAN ('2025-02-01')");
        markLoaded(table("t_dup").getPartition("tp1", true));
        starRocksAssert.withTable("CREATE TABLE t_sync_mv_base (dt date NOT NULL, k bigint NOT NULL, v bigint NULL) " +
                "DUPLICATE KEY(dt, k)\n" + PARTITIONS + HASH_DISTRIBUTION + PROPERTIES);
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW t_sync_mv AS " +
                "SELECT dt, k, sum(v) FROM t_sync_mv_base GROUP BY dt, k");
        // Marked after the view is built, so that the tablets of its index are loaded too.
        markLoaded(table("t_sync_mv_base").getPartition("p1"));
        markLoaded(table("t_sync_mv_base").getPartition("p2"));
    }

    protected static void createPartitionedTables(String duplicateKeyTable, String primaryKeyTable, String distribution)
            throws Exception {
        String columns = " (dt date NOT NULL, k bigint NOT NULL, v bigint NULL) ";
        starRocksAssert.withTable("CREATE TABLE " + duplicateKeyTable + columns + "DUPLICATE KEY(dt, k)\n" +
                PARTITIONS + distribution + PROPERTIES);
        starRocksAssert.withTable("CREATE TABLE " + primaryKeyTable + columns + "PRIMARY KEY(dt, k)\n" +
                PARTITIONS + distribution + PROPERTIES);
        List<String> tables = List.of(duplicateKeyTable, primaryKeyTable);
        for (String name : tables) {
            markLoaded(table(name).getPartition("p1"));
            markLoaded(table(name).getPartition("p2"));
        }
        SELECT_TABLES.add(duplicateKeyTable);
    }

    protected static void useProductionPruning() {
        savedRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = false;
        assertTrue(FeConstants.enablePruneEmptyOutputScan);
    }

    // WHERE can rule out the partition of a named tablet, or leave only partitions that hold no data, or none at all;
    // the tablet is then not read, and the statement reads the other tablets it names. A partition that holds no data
    // is pruned whatever the WHERE clause.
    @Test
    public void testTabletsOfPrunedPartitionsAreNotReadAndRaiseNoError() {
        List<Executable> cases = new ArrayList<>();
        for (String t : SELECT_TABLES) {
            long p1 = tabletOf(t, "p1");
            long p2 = tabletOf(t, "p2");
            long p3 = tabletOf(t, "p3");
            String select = "SELECT k FROM " + t;
            cases.add(reads(select + " TABLET (" + p1 + ")", t, List.of(p1)));
            cases.add(reads(select + " TABLET (" + p1 + ") WHERE dt = '2025-02-15'", t, List.of()));
            cases.add(reads(select + " PARTITION (p1, p2) TABLET (" + p1 + ") WHERE dt = '2025-02-15'", t, List.of()));
            cases.add(reads(select + " TABLET (" + p1 + ", " + p2 + ") WHERE dt = '2025-02-15'", t, List.of(p2)));
            cases.add(reads(select + " TABLET (" + p1 + ") WHERE dt = '2025-03-15'", t, List.of()));
            cases.add(reads(select + " TABLET (" + p1 + ") WHERE dt = '2025-05-15'", t, List.of()));
            cases.add(reads(select + " TABLET (" + p3 + ")", t, List.of()));
            cases.add(reads(select + " TABLET (" + p3 + ") WHERE dt >= '2025-03-01'", t, List.of()));
        }
        // The only partition of a table that is not partitioned holds no data.
        long unloaded = tabletOf("t_unloaded_pk", "t_unloaded_pk");
        cases.add(reads("SELECT k FROM t_unloaded_pk TABLET (" + unloaded + ")", "t_unloaded_pk", List.of()));
        assertAll(cases);
    }

    // A table reference reads the partitions PARTITION or TEMPORARY PARTITION names, or else the partitions of the
    // table, and each named tablet must be a tablet of them. Any other fails, whatever the WHERE clause, so that a
    // tablet that is gone, such as one a split replaced, is never mistaken for one that holds nothing.
    @Test
    public void testTabletsMustBelongToTheReadPartitions() {
        List<Executable> cases = new ArrayList<>();
        for (String t : SELECT_TABLES) {
            addReadPartitionCases(cases, "SELECT k FROM " + t, t);
        }
        long p1 = tabletOf("t_dup", "p1");
        long temporary = table("t_dup").getPartition("tp1", true).getDefaultPhysicalPartition()
                .getQueryableBaseIndex().getTabletIdsInOrder().get(0);
        cases.add(reads("SELECT k FROM t_dup TEMPORARY PARTITION (tp1) TABLET (" + temporary + ")", "t_dup",
                List.of(temporary)));
        cases.add(fails("SELECT k FROM t_dup TABLET (" + temporary + ")",
                "Invalid tablet id: '" + temporary +
                        "'. The tablet is in temporary partition 'tp1', which the statement does not name"));
        cases.add(fails("SELECT k FROM t_dup TEMPORARY PARTITION (tp1) TABLET (" + p1 + ")",
                notInNamedPartition(p1, "p1")));
        assertAll(cases);
    }

    private static void addReadPartitionCases(List<Executable> cases, String statement, String t) {
        long p1 = tabletOf(t, "p1");
        long otherTable = tabletOf("t_unloaded_pk", "t_unloaded_pk");
        cases.add(reads(statement + " PARTITION (p1) TABLET (" + p1 + ") WHERE v > 0", t, List.of(p1)));
        cases.add(fails(statement + " TABLET (1) WHERE v > 0", noSuchTablet(1, t)));
        cases.add(fails(statement + " TABLET (1) WHERE dt = '2025-03-15'", noSuchTablet(1, t)));
        cases.add(fails(statement + " TABLET (1) WHERE 1 = 0", noSuchTablet(1, t)));
        cases.add(fails(statement + " TABLET (" + otherTable + ") WHERE v > 0", noSuchTablet(otherTable, t)));
        cases.add(fails(statement + " PARTITION (p2) TABLET (" + p1 + ") WHERE v > 0", notInNamedPartition(p1, "p1")));
        cases.add(fails(statement + " PARTITION (p3) TABLET (" + p1 + ") WHERE v > 0", notInNamedPartition(p1, "p1")));
    }

    // A direct read of a synchronous materialized view reads the index of the view, so the tablets it names are checked
    // against that index: a tablet of the view is read, and a tablet of the base table is not one it reads.
    @Test
    public void testSyncMaterializedViewReadChecksTabletsOfTheViewIndex() {
        OlapTable base = table("t_sync_mv_base");
        long viewIndexMetaId = base.getIndexMetaIdByName("t_sync_mv");
        long viewTablet = base.getPartition("p1").getDefaultPhysicalPartition().getQueryableIndex(viewIndexMetaId)
                .getTabletIdsInOrder().get(0);
        long baseTablet = tabletOf("t_sync_mv_base", "p1");
        assertAll(
                reads("SELECT * FROM t_sync_mv TABLET (" + viewTablet + ") [_SYNC_MV_]", "t_sync_mv_base",
                        List.of(viewTablet)),
                fails("SELECT * FROM t_sync_mv TABLET (" + baseTablet + ") [_SYNC_MV_]",
                        noSuchTablet(baseTablet, "t_sync_mv_base")));
    }

    @Test
    public void testRepeatedTabletIsReadOnce() {
        List<Executable> cases = new ArrayList<>();
        for (String t : SELECT_TABLES) {
            long p1 = tabletOf(t, "p1");
            cases.add(reads("SELECT k FROM " + t + " TABLETS (" + p1 + ", " + p1 + ")", t, List.of(p1)));
        }
        assertAll(cases);
    }

    // A duplicate key table answers COUNT(*) from tablet metadata. A tablet of a partition that holds no data leaves the
    // count nothing to read, which makes it 0 as for any empty input, and a tablet the table does not have fails it.
    @Test
    public void testCountOverTabletOfUnloadedPartitionIsZeroAndOverMissingTabletFails() {
        long p1 = tabletOf("t_dup", "p1");
        long p3 = tabletOf("t_dup", "p3");
        assertAll(
                () -> {
                    String explain = explain("SELECT count(*) FROM t_dup TABLET (" + p3 + ")");
                    assertTrue(explain.contains("output: count(*)") && explain.contains("0:EMPTYSET") &&
                            !explain.contains("MetaScan"), explain);
                },
                fails("SELECT count(*) FROM t_dup TABLET (1)", noSuchTablet(1, "t_dup")),
                () -> {
                    String explain = explain("SELECT count(*) FROM t_dup TABLET (" + p1 + ", " + p3 + ")");
                    assertTrue(explain.contains("MetaScan") && explain.contains("TabletIds: [" + p1 + "]"), explain);
                });
    }

    protected static String noSuchTablet(long tabletId, String tableName) {
        return "Invalid tablet id: '" + tabletId + "'. Table '" + tableName + "' has no such tablet";
    }

    protected static String notInNamedPartition(long tabletId, String partitionName) {
        return "Invalid tablet id: '" + tabletId + "'. The tablet is in partition '" + partitionName +
                "', which the statement does not name";
    }

    protected static Executable reads(String sql, String tableName, List<Long> tablets) {
        return () -> assertEquals(tablets, scannedTablets(plan(sql), tableName), sql);
    }

    protected static Executable fails(String sql, String message) {
        return () -> {
            Exception e = assertThrows(Exception.class, () -> plan(sql), sql);
            assertTrue(e.getMessage().contains(message), sql + "\n" + e.getMessage());
        };
    }

    protected static ExecPlan plan(String sql) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.setDumpInfo(new QueryDumpInfo(connectContext));
        connectContext.getDumpInfo().setOriginStmt(sql);
        StatementBase statement = SqlParser.parse(sql, connectContext.getSessionVariable().getSqlMode()).get(0);
        return StatementPlanner.plan(statement, connectContext);
    }

    private static String explain(String sql) throws Exception {
        return plan(sql).getExplainString(TExplainLevel.NORMAL);
    }

    // The tablets the plan reads from the table, over all of its scans.
    private static List<Long> scannedTablets(ExecPlan plan, String tableName) {
        return plan.getScanNodes().stream()
                .filter(scan -> scan instanceof OlapScanNode && scan.getTableName().equals(tableName))
                .flatMap(scan -> ((OlapScanNode) scan).getScanTabletIds().stream())
                .collect(Collectors.toList());
    }

    protected static OlapTable table(String name) {
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB).getTable(name);
    }

    // The first tablet a scan of the partition reads.
    protected static long tabletOf(String tableName, String partitionName) {
        return table(tableName).getPartition(partitionName).getDefaultPhysicalPartition()
                .getQueryableBaseIndex().getTabletIdsInOrder().get(0);
    }

    protected static void markLoaded(Partition partition) {
        long version = Partition.PARTITION_INIT_VERSION + 1;
        Collection<PhysicalPartition> physicalPartitions = partition.getSubPartitions();
        for (PhysicalPartition physicalPartition : physicalPartitions) {
            physicalPartition.setVisibleVersion(version, System.currentTimeMillis());
            physicalPartition.setDataVersion(version);
            List<MaterializedIndex> indexes = physicalPartition.getLatestMaterializedIndices(IndexExtState.VISIBLE);
            for (MaterializedIndex index : indexes) {
                List<Tablet> tablets = index.getTablets();
                for (Tablet tablet : tablets) {
                    if (tablet instanceof LocalTablet) {
                        ((LocalTablet) tablet).getImmutableReplicas()
                                .forEach(replica -> replica.updateVersionInfo(version, -1, version));
                    }
                }
            }
        }
    }
}
