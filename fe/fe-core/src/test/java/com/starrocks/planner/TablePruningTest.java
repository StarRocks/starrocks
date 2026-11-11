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

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.starrocks.sql.optimizer.statistics.CachedStatisticStorageTest.DEFAULT_CREATE_TABLE_TEMPLATE;
import static com.starrocks.sql.optimizer.statistics.CachedStatisticStorageTest.connectContext;

public class TablePruningTest extends TablePruningTestBase {
    @BeforeAll
    public static void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.getSessionVariable().setEnablePipelineEngine(true);
        ctx.getSessionVariable().setCboPushDownAggregateMode(-1);
        ctx.getSessionVariable().setOptimizerExecuteTimeout(10000);
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase(StatsConstants.STATISTICS_DB_NAME)
                .useDatabase(StatsConstants.STATISTICS_DB_NAME)
                .withTable(DEFAULT_CREATE_TABLE_TEMPLATE);
        starRocksAssert.withDatabase("table_prune_db").useDatabase("table_prune_db");
        getSsbCreateTableSqlList().forEach(createTblSql -> {
            try {
                starRocksAssert.withTable(createTblSql);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        getSqlList("sql/ssb/", "lineorder0").forEach(createTblSql -> {
            try {
                starRocksAssert.withTable(createTblSql);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        String ssbViewFmt = "create view lineorder_flat_%s as\n" +
                "select\n" +
                "   `lo_orderdate`,\n" +
                "   `lo_orderkey`,\n" +
                "   `lo_linenumber`,\n" +
                "   `lo_custkey`,\n" +
                "   `lo_partkey`,\n" +
                "   `lo_suppkey`,\n" +
                "   `lo_orderpriority`,\n" +
                "   `lo_shippriority`,\n" +
                "   `lo_quantity`,\n" +
                "   `lo_extendedprice`,\n" +
                "   `lo_ordtotalprice`,\n" +
                "   `lo_discount`,\n" +
                "   `lo_revenue`,\n" +
                "   `lo_supplycost`,\n" +
                "   `lo_tax`,\n" +
                "   `lo_commitdate`,\n" +
                "   `lo_shipmode`,\n" +
                "   `c_name`,\n" +
                "   `c_address`,\n" +
                "   `c_city`,\n" +
                "   `c_nation`,\n" +
                "   `c_region`,\n" +
                "   `c_phone`,\n" +
                "   `c_mktsegment`,\n" +
                "   `s_name`,\n" +
                "   `s_address`,\n" +
                "   `s_city`,\n" +
                "   `s_nation`,\n" +
                "   `s_region`,\n" +
                "   `s_phone`,\n" +
                "   `p_name`,\n" +
                "   `p_mfgr`,\n" +
                "   `p_category`,\n" +
                "   `p_brand`,\n" +
                "   `p_color`,\n" +
                "   `p_type`,\n" +
                "   `p_size`,\n" +
                "   `p_container`\n" +
                "from\n %s";

        String v1JoinRel = "   lineorder l\n" +
                "   inner join customer c on (c.c_custkey = l.lo_custkey)\n" +
                "   inner join supplier s  on (s.s_suppkey = l.lo_suppkey)\n" +
                "   inner join part p on (p.p_partkey = l.lo_partkey);";

        String v2JoinRel = "   lineorder l\n" +
                "   left join customer c on (c.c_custkey = l.lo_custkey)\n" +
                "   left join supplier s  on (s.s_suppkey = l.lo_suppkey)\n" +
                "   left join part p on (p.p_partkey = l.lo_partkey);";
        starRocksAssert.withView(String.format(ssbViewFmt, "v1", v1JoinRel));
        starRocksAssert.withView(String.format(ssbViewFmt, "v2", v2JoinRel));

        String createDeptsSql = "CREATE TABLE `depts` (\n" +
                "  `deptno` int(11) NOT NULL COMMENT \"\",\n" +
                "  `name` varchar(25) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`deptno`, `name`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`deptno`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\",\n" +
                "\"enable_persistent_index\" = \"true\",\n" +
                "\"replicated_storage\" = \"false\",\n" +
                "\"unique_constraints\" = \"deptno\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ")";

        String createEmpsSql = "CREATE TABLE `emps` (\n" +
                "  `empid` int(11) NOT NULL COMMENT \"\",\n" +
                "  `deptno` int(11) NOT NULL COMMENT \"\",\n" +
                "  `name` varchar(25) NOT NULL COMMENT \"\",\n" +
                "  `salary` double NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`empid`, `deptno`, `name`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`empid`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\",\n" +
                "\"enable_persistent_index\" = \"true\",\n" +
                "\"replicated_storage\" = \"false\",\n" +
                "\"unique_constraints\" = \"empid\",\n" +
                "\"foreign_key_constraints\" = \"(deptno) REFERENCES depts(deptno)\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ")";

        String createView1 = "create view emps_flat_view as \n" +
                "select \n" +
                "\tempid, \n" +
                "\temps.deptno as deptno, \n" +
                "\tdepts.deptno as deptno2, \n" +
                "\temps.name as name, \n" +
                "\tsalary, \n" +
                "\tdepts.name as dept_name\n" +
                "from emps inner join depts on emps.deptno = depts.deptno";

        starRocksAssert.withTable(createDeptsSql);
        starRocksAssert.withTable(createEmpsSql);
        starRocksAssert.withView(createView1);

        getSqlList("sql/table_prune/", "A", "B", "CII", "CI", "D", "E", "FII", "FI")
                .forEach(createTableSql -> {
                    try {
                        starRocksAssert.withTable(createTableSql);
                    } catch (Exception e) {
                        Assertions.fail("create table fail");
                    }
                });
        getSqlList("sql/table_prune/", "AddFKConstraints")
                .forEach(fkConstraints -> Arrays.stream(fkConstraints.split("\n")).forEach(addFk ->
                        {
                            try {
                                starRocksAssert.alterTableProperties(addFk);
                            } catch (Exception e) {
                                Assertions.fail();
                            }
                        }
                ));
        String[][] showAndResults = new String[][] {
                {"A", "\"foreign_key_constraints\" = \"" +
                        "(a_pk) REFERENCES default_catalog.table_prune_db.D(d_pk);" +
                        "(a_b_fk) REFERENCES default_catalog.table_prune_db.B(b_pk)\""
                },
                {"B", "\"foreign_key_constraints\" = \"" +
                        "(b_ci_fk) REFERENCES default_catalog.table_prune_db.CI(ci_pk);" +
                        "(b_cii_fk0,b_cii_fk1) REFERENCES default_catalog.table_prune_db.CII(cii_pk0,cii_pk1)\""},
                {"D", "\"foreign_key_constraints\" = \"" +
                        "(d_pk) REFERENCES default_catalog.table_prune_db.A(a_pk);" +
                        "(d_e_fk) REFERENCES default_catalog.table_prune_db.E(e_pk)\""},
                {"E", "\"foreign_key_constraints\" = \"" +
                        "(e_fi_fk) REFERENCES default_catalog.table_prune_db.FI(fi_pk);" +
                        "(e_fii_fk0,e_fii_fk1) REFERENCES default_catalog.table_prune_db.FII(fii_pk0,fii_pk1)\""},
        };
        for (String[] showAndResult : showAndResults) {
            List<List<String>> res = starRocksAssert.show(String.format("show create table %s", showAndResult[0]));
            Assertions.assertTrue(res.size() >= 1 && res.get(0).size() >= 2);
            String createTableSql = res.get(0).get(1);
            Assertions.assertTrue(createTableSql.contains(showAndResult[1]), createTableSql);
        }

        String tableA = "CREATE TABLE table_a (\n" +
                "    col_a BIGINT,\n" +
                "    col_tenant BIGINT NOT NULL,\n" +
                "    col_x BIGINT,\n" +
                "    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,\n" +
                "    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP\n" +
                ") PRIMARY KEY(`col_a`, `col_tenant`)\n" +
                "DISTRIBUTED BY HASH(col_a, `col_tenant`) BUCKETS 1\n" +
                "PROPERTIES(\n" +
                "  \"replication_num\" = \"1\"\n" +
                ");";
        String tableB = "CREATE TABLE table_b (\n" +
                "    col_a BIGINT NOT NULL,\n" +
                "    col_tenant BIGINT NOT NULL,\n" +
                "    col_score DECIMAL(10,4),\n" +
                "    col_version VARCHAR(50),\n" +
                "    created_at DATETIME DEFAULT CURRENT_TIMESTAMP\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(col_tenant, col_a) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "    \"unique_constraints\" = \"col_a,col_tenant\",\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");";

        String tableC = "CREATE TABLE table_c (\n" +
                "    col_a BIGINT NOT NULL,\n" +
                "    col_tenant BIGINT NOT NULL,\n" +
                "    col_x BIGINT,\n" +
                "    col_conf DECIMAL(5,4),\n" +
                "    created_at DATETIME DEFAULT CURRENT_TIMESTAMP\n" +
                ")\n" +
                "PRIMARY KEY(`col_a`, `col_tenant`)\n" +
                "DISTRIBUTED BY HASH(col_tenant, col_a) BUCKETS 1\n" +
                "PROPERTIES(\n" +
                "  \"replication_num\" = \"1\"\n" +
                ");";
        String viewA = "CREATE OR REPLACE VIEW view_a (\n" +
                "        col_a,\n" +
                "        col_tenant,\n" +
                "        col_x,\n" +
                "        col_score\n" +
                ")\n" +
                "AS SELECT\n" +
                "  table_a.col_a,\n" +
                "  table_a.col_tenant,\n" +
                "  COALESCE(table_c.col_x, table_a.col_x) AS col_x,\n" +
                "  table_b.col_score\n" +
                "FROM\n" +
                "    table_a\n" +
                "LEFT JOIN\n" +
                "    table_b ON table_b.col_a = table_a.col_a\n" +
                "    AND table_b.col_tenant = table_a.col_tenant\n" +
                "LEFT JOIN \n" +
                "    table_c ON table_a.col_a = table_c.col_a\n" +
                "    AND table_c.col_tenant = table_a.col_tenant;";
        starRocksAssert.withTable(tableA);
        starRocksAssert.withTable(tableB);
        starRocksAssert.withTable(tableC);
        starRocksAssert.withView(viewA);

        // Tables with several partitions and tablets, for scans that name some of them.
        starRocksAssert.withTable("CREATE TABLE `pk_partitioned` (\n" +
                "  `k1` date NOT NULL,\n" +
                "  `city` varchar(20) NOT NULL,\n" +
                "  `pk` bigint NOT NULL,\n" +
                "  `v1` bigint NULL\n" +
                ") PRIMARY KEY(`k1`, `city`, `pk`)\n" +
                "PARTITION BY RANGE(`k1`) (\n" +
                "  PARTITION p1 VALUES LESS THAN ('2025-02-01'),\n" +
                "  PARTITION p2 VALUES LESS THAN ('2025-03-01'),\n" +
                "  PARTITION p3 VALUES LESS THAN ('2025-04-01')\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`pk`) BUCKETS 3\n" +
                "PROPERTIES (\"replication_num\" = \"1\");");
        starRocksAssert.withTable("CREATE TABLE `uk_partitioned` (\n" +
                "  `pk` bigint NOT NULL,\n" +
                "  `v1` bigint NULL\n" +
                ") PRIMARY KEY(`pk`)\n" +
                "PARTITION BY RANGE(`pk`) (\n" +
                "  PARTITION p1 VALUES LESS THAN ('100'),\n" +
                "  PARTITION p2 VALUES LESS THAN ('200'),\n" +
                "  PARTITION p3 VALUES LESS THAN ('300')\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`pk`) BUCKETS 3\n" +
                "PROPERTIES (\"replication_num\" = \"1\", \"unique_constraints\" = \"pk\");");
        starRocksAssert.withTable("CREATE TABLE `fk_to_uk_partitioned` (\n" +
                "  `id` bigint NOT NULL,\n" +
                "  `fk` bigint NOT NULL,\n" +
                "  `v2` bigint NULL\n" +
                ") DUPLICATE KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n" +
                "PROPERTIES (\"replication_num\" = \"1\", " +
                "\"foreign_key_constraints\" = \"(fk) REFERENCES uk_partitioned(pk)\");");
        for (String table : List.of("pk_partitioned", "uk_partitioned", "fk_to_uk_partitioned")) {
            for (Partition partition : olapTableOf(table).getPartitions()) {
                UtFrameUtils.setPartitionVersion(partition, Partition.PARTITION_INIT_VERSION + 1);
            }
        }
        FeConstants.runningUnitTest = true;
    }

    // A way to turn table pruning on: its name, for failure messages, and the session settings it changes.
    private record PruningMode(String name, Consumer<SessionVariable> settings) {
    }

    private static final PruningMode CBO_WITHOUT_JOIN_REORDER = new PruningMode(
            "CBO table pruning without join reordering", sv -> {
                sv.setEnableCboTablePrune(true);
                sv.disableJoinReorder();
            });
    private static final PruningMode CBO_WITH_JOIN_REORDER = new PruningMode(
            "CBO table pruning with join reordering", sv -> sv.setEnableCboTablePrune(true));
    private static final PruningMode RBO = new PruningMode(
            "RBO table pruning", sv -> sv.setEnableRboTablePrune(true));
    private static final PruningMode RBO_ON_UPDATE = new PruningMode(
            "RBO table pruning, on UPDATE too", sv -> {
                sv.setEnableRboTablePrune(true);
                sv.setEnableTablePruneOnUpdate(true);
            });

    // Table pruning merges two scans of a table joined on its primary key into one scan, but not when either scan
    // names partitions, tablets or replicas: that scan reads only part of the table, so the other scan cannot stand
    // in for it.
    @Test
    public void testSameTableScansNamingPartitionsTabletsOrReplicasAreNotMerged() {
        List<Long> p1 = tabletsOf("pk_partitioned", "p1");
        List<Long> p1AndP2 = tabletsOf("pk_partitioned", "p1", "p2");
        List<Long> p2AndP3 = tabletsOf("pk_partitioned", "p2", "p3");
        List<Long> all = tabletsOf("pk_partitioned", "p1", "p2", "p3");
        long tablet = p1.get(0);
        long replica = replicaOf("pk_partitioned", "p1", tablet);
        String on = " on a.k1 = b.k1 and a.city = b.city and a.pk = b.pk";
        String where = " where pk_partitioned.k1 = b.k1 and pk_partitioned.city = b.city and pk_partitioned.pk = b.pk";
        Object[][] cases = {
                // statement, the tablets each scan of pk_partitioned reads
                {"select a.pk, b.v1 from pk_partitioned partition (p1) a join pk_partitioned b" + on,
                        List.of(p1, all)},
                {"select a.pk, b.v1 from pk_partitioned a join pk_partitioned partition (p1) b" + on,
                        List.of(p1, all)},
                {"select a.pk, b.v1 from pk_partitioned partition (p1, p2) a join pk_partitioned partition (p2, p3) b" +
                        on, List.of(p1AndP2, p2AndP3)},
                {"select a.pk, b.v1 from pk_partitioned tablet (" + tablet + ") a join pk_partitioned b" + on,
                        List.of(List.of(tablet), all)},
                {"select a.pk, b.v1 from pk_partitioned replica (" + replica + ") a join pk_partitioned b" + on,
                        List.of(List.of(tablet), all)},
                {"select a.pk, b.v1 from pk_partitioned a left join pk_partitioned partition (p1) b" + on,
                        List.of(p1, all)},
                {"select a.pk, b.v1 from pk_partitioned partition (p1) a left join pk_partitioned b" + on,
                        List.of(p1, all)},
                {"update pk_partitioned set v1 = b.v1 + 1 from pk_partitioned partition (p1) b" + where,
                        List.of(p1, all)},
                // Without these clauses the two scans are merged into one.
                {"select a.pk, b.v1 from pk_partitioned a join pk_partitioned b" + on + " where b.v1 > 0",
                        List.of(all)},
        };
        assertScannedTablets("pk_partitioned", List.of(CBO_WITHOUT_JOIN_REORDER, CBO_WITH_JOIN_REORDER, RBO,
                RBO_ON_UPDATE), cases);
    }

    // Table pruning removes the referenced side of a join on a foreign key when only the key is read from it, but
    // not when that side names partitions, tablets or replicas: it then reads only part of the table and can miss
    // rows the foreign key refers to.
    @Test
    public void testReferencedScanNamingPartitionsTabletsOrReplicasIsNotRemoved() {
        List<Long> p1 = tabletsOf("uk_partitioned", "p1");
        long tablet = p1.get(0);
        long replica = replicaOf("uk_partitioned", "p1", tablet);
        long referencingTablet = tabletsOf("fk_to_uk_partitioned", "fk_to_uk_partitioned").get(0);
        String join = "select f.id, f.fk from fk_to_uk_partitioned f join uk_partitioned";
        Object[][] cases = {
                // statement, the tablets each scan of uk_partitioned reads
                {join + " partition (p1) u on f.fk = u.pk", List.of(p1)},
                {join + " tablet (" + tablet + ") u on f.fk = u.pk", List.of(List.of(tablet))},
                {join + " replica (" + replica + ") u on f.fk = u.pk", List.of(List.of(tablet))},
                // The referencing side may name some of its tablets: the referenced side is still removed.
                {"select f.id, f.fk from fk_to_uk_partitioned tablet (" + referencingTablet + ") f " +
                        "join uk_partitioned u on f.fk = u.pk", List.of()},
        };
        assertScannedTablets("uk_partitioned", List.of(CBO_WITHOUT_JOIN_REORDER, CBO_WITH_JOIN_REORDER, RBO), cases);
    }

    // Plans each statement under each mode and checks the tablets each scan of the table reads, reporting every
    // statement and mode that reads something else.
    @SuppressWarnings("unchecked")
    private static void assertScannedTablets(String table, List<PruningMode> modes, Object[][] cases) {
        List<Executable> checks = Lists.newArrayList();
        for (PruningMode mode : modes) {
            for (Object[] c : cases) {
                checks.add(() -> {
                    mode.settings().accept(ctx.getSessionVariable());
                    try {
                        ExecPlan plan = UtFrameUtils.getPlanAndFragment(ctx, (String) c[0]).second;
                        Assertions.assertEquals(sortedScans((List<List<Long>>) c[1]), scannedTablets(plan, table),
                                mode.name() + ": " + c[0]);
                    } finally {
                        SessionVariable sv = ctx.getSessionVariable();
                        sv.setEnableCboTablePrune(false);
                        sv.setEnableRboTablePrune(false);
                        sv.setEnableTablePruneOnUpdate(false);
                        sv.enableJoinReorder();
                    }
                });
            }
        }
        Assertions.assertAll(checks);
    }

    // The tablets each scan of the table reads, taken from its scan ranges: REPLICA does not narrow the tablets a scan
    // selects, it drops the scan ranges of the tablets that have none of the named replicas.
    private static List<List<Long>> scannedTablets(ExecPlan plan, String table) {
        return sortedScans(plan.getScanNodes().stream()
                .filter(scan -> scan instanceof OlapScanNode && scan.getTableName().equals(table))
                .map(scan -> ((OlapScanNode) scan).getScanRangeLocations(0).stream()
                        .map(range -> range.getScan_range().getInternal_scan_range().getTablet_id())
                        .collect(Collectors.toList()))
                .collect(Collectors.toList()));
    }

    // Sorts the tablets of each scan, and then the scans, so that scans compare in whatever order a plan lists them.
    private static List<List<Long>> sortedScans(List<List<Long>> scans) {
        return scans.stream()
                .map(tablets -> tablets.stream().sorted().collect(Collectors.toList()))
                .sorted(Comparator.<List<Long>>comparingInt(List::size).thenComparing(Object::toString))
                .collect(Collectors.toList());
    }

    private static List<Long> tabletsOf(String table, String... partitions) {
        OlapTable olapTable = olapTableOf(table);
        return Arrays.stream(partitions)
                .flatMap(partition -> olapTable.getPartition(partition).getDefaultPhysicalPartition()
                        .getQueryableBaseIndex().getTabletIdsInOrder().stream())
                .sorted()
                .collect(Collectors.toList());
    }

    private static long replicaOf(String table, String partition, long tablet) {
        return olapTableOf(table).getPartition(partition).getDefaultPhysicalPartition().getQueryableBaseIndex()
                .getTablet(tablet).getAllReplicas().get(0).getId();
    }

    private static OlapTable olapTableOf(String table) {
        return (OlapTable) ctx.getGlobalStateMgr().getLocalMetastore().getDb("table_prune_db").getTable(table);
    }

    @Test
    public void testViewPruneOneFromTwoTables() throws Exception {
        String sqlList[] = {
                "select empid from emps_flat_view",
                "select empid, deptno2 from emps_flat_view",
                "select r0.* from emps r0 inner join emps r1 on r0.empid=r1.empid"
        };
        for (String sql : sqlList) {
            checkHashJoinCountWithOnlyRBO(sql, 0);
            checkHashJoinCountWithOnlyCBO(sql, 0);
            checkHashJoinCountWithBothRBOAndCBO(sql, 0);
        }
    }

    @Test
    public void testViewSsbPrune() {
        Object[][] cases = new Object[][] {
                {"select 1 from lineorder_flat_v1", 0, 0},
                {"select 1 from lineorder_flat_v2", 0, 0},
                {"select 1 from lineorder_flat_v1 limit 10", 0, 0},
                {"select lo_orderdate from lineorder_flat_v1", 0, 0},
                {"select lo_orderdate from lineorder_flat_v2", 0, 0},
                {"select lo_orderdate from lineorder_flat_v2 limit 10", 0, 0},
                {"select lo_orderdate from lineorder_flat_v2 where lo_orderdate > 19980101", 0, 0},
                {"select lo_orderdate,c_address from lineorder_flat_v1", 1, 1},
                {"select lo_orderdate,s_name from lineorder_flat_v2", 1, 1},
                {"select lo_orderdate,c_address,s_name from lineorder_flat_v1", 2, 2},
                {"select lo_orderdate,c_address,s_name from lineorder_flat_v2", 2, 2},
                {"select lo_orderdate,c_address,s_name from lineorder_flat_v2 where c_city like 'A%' limit 10", 2, 3},
                {"select c_address,s_name from lineorder_flat_v1", 2, 2},
                {"select c_address,s_name from lineorder_flat_v2 where s_nation = 'USA' limit 100", 2, 3},
        };
        for (Object[] tc : cases) {
            String sql = (String) tc[0];
            int rboNumHashJoins = (Integer) tc[1];
            int cboNumHashJoins = (Integer) tc[2];
            checkHashJoinCountWithOnlyRBO(sql, rboNumHashJoins);
            checkHashJoinCountWithOnlyCBO(sql, cboNumHashJoins);
            checkHashJoinCountWithBothRBOAndCBO(sql, rboNumHashJoins);
        }
    }

    @Test
    public void testViewSsbPruneBasedOnUniqueKeys() {
        String fromClause0 = "lineorder0 l\n" +
                "   left join customer c on (c.c_custkey = l.lo_custkey)\n" +
                "   left join supplier s  on (s.s_suppkey = l.lo_suppkey)\n" +
                "   left join part p on (p.p_partkey = l.lo_partkey)";

        String fromClause1 =
                "(select t1.*,p.* from part p right join \n" +
                        "(select t0.*, s.*  from supplier s right join \n" +
                        "(select c.*, l.* from customer c right join lineorder0 l \n" +
                        "   on (c.c_custkey = l.lo_custkey)) t0\n" +
                        "   on (s.s_suppkey = t0.lo_suppkey)) t1\n" +
                        "   on (p.p_partkey = t1.lo_partkey)) t2";

        String[] whereClauses = {
                "lo_custkey > 10",
                "lo_custkey > 10 and lo_custkey > 10",
                "lo_custkey > 10 and lo_custkey > 10 and lo_partkey > 10",
        };

        Object[][] items = new Object[][] {
                {"1", 0, 0},
                {"lo_orderdate", 0, 0},
                {"lo_orderdate,c_address", 1, 1},
                {"lo_orderdate,s_name", 1, 1},
                {"lo_orderdate,c_address,s_name", 2, 2},
                {"c_address,s_name", 2, 2},
        };
        List<Object[]> cases =
                Stream.of(fromClause0, fromClause1).flatMap(fromClause -> Stream.of(whereClauses).flatMap(
                        whereClause -> Stream.of(items).map(tc -> new Object[] {
                                String.format("select %s from %s where %s", tc[0], fromClause, whereClause), tc[1],
                                tc[2]}))).collect(
                        Collectors.toList());

        String sql0 = "select lo_orderdate,s_name from lineorder0 l\n" +
                "   left join customer c on (c.c_custkey = l.lo_custkey)\n" +
                "   left join supplier s  on (s.s_suppkey = l.lo_suppkey)\n" +
                "   left join part p on (p.p_partkey = l.lo_partkey) where lo_custkey > 10";
        for (Object[] tc : cases) {
            String sql = (String) tc[0];
            int cboNumHashJoins = (Integer) tc[2];
            checkHashJoinCountWithOnlyRBO(sql, cboNumHashJoins);
            checkHashJoinCountWithBothRBOAndCBO(sql, cboNumHashJoins);
        }
    }

    @Test
    public void testOuterJoinContainsConstantPredicate() {
        String[] sqlList = {
                "select l.*\n" +
                        "from lineorder l left join[shuffle] (select * from customer where murmur_hash3_32(10) = 10) c on (c.c_custkey = l.lo_custkey)\n" +
                        "where murmur_hash3_32(10) = 10",

                "select l.*\n" +
                        "from (select * from customer where murmur_hash3_32(10) = 10) c right join[shuffle] lineorder l on (c.c_custkey = l.lo_custkey)\n" +
                        "where murmur_hash3_32(10) = 10",
        };
        for (String sql : sqlList) {
            checkHashJoinCountWithOnlyRBO(sql, 0);
            checkHashJoinCountWithOnlyCBO(sql, 0);
            checkHashJoinCountWithBothRBOAndCBO(sql, 0);
        }
    }

    @Test
    public void testInterpolateProjection() {
        Object[][] cases = new Object[][] {
                {"select c.c_custkey, l.lo_custkey from lineorder l inner join customer c on (c.c_custkey = l.lo_custkey)",
                        0, 0}
        };
        for (Object[] tc : cases) {
            String sql = (String) tc[0];
            int rboNumHashJoins = (Integer) tc[1];
            int cboNumHashJoins = (Integer) tc[2];
            checkHashJoinCountWithOnlyRBO(sql, rboNumHashJoins);
            checkHashJoinCountWithOnlyCBO(sql, cboNumHashJoins);
            checkHashJoinCountWithBothRBOAndCBO(sql, rboNumHashJoins);
        }
    }

    @Test
    public void testAggregateOverMultiJoin() {
        String sqlFmt = "select %s from  \n" +
                "A A0 \n" +
                "join A A1 on A0.a_pk = A1.a_pk\n" +
                "join A A2 on A0.a_pk = A2.a_pk\n" +
                "join D D0 on A2.a_pk = D0.d_pk\n" +
                "join D D1 on D0.d_pk = D1.d_pk\n" +
                "join D D2 on D0.d_pk = D2.d_pk\n" +
                "join B on A0.a_b_fk = B.b_pk\n" +
                "join CI on B.b_ci_fk = CI.ci_pk\n" +
                "join CII on B.b_cii_fk0 = CII.cii_pk0 and B.b_cii_fk1 = CII.cii_pk1\n" +
                "join E on D0.d_e_fk = E.e_pk\n" +
                "join FI on E.e_fi_fk = FI.fi_pk\n" +
                "join FII on E.e_fii_fk0 = FII.fii_pk0 and E.e_fii_fk1 = FII.fii_pk1\n" +
                "where %s";

        Object[][] testCases = new Object[][] {
                {"avg(A0.a_c0+A1.a_c1), sum(A2.a_c2), max(B.b_pk), min(D0.d_c0)", "true", 1, 8, 1},
                {"count(1)", "true", 0, 8, 0},
                {"sum(1)", "true", 0, 8, 0},
                {"avg(A0.a_c0+A1.a_c1), sum(A2.a_c2), max(B.b_pk), min(D0.d_c0), min(D1.d_c1)", "D2.d_c3>100", 1, 8, 1},
        };
        for (Object[] tc : testCases) {
            String sql = String.format(sqlFmt, tc[0], tc[1]);
            int rboNumHashJoins = (Integer) tc[2];
            int cboNumHashJoins = (Integer) tc[3];
            int finalNmHashJoins = (Integer) tc[4];
            //checkHashJoinCountWithOnlyRBO(sql, rboNumHashJoins);
            //checkHashJoinCountWithOnlyCBO(sql, cboNumHashJoins);
            checkHashJoinCountWithBothRBOAndCBO(sql, finalNmHashJoins);
        }
    }

    @Test
    public void testPermuteFromClausesRandomlyAndMoveOnJoinPredicatesToWhereClauses() throws Exception {
        List<String> tables = Lists.newArrayList("A A0",
                "A A1",
                "A A2",
                "D D0",
                "D D1",
                "D D2",
                "B",
                "CI",
                "CII",
                "E",
                "FI",
                "FII");

        Object[][] testCases = new Object[][] {
                {"avg(A0.a_c0+A1.a_c1), sum(A2.a_c2), max(B.b_pk), min(D0.d_c0), min(D1.d_c1)", "", 1, 1},
                {"A0.a_c0+A1.a_c1, A2.a_c2, B.b_pk, D0.d_c0, D1.d_c1", "limit 1", 1, 1},
                {"sum(FII.fii_c0), sum(FII.fii_c0+1)", "limit 1", 1, 1},
                {"sum(FII.fii_c0), sum(CII.cii_c0)", "limit 1", 1, 1, 1},
        };
        String[] whereClauses = new String[] {
                "A0.a_pk = A2.a_pk AND A0.a_pk = A1.a_pk AND E.e_fii_fk1 = FII.fii_pk1 AND D0.d_e_fk = E.e_pk " +
                        "AND D0.d_pk = D2.d_pk AND A2.a_pk = D0.d_pk AND D0.d_pk = D1.d_pk AND A0.a_b_fk = B.b_pk " +
                        "AND B.b_cii_fk1 = CII.cii_pk1 AND B.b_ci_fk = CI.ci_pk AND B.b_cii_fk0 = CII.cii_pk0 " +
                        "AND D2.d_c3>100 AND E.e_fi_fk = FI.fi_pk AND E.e_fii_fk0 = FII.fii_pk0",
                "A0.a_pk = A1.a_pk AND D0.d_pk = D1.d_pk AND A0.a_pk = A2.a_pk AND D0.d_pk = D2.d_pk " +
                        "AND D0.d_e_fk = E.e_pk AND A2.a_pk = D0.d_pk AND D2.d_c3>100 AND A0.a_b_fk = B.b_pk " +
                        "AND B.b_ci_fk = CI.ci_pk AND B.b_cii_fk1 = CII.cii_pk1 AND B.b_cii_fk0 = CII.cii_pk0 " +
                        "AND E.e_fii_fk0 = FII.fii_pk0 AND E.e_fi_fk = FI.fi_pk AND E.e_fii_fk1 = FII.fii_pk1",
                "A0.a_pk = A1.a_pk AND E.e_fii_fk0 = FII.fii_pk0 AND A0.a_b_fk = B.b_pk AND A0.a_pk = A2.a_pk " +
                        "AND B.b_cii_fk0 = CII.cii_pk0 AND B.b_cii_fk1 = CII.cii_pk1 AND D2.d_c3>100 " +
                        "AND B.b_ci_fk = CI.ci_pk AND D0.d_pk = D1.d_pk AND A2.a_pk = D0.d_pk " +
                        "AND E.e_fi_fk = FI.fi_pk AND D0.d_pk = D2.d_pk AND D0.d_e_fk = E.e_pk " +
                        "AND E.e_fii_fk1 = FII.fii_pk1",
                "A0.a_pk = A1.a_pk AND A0.a_pk = A2.a_pk AND D2.d_c3>100 AND A2.a_pk = D0.d_pk " +
                        "AND D0.d_pk = D1.d_pk AND D0.d_e_fk = E.e_pk AND E.e_fi_fk = FI.fi_pk " +
                        "AND D0.d_pk = D2.d_pk AND E.e_fii_fk0 = FII.fii_pk0 AND A0.a_b_fk = B.b_pk " +
                        "AND B.b_ci_fk = CI.ci_pk AND B.b_cii_fk1 = CII.cii_pk1 AND B.b_cii_fk0 = CII.cii_pk0 " +
                        "AND E.e_fii_fk1 = FII.fii_pk1",
                "A0.a_pk = A1.a_pk AND A0.a_pk = A2.a_pk AND D0.d_pk = D1.d_pk AND D2.d_c3>100 " +
                        "AND A2.a_pk = D0.d_pk AND A0.a_b_fk = B.b_pk AND D0.d_pk = D2.d_pk " +
                        "AND B.b_ci_fk = CI.ci_pk AND B.b_cii_fk0 = CII.cii_pk0 AND D0.d_e_fk = E.e_pk " +
                        "AND B.b_cii_fk1 = CII.cii_pk1 AND E.e_fi_fk = FI.fi_pk AND E.e_fii_fk0 = FII.fii_pk0 " +
                        "AND E.e_fii_fk1 = FII.fii_pk1",

        };
        String sqlFmt = "select %s from %s where %s %s";
        for (Object[] tc : testCases) {
            String selectItems = (String) tc[0];
            String extraLimit = (String) tc[1];
            int rboNumHashJoins = (Integer) tc[2];
            int finalNmHashJoins = (Integer) tc[3];

            for (String whereClause : whereClauses) {
                String fromClause = tables.stream().collect(Collectors.joining(",\n"));
                String sql = String.format(sqlFmt, selectItems, fromClause, whereClause, extraLimit);
                checkHashJoinCountWithBothRBOAndCBOLessThan(sql, 12);
            }
        }
    }

    @Test
    public void testFKReferencedMutually() throws Exception {
        Object[][] cases = {
                {"select A.a_pk, D.d_pk from A join D on a_pk = d_pk", 0},
                {"select A.a_pk, D.d_pk from A join D on a_pk = d_pk where A.a_c0>0", 1},
                {"select A.a_pk, D.d_pk from A join D on a_pk = d_pk where A.a_c0>0 and A.a_c1 < 10", 2},
                {"select A0.a_pk, D.d_pk from A A0 inner join A A1 on " +
                        "A0.a_pk = A1.a_pk join D on A0.a_pk = d_pk where A0.a_c0>0 and A1.a_c1 < 10", 2},
                {"select A0.a_pk, D0.d_pk from A A0 inner join A A1 on " +
                        "A0.a_pk = A1.a_pk join D D0 on A0.a_pk = D0.d_pk join D D1 on D0.d_pk = D1.d_pk " +
                        "where A0.a_c0>0 and A1.a_c1 < 10", 2}
        };

        Pattern exprPat = Pattern.compile("\\[\\d+:");
        for (Object[] tc : cases) {
            String sql = (String) tc[0];
            int numPredicates = (Integer) tc[1];
            String plan = checkHashJoinCountWithBothRBOAndCBO(sql, 0);
            Optional<String> predLn = Stream.of(plan.split("\n")).filter(ln -> ln.contains("Predicates:")).findFirst();
            Assertions.assertTrue(numPredicates == 0 || predLn.isPresent());
            if (predLn.isPresent()) {
                String ln = predLn.get();
                Matcher matcher = exprPat.matcher(ln);
                for (int i = 0; i < numPredicates; ++i) {
                    Assertions.assertTrue(matcher.find());
                }
            }
        }
    }

    @Test
    public void testCaseCboTwoSameTableJoinOnTheSameKey() throws Exception {
        List<String> selectItems = Lists.newArrayList(
                "A0.a_c0, A1.a_c1, A0.a_c2 + A1.a_c3",
                "A0.a_c0, A0.a_c1, A0.a_c2 + A0.a_c3",
                "A1.a_c0, A1.a_c1, A1.a_c2 + A1.a_c3");

        List<String> fromClauses = Lists.newArrayList(
                "A A0 join A A1 on A1.a_pk = A0.a_pk",
                "A A0 left join A A1 on A1.a_pk = A0.a_pk",
                "A A0 right join A A1 on A1.a_pk = A0.a_pk");

        // todo table prune not work when move around predicates from join conditions
        List<String> whereClauses = Lists.newArrayList(
                "true",
                "false",
                // "A0.a_pk>10",
                // "A1.a_pk>10",
                // "murmur_hash3_32(A0.a_pk)>10",
                // "murmur_hash3_32(A1.a_pk)>10",
                "murmur_hash3_32(A1.a_pk)>murmur_hash3_32(A1.a_pk)",
                "murmur_hash3_32(A0.a_pk)>10 and murmur_hash3_32(A1.a_pk)>10",
                "murmur_hash3_32(A0.a_c0)>10 and murmur_hash3_32(A0.a_c1)>10",
                "murmur_hash3_32(A0.a_c0)>10 and murmur_hash3_32(A1.a_c2)>10");

        List<String> queryList = selectItems.stream().flatMap(s -> fromClauses.stream()
                        .flatMap(f -> whereClauses.stream().map(w -> Lists.newArrayList(s, f, w))))
                .map(param -> String.format("select %s from %s where %s", param.get(0), param.get(1), param.get(2)))
                .collect(Collectors.toList());

        queryList.forEach(q -> {
            try {
                checkHashJoinCountWithBothRBOAndCBO(q, 0);
            } catch (Exception e) {
                Assertions.fail("Query=" + q + " failed to plan");
            }
        });
    }

    @Test
    public void testCaseCboThreeSameTableJoinOnTheSameKey() throws Exception {
        List<String> selectItems = Lists.newArrayList(
                "a0_pk, a1_pk, a2_pk, a0_c0, a1_c1, a2_c2, a0_c1",
                "a0_pk",
                "a1_pk, a2_pk",
                "sum(a0_c0+a1_c1+a2_c2+a0_c1)");

        List<String> fromClauses = Lists.newArrayList(
                "select A0.a_pk as a0_pk, A1.a_pk as a1_pk, A2.a_pk as a2_pk, " +
                        "murmur_hash3_32(A0.a_c0+A0.a_c1) as a0_c0, " +
                        "murmur_hash3_32(A1.a_c1+A1.a_c2) as a1_c1, " +
                        "murmur_hash3_32(A2.a_c2+A2.a_c3) as a2_c2, " +
                        "murmur_hash3_32(A0.a_c1+A1.a_c2+A2.a_c2) as a0_c1 " +
                        "from A A0, A A1, A A2 where A0.a_pk = A1.a_pk and A1.a_pk = A2.a_pk",
                "select A0.a_pk as a0_pk, A1.a_pk as a1_pk, A2.a_pk as a2_pk, " +
                        "a0_c0, a1_c1, a2_c2, murmur_hash3_32(A0.a_c0+A1.a_c1+A2.a_c2) as a0_c1 " +
                        "from " +
                        "   (select a_pk, a_c0, murmur_hash3_32(a_c0+a_c0) as a0_c0 from A) A0, " +
                        "   (select a_pk, a_c1, murmur_hash3_32(a_c1+a_c2) as a1_c1 from A) A1," +
                        "   (select a_pk, a_c2, murmur_hash3_32(a_c2+a_c3) as a2_c2 from A) A2 " +
                        "where A0.a_pk = A1.a_pk and A1.a_pk = A2.a_pk"
        );

        List<String> whereClauses = Lists.newArrayList(
                "true",
                "false",
                "a0_pk>10",
                "a1_pk>10 and a2_pk > 10",
                "murmur_hash3_32(a1_pk)>10",
                "murmur_hash3_32(a0_c0+a1_c1+a2_c2)>10",
                "murmur_hash3_32(a0_c0)>murmur_hash3_32(a1_c1)",
                "murmur_hash3_32(a0_c1)>10 and murmur_hash3_32(a0_c0+a1_c1+a2_c2)>10");

        List<String> queryList = selectItems.stream().flatMap(s -> fromClauses.stream()
                        .flatMap(f -> whereClauses.stream().map(w -> Lists.newArrayList(s, f, w))))
                .map(param -> String.format("select %s from (%s) t where %s", param.get(0), param.get(1), param.get(2)))
                .collect(Collectors.toList());

        queryList.forEach(q -> {
            checkHashJoinCountWithOnlyRBOLessThan(q, 4);
            checkHashJoinCountWithBothRBOAndCBOLessThan(q, 2);
        });
    }

    @Test
    public void testCaseCboFKJoinPKOfTwoTables() throws Exception {
        List<String> selectItems = Lists.newArrayList(
                "e_c0",
                "e_c0, e_c1",
                "e_c0, e_fii_fk0, fii_pk0",
                "fii_pk0, fii_pk1");

        List<String> fromClauses = Lists.newArrayList(
                "select E.*, FII.* from E join FII on E.e_fii_fk0 = FII.fii_pk0 and E.e_fii_fk1 = FII.fii_pk1"
        );

        List<String> whereClauses = Lists.newArrayList(
                "true",
                "false",
                "fii_pk0>10",
                "fii_pk0<10 and fii_pk1>10"
        );

        List<String> queryList = selectItems.stream().flatMap(s -> fromClauses.stream()
                        .flatMap(f -> whereClauses.stream().map(w -> Lists.newArrayList(s, f, w))))
                .map(param -> String.format("select %s from (%s) t where %s", param.get(0), param.get(1), param.get(2)))
                .collect(Collectors.toList());

        queryList.forEach(q -> {
            checkHashJoinCountWithOnlyRBO(q, 0);
            checkHashJoinCountWithBothRBOAndCBO(q, 0);
        });
    }

    @Test
    public void testCboFKOuterJoinPKOfTwoTablesPruneFail() throws Exception {
        List<String> selectItems = Lists.newArrayList(
                "e_c0, fii_c0",
                "e_c0, e_c1, fii_c0",
                "e_c0+e_fii_fk0+fii_pk0+fii_c0",
                "fii_pk0, fii_pk1, murmur_hash3_32(fii_c0)");

        List<String> fromClauses = Lists.newArrayList(
                "select E.*, FII.* from E left join[shuffle] FII on E.e_fii_fk0 = FII.fii_pk0 and E.e_fii_fk1 = FII.fii_pk1",
                "select E.*, FII.* from FII right join[shuffle] E on E.e_fii_fk0 = FII.fii_pk0 and E.e_fii_fk1 = FII.fii_pk1"
        );

        List<String> whereClauses = Lists.newArrayList(
                "fii_pk0>10",
                "fii_pk0<10 and fii_pk1>10"
        );

        List<String> queryList = selectItems.stream().flatMap(s -> fromClauses.stream()
                        .flatMap(f -> whereClauses.stream().map(w -> Lists.newArrayList(s, f, w))))
                .map(param -> String.format("select %s from (%s) t where %s", param.get(0), param.get(1), param.get(2)))
                .collect(Collectors.toList());

        queryList.forEach(q -> {
            checkHashJoinCountWithOnlyRBO(q, 1);
            checkHashJoinCountWithBothRBOAndCBO(q, 1);
        });
    }

    @Test
    public void testCboFKOuterJoinPKOfTwoTablesPruneSuccess() throws Exception {
        ctx.getSessionVariable().setEnableRboTablePrune(true);
        ctx.getSessionVariable().setOptimizerExecuteTimeout(3000000);

        List<String> selectItems = Lists.newArrayList(
                "e_c0, e_fi_fk",
                "e_c0, e_fi_fk, e_fii_fk0",
                "e_fii_fk0+murmur_hash3_32(e_fii_fk1)",
                "murmur_hash3_32(e_fii_fk0+e_fii_fk1)");

        List<String> fromClauses = Lists.newArrayList(
                "select E.*, FII.* from E left join[shuffle] FII on E.e_fii_fk0 = FII.fii_pk0 and E.e_fii_fk1 = FII.fii_pk1",
                "select E.*, FII.* from FII right join[shuffle] E on E.e_fii_fk0 = FII.fii_pk0 and E.e_fii_fk1 = FII.fii_pk1"
        );

        List<String> whereClauses = Lists.newArrayList(
                "true",
                "false",
                "e_c0>10",
                "e_fii_fk0>10",
                "e_fii_fk0<10 and e_fii_fk1>10"
        );

        List<String> queryList = selectItems.stream().flatMap(s -> fromClauses.stream()
                        .flatMap(f -> whereClauses.stream().map(w -> Lists.newArrayList(s, f, w))))
                .map(param -> String.format("select %s from (%s) t where %s", param.get(0), param.get(1), param.get(2)))
                .collect(Collectors.toList());

        queryList.forEach(q -> {
            checkHashJoinCountWithOnlyRBO(q, 0);
            checkHashJoinCountWithBothRBOAndCBO(q, 0);
        });
    }

    @Test
    public void testManyTablesJoinOnTheSamePK() {
        String sql100Tables =
                generateSameTableJoinSql(100, "A t%d", (l, r) -> String.format("on t%d.a_pk = t%d.a_pk", l, r),
                        "t0.a_pk, t1.a_c0",
                        "t2.a_c1>10 and t99.a_c2>100", "limit 1");
        String sql500Tables = generateSameTableJoinSql(100, "FII t%d",
                (l, r) -> String.format("on t%d.fii_pk0 = t%d.fii_pk0 and t%d.fii_pk1 = t%d.fii_pk1", l, r, l, r),
                "t0.fii_c0, t99.fii_pk0", "t2.fii_c0>10 and t98.fii_c0 >100", "limit 1");
        checkHashJoinCountWithOnlyRBO(sql100Tables, 0);
        checkHashJoinCountWithOnlyRBO(sql500Tables, 0);
    }

    @Test
    public void testIntraGroupJoiningOnTheSamePKAndInterGroupLeftJoiningOnFKPK() {
        String fromClause = "" +
                "A A0 inner join A A1 on A0.a_pk = A1.a_pk\n" +
                "left join B B0 on A1.a_b_fk = B0.b_pk inner join B B1 on B0.b_pk = B1.b_pk\n" +
                "inner join B B2 on B1.b_pk = B2.b_pk\n" +
                "left join CI CI0 on B2.b_ci_fk = CI0.ci_pk inner join CI CI1 on CI0.ci_pk = CI1.ci_pk\n" +
                "inner join CI CI2 on CI1.ci_pk = CI2.ci_pk inner join CI CI3 on CI2.ci_pk = CI3.ci_pk\n" +
                "left join CII CII0 on B0.b_cii_fk0 = CII0.cii_pk0 and B0.b_cii_fk1 = CII0.cii_pk1\n" +
                "inner join CII CII1 on CII0.cii_pk0 = CII1.cii_pk0 and CII0.cii_pk1 = CII1.cii_pk1\n" +
                "inner join CII CII2 on CII1.cii_pk0 = CII2.cii_pk0 and CII1.cii_pk1 = CII2.cii_pk1\n" +
                "inner join CII CII3 on CII2.cii_pk0 = CII3.cii_pk0 and CII2.cii_pk1 = CII3.cii_pk1\n";
        Object[][] cases = {
                {"sum(coalesce(CII2.cii_c0,0)+coalesce(CII1.cii_pk0, 0))", 2},
                {"sum(coalesce(CII2.cii_c0,0)+coalesce(CI0.ci_c0,0))", 3},
                {"sum(coalesce(B2.b_c0,0)+coalesce(B1.b_c0,0))", 1},
                {"sum(A0.a_c0+A1.a_c1)", 0},
        };
        try {
            ctx.getSessionVariable().setOptimizerExecuteTimeout(20000);
            for (Object[] tc : cases) {
                String selectStmt = (String) tc[0];
                int numHashJoins = (Integer) tc[1];
                String sql = String.format("select %s from %s", selectStmt, fromClause);
                checkHashJoinCountWithOnlyRBOLessThan(sql, 12);
            }
        } finally {
            ctx.getSessionVariable().setOptimizerExecuteTimeout(3000);
        }
    }

    @Test
    public void testIntraGroupJoiningOnTheSamePKAndInterGroupInnerJoiningOnFKPK() {
        String fromClause = "" +
                "A A0 inner join A A1 on A0.a_pk = A1.a_pk\n" +
                "inner join B B0 on A1.a_b_fk = B0.b_pk inner join B B1 on B0.b_pk = B1.b_pk\n" +
                "inner join B B2 on B1.b_pk = B2.b_pk\n" +
                "inner join CI CI0 on B2.b_ci_fk = CI0.ci_pk inner join CI CI1 on CI0.ci_pk = CI1.ci_pk\n" +
                "inner join CI CI2 on CI1.ci_pk = CI2.ci_pk inner join CI CI3 on CI2.ci_pk = CI3.ci_pk\n" +
                "inner join CII CII0 on B0.b_cii_fk0 = CII0.cii_pk0 and B0.b_cii_fk1 = CII0.cii_pk1\n" +
                "inner join CII CII1 on CII0.cii_pk0 = CII1.cii_pk0 and CII0.cii_pk1 = CII1.cii_pk1\n" +
                "inner join CII CII2 on CII1.cii_pk0 = CII2.cii_pk0 and CII1.cii_pk1 = CII2.cii_pk1\n" +
                "inner join CII CII3 on CII2.cii_pk0 = CII3.cii_pk0 and CII2.cii_pk1 = CII3.cii_pk1\n";
        Object[][] cases = {
                {"sum(coalesce(CII2.cii_c0,0)+coalesce(CII1.cii_pk0, 0))", 2},
                {"sum(coalesce(CII2.cii_c0,0)+coalesce(CI0.ci_c0,0))", 3},
                {"sum(coalesce(B2.b_c0,0)+coalesce(B1.b_c0,0))", 1},
                {"sum(A0.a_c0+A1.a_c1)", 0},
        };
        for (Object[] tc : cases) {
            String selectStmt = (String) tc[0];
            int numHashJoins = (Integer) tc[1];
            String sql = String.format("select %s from %s", selectStmt, fromClause);
            checkHashJoinCountWithOnlyRBO(sql, numHashJoins);
        }
    }

    @Test
    public void testSameTablesLeftJoinOnTheSamePK() {
        Object[][] cases = new Object[][] {
                {"select 1 from customer a left join customer b on a.c_custkey = b.c_custkey", 0, 0},
                {"select a.c_name from customer a left join customer b on a.c_custkey = b.c_custkey", 0, 0},
                {"select murmur_hash3_32(a.c_name)+murmur_hash3_32(a.c_custkey) " +
                        "from customer a left join customer b on a.c_custkey = b.c_custkey",
                        0, 0},
                {"select 1 from customer a right join customer b on a.c_custkey = b.c_custkey", 0, 0},
                {"select b.c_name from customer a right join customer b on a.c_custkey = b.c_custkey", 0, 0},
                {"select murmur_hash3_32(b.c_name)+murmur_hash3_32(b.c_custkey) " +
                        "from customer a right join customer b on a.c_custkey = b.c_custkey",
                        0, 0},

                {"select b.c_name from customer a left join customer b on a.c_custkey = b.c_custkey", 1, 0},
                {"select murmur_hash3_32(b.c_name)+murmur_hash3_32(b.c_custkey) " +
                        "from customer a left join customer b on a.c_custkey = b.c_custkey",
                        1, 0},
                {"select a.c_name from customer a right join customer b on a.c_custkey = b.c_custkey", 1, 0},
                {"select murmur_hash3_32(a.c_name)+murmur_hash3_32(a.c_custkey) " +
                        "from customer a right join customer b on a.c_custkey = b.c_custkey",
                        1, 0},

                {"select 1 from " +
                        "customer a left join " +
                        "(select c_custkey from customer where c_name = 'USA') b " +
                        "on a.c_custkey = b.c_custkey", 0, 0},
                {"select a.c_name from " +
                        "customer a left join " +
                        "(select c_custkey from customer where c_name = 'USA') b " +
                        "on a.c_custkey = b.c_custkey", 0, 0},
                {"select coalesce(b.c_custkey,-1) as custkey from " +
                        "customer a left join " +
                        "(select c_custkey from customer where c_name = 'USA') b " +
                        "on a.c_custkey = b.c_custkey", 1, 1},
                {"select 1 from " +
                        "customer a left join " +
                        "(select c_custkey from customer limit 10) b " +
                        "on a.c_custkey = b.c_custkey", 1, 1},
                {"select a.c_name from " +
                        "customer a left join " +
                        "(select c_custkey from customer limit 10) b " +
                        "on a.c_custkey = b.c_custkey", 1, 1},
                {"select coalesce(b.c_custkey,-1) as custkey from " +
                        "customer a left join " +
                        "(select c_custkey from customer limit 10) b " +
                        "on a.c_custkey = b.c_custkey", 1, 1},

                {"select 1 from " +
                        "customer a left join " +
                        "(select c_custkey from customer where c_name = 'USA' limit 10) b " +
                        "on a.c_custkey = b.c_custkey", 1, 1},
                {"select a.c_name from " +
                        "customer a left join " +
                        "(select c_custkey from customer where c_name = 'USA' limit 10) b " +
                        "on a.c_custkey = b.c_custkey", 1, 1},
                {"select coalesce(b.c_custkey,-1) as custkey from " +
                        "customer a left join " +
                        "(select c_custkey from customer where c_name = 'USA' limit 10) b " +
                        "on a.c_custkey = b.c_custkey", 1, 1},

        };
        for (Object[] tc : cases) {
            String sql = (String) tc[0];
            int rboNumHashJoins = (Integer) tc[1];
            int cboNumHashJoins = (Integer) tc[2];
            checkHashJoinCountWithOnlyRBO(sql, rboNumHashJoins);
            checkHashJoinCountWithOnlyCBO(sql, cboNumHashJoins);
            checkHashJoinCountWithBothRBOAndCBO(sql, cboNumHashJoins);
        }
    }

    @Test
    public void testExplainLogicalCloneOperator() throws Exception {
        String tabAA = "CREATE TABLE `AA` (\n" +
                "    `id` int(11) NOT NULL,\n" +
                "    `b_id` int(11) NOT NULL,\n" +
                "    `name` varchar(25) NOT NULL\n" +
                "    ) ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 10 PROPERTIES (\"replication_num\" = \"1\");\n";
        String tabBB = "CREATE TABLE `BB` (\n" +
                "      `id` int(11) NOT NULL,\n" +
                "      `name` varchar(25) NOT NULL,\n" +
                "      `age` varchar(25)\n" +
                "      ) ENGINE=OLAP\n" +
                "UNIQUE KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 10  PROPERTIES (\"replication_num\" = \"1\");";
        starRocksAssert.withTable(tabAA);
        starRocksAssert.withTable(tabBB);
        final boolean prev = ctx.getSessionVariable().isEnableGlobalLateMaterialization();
        try {
            starRocksAssert.alterTableProperties(
                    "alter table AA set(\"foreign_key_constraints\" = \"(b_id) REFERENCES BB(id)\");");
            String sql = "select AA.b_id, BB.id from AA inner join BB on AA.b_id = BB.id";
            ctx.getSessionVariable().setEnableCboTablePrune(true);
            ctx.getSessionVariable().setEnableGlobalLateMaterialization(false);
            String plan = UtFrameUtils.explainLogicalPlan(ctx, sql);
            Assertions.assertTrue(plan.contains("CLONE"), plan);
        } finally {
            ctx.getSessionVariable().setEnableGlobalLateMaterialization(prev);
            starRocksAssert.dropTable("AA");
            starRocksAssert.dropTable("BB");
        }
    }

    @Test
    public void testPruneWithPKUKTable1() throws Exception {
        ctx.getSessionVariable().setEnableCboTablePrune(true);

        final String tabAA = "CREATE TABLE `AA` (\n" +
                "    `id` int(11) NOT NULL,\n" +
                "    `id2` int(11) NOT NULL,\n" +
                "    `name` varchar(25) NOT NULL\n" +
                "    ) ENGINE=OLAP\n" +
                "PRIMARY KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 10 PROPERTIES (\"replication_num\" = \"1\");\n";
        final String tabBB = "CREATE TABLE `BB` (\n" +
                "      `id` int(11) NOT NULL,\n" +
                "      `id2` int(11) NOT NULL,\n" +
                "      `name` varchar(25) NOT NULL,\n" +
                "      `age` varchar(25)\n" +
                "      ) ENGINE=OLAP\n" +
                "UNIQUE KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 10  PROPERTIES (\"replication_num\" = \"1\");";
        starRocksAssert.withTable(tabAA);
        starRocksAssert.withTable(tabBB);
        final boolean prev = ctx.getSessionVariable().isEnableGlobalLateMaterialization();
        try {
            ctx.getSessionVariable().setEnableGlobalLateMaterialization(false);
            starRocksAssert.alterTableProperties(
                    "alter table AA set(\"foreign_key_constraints\" = \"AA(id2) REFERENCES BB(id)\");");
            final String sql = "select AA.id2, BB.id from AA inner join BB on AA.id2 = BB.id";
            final String plan = UtFrameUtils.explainLogicalPlan(ctx, sql);
            ctx.getSessionVariable().setEnableGlobalLateMaterialization(false);

            PlanTestBase.assertNotContains(plan, "BB");
            PlanTestBase.assertContains(plan, "CLONE");
        } finally {
            ctx.getSessionVariable().setEnableGlobalLateMaterialization(prev);
            starRocksAssert.dropTable("AA");
            starRocksAssert.dropTable("BB");
        }
    }

    @Test
    public void testPruneWithPKUKTable2() throws Exception {
        ctx.getSessionVariable().setEnableCboTablePrune(true);

        final String tabAA = "CREATE TABLE `AA` (\n" +
                "    `id` int(11) NOT NULL,\n" +
                "    `id2` int(11) NOT NULL,\n" +
                "    `id3` int(11) NOT NULL,\n" +
                "    `name` varchar(25) NOT NULL\n" +
                "    ) ENGINE=OLAP\n" +
                "PRIMARY KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 10 PROPERTIES (" +
                "   \"replication_num\" = \"1\"," +
                "   \"unique_constraints\" = \"id3\"" +
                ");\n";
        final String tabBB = "CREATE TABLE `BB` (\n" +
                "      `id` int(11) NOT NULL,\n" +
                "      `id2` int(11) NOT NULL,\n" +
                "      `name` varchar(25) NOT NULL,\n" +
                "      `age` varchar(25)\n" +
                "      ) ENGINE=OLAP\n" +
                "PRIMARY KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 10  PROPERTIES " +
                "(" +
                "   \"replication_num\" = \"1\"," +
                "   \"unique_constraints\" = \"id2\"" +
                ");";
        starRocksAssert.withTable(tabAA);
        starRocksAssert.withTable(tabBB);
        // add non primary key foreign key constraints should be ok
        starRocksAssert.alterTableProperties(
                "alter table AA set(\"unique_constraints\" = \"name\");");
        // add non primary key foreign key constraints should be ok
        starRocksAssert.alterTableProperties(
                "alter table AA set(\"foreign_key_constraints\" = \"AA(id2) REFERENCES BB(id2)\");");
        final boolean prev = ctx.getSessionVariable().isEnableGlobalLateMaterialization();

        try {
            ctx.getSessionVariable().setEnableGlobalLateMaterialization(false);
            // test table prune with non-primary keys
            {
                final String sql = "select AA.id2, BB.id2 from AA inner join BB on AA.id2 = BB.id2";
                String plan = UtFrameUtils.explainLogicalPlan(ctx, sql);
                PlanTestBase.assertNotContains(plan, "BB");
                PlanTestBase.assertContains(plan, "CLONE");
            }

            // test not table prune with non-fks
            {
                String sql = "select AA.id, BB.id from AA inner join BB on AA.id = BB.id";
                String plan = UtFrameUtils.explainLogicalPlan(ctx, sql);
                PlanTestBase.assertContains(plan, "BB");
                PlanTestBase.assertNotContains(plan, "CLONE");
            }
        } finally {
            ctx.getSessionVariable().setEnableGlobalLateMaterialization(prev);
            starRocksAssert.dropTable("AA");
            starRocksAssert.dropTable("BB");
        }

    }

    @Test
    public void testBug() throws Exception {
        String sql = "select\n" +
                "  cast(101 as int),\n" +
                "  cast(1 as bigint),\n" +
                "  dict_merge(`c_city`, 255) as _dict_merge_ \n" +
                "from customer[_META_]\n";
        ctx.getSessionVariable().setEnableCboTablePrune(true);
        ctx.getSessionVariable().setEnableRboTablePrune(true);
        String plan = UtFrameUtils.explainLogicalPlan(ctx, sql);
        Assertions.assertTrue(plan.contains("META-SCAN"), plan);
    }

    @Test
    public void testPruneWithSwapTables() throws Exception {
        ctx.getSessionVariable().setEnableCboTablePrune(true);
        ctx.getSessionVariable().setEnableRboTablePrune(true);
        String s1 = "CREATE TABLE s1 \n" +
                "(\n" +
                "    k1 int not null, k2 int, k3 int\n" +
                ")\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "DISTRIBUTED BY RANDOM \n" +
                "PROPERTIES(\"replication_num\" = \"1\", 'unique_constraints'='s1.k1');";
        String s2 = "CREATE TABLE s2 \n" +
                "(\n" +
                "    k1 int not null, k2 int, k3 int\n" +
                ")\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "DISTRIBUTED BY RANDOM \n" +
                "PROPERTIES(\"replication_num\" = \"1\", 'unique_constraints'='s2.k1');";
        String s3 = "CREATE TABLE s3 \n" +
                "(\n" +
                "    k1 int not null, k2 int, k3 int\n" +
                ")\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "DISTRIBUTED BY RANDOM \n" +
                "PROPERTIES(\"replication_num\" = \"1\", 'foreign_key_constraints'='s3(k1) REFERENCES s1(k1)');";
        starRocksAssert.withTable(s1);
        starRocksAssert.withTable(s2);
        starRocksAssert.withTable(s3);

        ctx.getSessionVariable().setOptimizerExecuteTimeout(300000);
        String sql = "select s3.k2 from s1 join s3 on s1.k1 = s3.k1;";
        {
            String plan = UtFrameUtils.explainLogicalPlan(ctx, sql);
            PlanTestBase.assertNotContains(plan, "s1");
        }

        String replaceStmt = "ALTER TABLE s2 SWAP WITH s1";
        starRocksAssert.alterTable(replaceStmt);
        {
            String plan = UtFrameUtils.explainLogicalPlan(ctx, sql);
            PlanTestBase.assertNotContains(plan, "s1");
        }
        starRocksAssert.dropTable("s1");
        starRocksAssert.dropTable("s2");
        starRocksAssert.dropTable("s3");
    }

    @Test
    public void testJoinMissingOtherConjuncts() throws Exception {
        String sql = "SELECT COUNT(col_a)\n" +
                "FROM view_a\n" +
                "WHERE col_x = 1002 AND col_tenant = 1001\n" +
                "LIMIT 10;";
        checkHashJoinCountWithBothRBOAndCBO(sql, 1);
        ctx.getSessionVariable().setEnableRboTablePrune(true);
        ctx.getSessionVariable().setEnableCboTablePrune(true);
        String plan = UtFrameUtils.getFragmentPlan(ctx, sql);
        Assertions.assertTrue(plan.contains("  3:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: col_a = 11: col_a\n" +
                "  |  equal join conjunct: 2: col_tenant = 12: col_tenant\n" +
                "  |  other predicates: coalesce(13: col_x, 3: col_x) = 1002"));
    }

    @Test
    public void testSelfInnerJoinWithOtherConjuncts() throws Exception {
        String sql = "select count(1) from \n" +
                "table_a a1 inner join table_a a2 \n" +
                "\ton a1.col_a = a2.col_a and \n" +
                "\ta1.col_tenant = a2.col_tenant and \n" +
                "\ta1.created_at < a2.updated_at;";
        checkHashJoinCountWithOnlyCBO(sql, 0);
        ctx.getSessionVariable().setEnableCboTablePrune(true);
        String plan = UtFrameUtils.getFragmentPlan(ctx, sql);
        Assertions.assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     TABLE: table_a\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 4: created_at < 5: updated_at"));
    }
}
