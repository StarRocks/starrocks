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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.common.PCell;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TExplainLevel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A materialized view over a table partitioned by expressions, which is list partitioned by hidden generated
 * columns, may be partitioned by its output columns whose definitions are those expressions, e.g. dt_hour of
 * date_trunc('hour', created_at) AS dt_hour on a table partitioned by (project_id, date_trunc('hour', created_at)),
 * where its query reads that table alone. Each such output column maps one to one to the generated partition column,
 * without any transform.
 */
public class MvListPartitionGeneratedColumnTest extends MVTestBase {
    // an automatic list partition on (project_id, hour)
    private static final String HOURLY_EVENTS = "CREATE TABLE hourly_events (\n" +
            "  project_id BIGINT NOT NULL,\n" +
            "  created_at DATETIME NOT NULL,\n" +
            "  campaign_id BIGINT,\n" +
            "  template_id BIGINT,\n" +
            "  email VARCHAR(128)\n" +
            ")\n" +
            "DUPLICATE KEY(project_id, created_at)\n" +
            "PARTITION BY (project_id, date_trunc('hour', created_at))\n" +
            "DISTRIBUTED BY HASH(project_id) BUCKETS 3";

    private static final String HOURLY_MV = "CREATE MATERIALIZED VIEW hourly_mv\n" +
            "PARTITION BY (project_id, dt_hour)\n" +
            "DISTRIBUTED BY HASH(project_id) BUCKETS 3\n" +
            "REFRESH DEFERRED MANUAL\n" +
            "AS SELECT project_id, campaign_id, template_id, date_trunc('hour', created_at) AS dt_hour,\n" +
            "  count(*) AS cnt, hll_union(hll_hash(email)) AS hll_email\n" +
            "FROM hourly_events\n" +
            "GROUP BY project_id, campaign_id, template_id, date_trunc('hour', created_at)";

    // an automatic list partition on (project bucket, day), as a duplicate key or a primary key table
    private static final String DAILY_EVENTS = "CREATE TABLE daily_events (\n" +
            "  project_id BIGINT NOT NULL,\n" +
            "  created_at DATETIME NOT NULL,\n" +
            "  msg_id BIGINT NOT NULL,\n" +
            "  campaign_id BIGINT,\n" +
            "  email VARCHAR(128)\n" +
            ")\n" +
            "%s KEY(project_id, created_at, msg_id)\n" +
            "PARTITION BY (mod(project_id, 16), date_trunc('day', created_at))\n" +
            "DISTRIBUTED BY HASH(project_id) BUCKETS 3";

    private static final String DAILY_MV = "CREATE MATERIALIZED VIEW daily_mv\n" +
            "PARTITION BY (pmod, dt_day)\n" +
            "DISTRIBUTED BY HASH(project_id) BUCKETS 3\n" +
            "REFRESH DEFERRED MANUAL\n" +
            "AS SELECT mod(project_id, 16) AS pmod, date_trunc('day', created_at) AS dt_day,\n" +
            "  date_trunc('hour', created_at) AS dt_hour, project_id, campaign_id,\n" +
            "  count(*) AS cnt, hll_union(hll_hash(email)) AS hll_email\n" +
            "FROM daily_events\n" +
            "GROUP BY mod(project_id, 16), date_trunc('day', created_at), date_trunc('hour', created_at),\n" +
            "  project_id, campaign_id";

    // an automatic list partition on (project_id, dt), dt being a generated column of the hour named by the table
    private static final String NAMED_EVENTS = "CREATE TABLE named_events (\n" +
            "  project_id BIGINT NOT NULL,\n" +
            "  created_at DATETIME NOT NULL,\n" +
            "  campaign_id BIGINT,\n" +
            "  dt DATETIME AS date_trunc('hour', created_at)\n" +
            ")\n" +
            "DUPLICATE KEY(project_id, created_at)\n" +
            "PARTITION BY (project_id, dt)\n" +
            "DISTRIBUTED BY HASH(project_id) BUCKETS 3";

    // an unpartitioned table that the events join
    private static final String CAMPAIGNS = "CREATE TABLE campaigns (\n" +
            "  campaign_id BIGINT NOT NULL,\n" +
            "  name VARCHAR(64)\n" +
            ")\n" +
            "DUPLICATE KEY(campaign_id)\n" +
            "DISTRIBUTED BY HASH(campaign_id) BUCKETS 3";

    // an automatic list partition on (project_id, second), from_unixtime depending on the time zone of the session
    private static final String UNIX_EVENTS = "CREATE TABLE unix_events (\n" +
            "  project_id BIGINT NOT NULL,\n" +
            "  ts BIGINT NOT NULL,\n" +
            "  campaign_id BIGINT\n" +
            ")\n" +
            "DUPLICATE KEY(project_id, ts)\n" +
            "PARTITION BY (project_id, from_unixtime(ts))\n" +
            "DISTRIBUTED BY HASH(project_id) BUCKETS 3";

    // an automatic list partition on (project_id, cid), cid being a generated column that is campaign_id itself
    private static final String IDENTITY_EVENTS = "CREATE TABLE identity_events (\n" +
            "  project_id BIGINT NOT NULL,\n" +
            "  campaign_id BIGINT NOT NULL,\n" +
            "  email VARCHAR(128),\n" +
            "  cid BIGINT AS (campaign_id)\n" +
            ")\n" +
            "DUPLICATE KEY(project_id)\n" +
            "PARTITION BY (project_id, cid)\n" +
            "DISTRIBUTED BY HASH(project_id) BUCKETS 3";

    // an automatic list partition on (project_id, day), the day of a string cast to DATETIME
    private static final String CAST_EVENTS = "CREATE TABLE cast_events (\n" +
            "  project_id BIGINT NOT NULL,\n" +
            "  created_str VARCHAR(32) NOT NULL,\n" +
            "  campaign_id BIGINT\n" +
            ")\n" +
            "DUPLICATE KEY(project_id)\n" +
            "PARTITION BY (project_id, date_trunc('day', CAST(created_str AS DATETIME)))\n" +
            "DISTRIBUTED BY HASH(project_id) BUCKETS 3";

    // an automatic list partition on (project bucket, dt), dt being a generated column of the hour named by the table
    private static final String NAMED_BUCKET_EVENTS = "CREATE TABLE named_bucket_events (\n" +
            "  project_id BIGINT NOT NULL,\n" +
            "  created_at DATETIME NOT NULL,\n" +
            "  campaign_id BIGINT,\n" +
            "  dt DATETIME AS date_trunc('hour', created_at)\n" +
            ")\n" +
            "DUPLICATE KEY(project_id, created_at)\n" +
            "PARTITION BY (mod(project_id, 16), dt)\n" +
            "DISTRIBUTED BY HASH(project_id) BUCKETS 3";

    // an automatic list partition on (hour, cid), cid being a generated column that is campaign_id itself
    private static final String HOUR_IDENTITY_EVENTS = "CREATE TABLE hour_identity_events (\n" +
            "  project_id BIGINT NOT NULL,\n" +
            "  created_at DATETIME NOT NULL,\n" +
            "  campaign_id BIGINT NOT NULL,\n" +
            "  email VARCHAR(128),\n" +
            "  cid BIGINT AS (campaign_id)\n" +
            ")\n" +
            "DUPLICATE KEY(project_id, created_at)\n" +
            "PARTITION BY (date_trunc('hour', created_at), cid)\n" +
            "DISTRIBUTED BY HASH(project_id) BUCKETS 3";

    // an automatic list partition on (bucket of a LARGEINT, day)
    private static final String LARGE_EVENTS = "CREATE TABLE large_events (\n" +
            "  project_id BIGINT NOT NULL,\n" +
            "  created_at DATETIME NOT NULL,\n" +
            "  k LARGEINT NOT NULL,\n" +
            "  email VARCHAR(128)\n" +
            ")\n" +
            "DUPLICATE KEY(project_id, created_at)\n" +
            "PARTITION BY (mod(k, 16), date_trunc('day', created_at))\n" +
            "DISTRIBUTED BY HASH(project_id) BUCKETS 3";

    // an automatic list partition on (project bucket, day of created_at), with another time column received_at
    private static final String RECEIVED_EVENTS = "CREATE TABLE received_events (\n" +
            "  project_id BIGINT NOT NULL,\n" +
            "  created_at DATETIME NOT NULL,\n" +
            "  received_at DATETIME NOT NULL,\n" +
            "  email VARCHAR(128)\n" +
            ")\n" +
            "DUPLICATE KEY(project_id, created_at)\n" +
            "PARTITION BY (mod(project_id, 16), date_trunc('day', created_at))\n" +
            "DISTRIBUTED BY HASH(project_id) BUCKETS 3";

    // an automatic list partition on (project_id, bucket), the bucket mod(project_id, divisor) of two columns
    private static final String TWO_COLUMN_EVENTS = "CREATE TABLE two_column_events (\n" +
            "  project_id BIGINT NOT NULL,\n" +
            "  divisor BIGINT NOT NULL,\n" +
            "  email VARCHAR(128)\n" +
            ")\n" +
            "DUPLICATE KEY(project_id)\n" +
            "PARTITION BY (project_id, mod(project_id, divisor))\n" +
            "DISTRIBUTED BY HASH(project_id) BUCKETS 3";

    private static final List<String> HOURLY_REF_COLUMNS = List.of("project_id", "__generated_partition_column_0");
    private static final List<String> DAILY_REF_COLUMNS =
            List.of("__generated_partition_column_0", "__generated_partition_column_1");

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
    }

    private static List<String> names(List<Column> columns) {
        return columns.stream().map(Column::getName).collect(Collectors.toList());
    }

    private static void assertPartitionMapping(MaterializedView mv, String baseTableName,
                                               List<String> mvPartitionColumns, List<String> refPartitionColumns) {
        Assertions.assertTrue(mv.getPartitionInfo().isListPartition());
        // partitioned by its own output columns, with no generated partition column of its own
        Assertions.assertEquals(mvPartitionColumns, names(mv.getPartitionColumns()));
        Assertions.assertTrue(mv.getBaseSchema().stream().noneMatch(Column::isGeneratedColumn));
        Map<Table, List<Column>> refColumns = mv.getRefBaseTablePartitionColumns();
        Assertions.assertEquals(1, refColumns.size(), refColumns.toString());
        Map.Entry<Table, List<Column>> entry = refColumns.entrySet().iterator().next();
        Assertions.assertEquals(baseTableName, entry.getKey().getName());
        Assertions.assertEquals(refPartitionColumns, names(entry.getValue()));
    }

    private static Set<PCell> listCells(OlapTable table) {
        return Sets.newHashSet(table.getListPartitionItems().getPCells());
    }

    /**
     * Asserts that the scan of {@code table} in {@code plan} reads {@code partitions}, e.g. 1/3.
     */
    private static void assertScanPartitions(String plan, String table, String partitions) {
        int scan = plan.indexOf("TABLE: " + table + "\n");
        Assertions.assertTrue(scan >= 0, plan);
        int selected = plan.indexOf("partitions=", scan);
        Assertions.assertTrue(selected >= 0, plan);
        Assertions.assertTrue(plan.startsWith("partitions=" + partitions + "\n", selected), plan);
    }

    private void addHourlyPartitions() throws Exception {
        addListPartition("hourly_events", "p1_10", "1", "2026-06-01 10:00:00");
        addListPartition("hourly_events", "p1_11", "1", "2026-06-01 11:00:00");
        addListPartition("hourly_events", "p2_10", "2", "2026-06-01 10:00:00");
        executeInsertSql("INSERT INTO hourly_events PARTITION(p1_10) " +
                "VALUES (1, '2026-06-01 10:05:00', 11, 101, 'a@x.com'), (1, '2026-06-01 10:50:00', 12, 101, 'b@x.com')");
        executeInsertSql("INSERT INTO hourly_events PARTITION(p1_11) " +
                "VALUES (1, '2026-06-01 11:30:00', 11, 101, 'a@x.com')");
        executeInsertSql("INSERT INTO hourly_events PARTITION(p2_10) " +
                "VALUES (2, '2026-06-01 10:20:00', 21, 201, 'c@x.com')");
    }

    @Test
    public void testCreateOnHourlyPartitions() {
        starRocksAssert.withTable(HOURLY_EVENTS, () -> {
            starRocksAssert.withMaterializedView(HOURLY_MV, () -> {
                assertPartitionMapping(getMv("hourly_mv"), "hourly_events", List.of("project_id", "dt_hour"),
                        HOURLY_REF_COLUMNS);
            });
        });
    }

    @Test
    public void testCreateOnProjectBucketAndDayPartitions() {
        for (String keysType : List.of("DUPLICATE", "PRIMARY")) {
            starRocksAssert.withTable(String.format(DAILY_EVENTS, keysType), () -> {
                // mod() is no partition function of the mv: pmod is the generated partition column itself
                starRocksAssert.withMaterializedView(DAILY_MV, () -> {
                    assertPartitionMapping(getMv("daily_mv"), "daily_events", List.of("pmod", "dt_day"),
                            DAILY_REF_COLUMNS);
                });
            });
        }
    }

    @Test
    public void testCreateWithOutputColumnNamedLikeItsBaseColumn() {
        starRocksAssert.withTable(HOURLY_EVENTS, () -> {
            // created_at of the mv is the hour of the base table's created_at, the generated partition column
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW hourly_mv_same_name\n" +
                    "PARTITION BY (project_id, created_at)\n" +
                    "DISTRIBUTED BY HASH(project_id) BUCKETS 3\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT project_id, date_trunc('hour', created_at) AS created_at, count(*) AS cnt\n" +
                    "FROM hourly_events\n" +
                    "GROUP BY project_id, date_trunc('hour', created_at)", () -> {
                        assertPartitionMapping(getMv("hourly_mv_same_name"), "hourly_events",
                                List.of("project_id", "created_at"), HOURLY_REF_COLUMNS);
                    });
        });
    }

    @Test
    public void testCreateOnNamedGeneratedPartitionColumn() {
        starRocksAssert.withTable(NAMED_EVENTS, () -> {
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW named_mv\n" +
                    "PARTITION BY (project_id, dt_hour)\n" +
                    "DISTRIBUTED BY HASH(project_id) BUCKETS 3\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT project_id, date_trunc('hour', created_at) AS dt_hour, count(*) AS cnt\n" +
                    "FROM named_events GROUP BY project_id, date_trunc('hour', created_at)", () -> {
                        assertPartitionMapping(getMv("named_mv"), "named_events", List.of("project_id", "dt_hour"),
                                List.of("project_id", "dt"));
                    });
        });
    }

    @Test
    public void testColumnList() {
        starRocksAssert.withTable(HOURLY_EVENTS, () -> {
            // a column list that keeps the query's names
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW same_names_mv (project_id, dt_hour, cnt)\n" +
                    "PARTITION BY (project_id, dt_hour)\n" +
                    "DISTRIBUTED BY HASH(project_id) BUCKETS 3\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT project_id, date_trunc('hour', created_at) AS dt_hour, count(*) AS cnt\n" +
                    "FROM hourly_events GROUP BY project_id, date_trunc('hour', created_at)", () -> {
                        assertPartitionMapping(getMv("same_names_mv"), "hourly_events",
                                List.of("project_id", "dt_hour"), HOURLY_REF_COLUMNS);
                    });
            // a column list that renames a partition column leaves all partition columns to the existing checks
            assertNotMapped("CREATE MATERIALIZED VIEW renamed_mv (project_id, h, cnt)\n" +
                    "PARTITION BY (project_id, h)\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT project_id, date_trunc('hour', created_at) AS dt_hour, count(*) AS cnt\n" +
                    "FROM hourly_events GROUP BY project_id, date_trunc('hour', created_at)", "h");
        });
    }

    @Test
    public void testCreateWithStarExpandedPartitionColumn() {
        starRocksAssert.withTable(HOURLY_EVENTS, () -> {
            addHourlyPartitions();
            OlapTable base = (OlapTable) getTable(DB_NAME, "hourly_events");
            // project_id of the star refers directly to the base column, like a listed one
            List<String> queries = List.of(
                    "SELECT *, date_trunc('hour', created_at) AS dt_hour FROM hourly_events",
                    "SELECT e.*, date_trunc('hour', e.created_at) AS dt_hour FROM hourly_events e");
            for (String query : queries) {
                starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW star_mv\n" +
                        "PARTITION BY (project_id, dt_hour)\n" +
                        "DISTRIBUTED BY HASH(project_id) BUCKETS 3\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "AS " + query, () -> {
                            MaterializedView mv = getMv("star_mv");
                            assertPartitionMapping(mv, "hourly_events", List.of("project_id", "dt_hour"),
                                    HOURLY_REF_COLUMNS);
                            TaskRun taskRun = buildMVTaskRun(mv, DB_NAME);
                            ExecPlan execPlan = getMVRefreshExecPlan(taskRun);
                            Assertions.assertNotNull(execPlan);
                            assertScanPartitions(execPlan.getExplainString(TExplainLevel.NORMAL), "hourly_events", "3/3");
                            Assertions.assertEquals(listCells(base), listCells(mv));
                            // a load into one hour refreshes that hour only
                            executeInsertSql("INSERT INTO hourly_events PARTITION(p1_11) " +
                                    "VALUES (1, '2026-06-01 11:40:00', 13, 102, 'd@x.com')");
                            execPlan = getMVRefreshExecPlan(taskRun);
                            Assertions.assertNotNull(execPlan);
                            assertScanPartitions(execPlan.getExplainString(TExplainLevel.NORMAL), "hourly_events", "1/3");
                        });
            }
            // the existing checks would resolve project_id of the star to dt_hour, the output at its position in the
            // table, so the mv is left to them
            assertNotMapped("CREATE MATERIALIZED VIEW star_after_mv\n" +
                    "PARTITION BY (project_id, dt_hour)\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT date_trunc('hour', created_at) AS dt_hour, * FROM hourly_events", "dt_hour");
        });
    }

    /**
     * Asserts that the mv is rejected as before: its partition column {@code column}, which maps to no generated
     * partition column, is no column of the base table.
     */
    private void assertNotMapped(String mvSql, String column) {
        Exception e = Assertions.assertThrows(Exception.class, () -> starRocksAssert.withMaterializedView(mvSql));
        Assertions.assertTrue(e.getMessage().contains("Please check the partition expression " + column +
                ", it should refer base table's partition column directly"), e.getMessage());
    }

    @Test
    public void testRejectOutputColumnsNotGeneratedPartitionColumns() {
        starRocksAssert.withTable(HOURLY_EVENTS, () -> {
            // a day is a roll-up of the hour partitions, not one of them
            assertNotMapped("CREATE MATERIALIZED VIEW day_rollup_mv\n" +
                    "PARTITION BY (project_id, dt_day)\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT project_id, date_trunc('day', created_at) AS dt_day, count(*) AS cnt\n" +
                    "FROM hourly_events GROUP BY project_id, date_trunc('day', created_at)", "dt_day");
            // an alias of a plain partition column
            assertNotMapped("CREATE MATERIALIZED VIEW plain_alias_mv\n" +
                    "PARTITION BY (pid, dt_hour)\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT project_id AS pid, date_trunc('hour', created_at) AS dt_hour, count(*) AS cnt\n" +
                    "FROM hourly_events GROUP BY project_id, date_trunc('hour', created_at)", "pid");
        });
        starRocksAssert.withTable(String.format(DAILY_EVENTS, "DUPLICATE"), () -> {
            // a different modulus is no generated partition column either
            assertNotMapped("CREATE MATERIALIZED VIEW mod8_mv\n" +
                    "PARTITION BY (pmod, dt_day)\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT mod(project_id, 8) AS pmod, date_trunc('day', created_at) AS dt_day,\n" +
                    "  count(*) AS cnt\n" +
                    "FROM daily_events GROUP BY mod(project_id, 8), date_trunc('day', created_at)", "pmod");
        });
    }

    @Test
    public void testRejectOutputColumnsUnsafeToRefreshByPartition() {
        String hourlyEventsB = HOURLY_EVENTS.replace("hourly_events", "hourly_events_b");
        List<String> tables =
                List.of(HOURLY_EVENTS, hourlyEventsB, NAMED_EVENTS, CAMPAIGNS, RECEIVED_EVENTS, NAMED_BUCKET_EVENTS);
        starRocksAssert.withTables(tables, () -> {
            // a window across the hours of a project would be computed from one hour at a time
            assertNotMapped("CREATE MATERIALIZED VIEW window_mv\n" +
                    "PARTITION BY (project_id, dt_hour)\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT project_id, date_trunc('hour', created_at) AS dt_hour, campaign_id,\n" +
                    "  sum(campaign_id) OVER (PARTITION BY project_id) AS project_total\n" +
                    "FROM hourly_events", "dt_hour");
            // each branch of the union has its own generated partition column
            assertNotMapped("CREATE MATERIALIZED VIEW union_mv\n" +
                    "PARTITION BY (project_id, dt_hour)\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT project_id, date_trunc('hour', created_at) AS dt_hour, campaign_id FROM hourly_events\n" +
                    "UNION ALL\n" +
                    "SELECT project_id, date_trunc('hour', created_at) AS dt_hour, campaign_id FROM hourly_events_b",
                    "dt_hour");
            // the rows of one hour depend on the rows of the others
            assertNotMapped("CREATE MATERIALIZED VIEW limit_mv\n" +
                    "PARTITION BY (project_id, dt_hour)\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT project_id, date_trunc('hour', created_at) AS dt_hour, campaign_id\n" +
                    "FROM hourly_events LIMIT 10", "dt_hour");
            assertNotMapped("CREATE MATERIALIZED VIEW rollup_mv\n" +
                    "PARTITION BY (project_id, dt_hour)\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT project_id, date_trunc('hour', created_at) AS dt_hour, count(*) AS cnt\n" +
                    "FROM hourly_events GROUP BY ROLLUP(project_id, date_trunc('hour', created_at))", "dt_hour");
            // the query reads other tables than the ref base table, or reads it more than once or within a derived
            // table, through which the mv creation would resolve the generated column again by its name
            assertNotMapped("CREATE MATERIALIZED VIEW self_join_mv\n" +
                    "PARTITION BY (project_id, dt_hour)\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT b.project_id, date_trunc('hour', b.created_at) AS dt_hour, count(*) AS cnt\n" +
                    "FROM hourly_events a JOIN hourly_events b ON a.template_id = b.template_id\n" +
                    "GROUP BY b.project_id, date_trunc('hour', b.created_at)", "dt_hour");
            assertNotMapped("CREATE MATERIALIZED VIEW join_mv\n" +
                    "PARTITION BY (project_id, dt_hour)\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT b.project_id, date_trunc('hour', b.created_at) AS dt_hour, count(*) AS cnt\n" +
                    "FROM hourly_events a JOIN hourly_events_b b\n" +
                    "  ON a.project_id = b.project_id AND a.created_at = b.created_at\n" +
                    "GROUP BY b.project_id, date_trunc('hour', b.created_at)", "dt_hour");
            assertNotMapped("CREATE MATERIALIZED VIEW campaign_join_mv\n" +
                    "PARTITION BY (project_id, dt_hour)\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT e.project_id, date_trunc('hour', e.created_at) AS dt_hour, c.name, count(*) AS cnt\n" +
                    "FROM hourly_events e JOIN campaigns c ON e.campaign_id = c.campaign_id\n" +
                    "GROUP BY e.project_id, date_trunc('hour', e.created_at), c.name", "dt_hour");
            assertNotMapped("CREATE MATERIALIZED VIEW derived_mv\n" +
                    "PARTITION BY (project_id, dt_hour)\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT project_id, date_trunc('hour', created_at) AS dt_hour, count(*) AS cnt\n" +
                    "FROM (SELECT project_id, created_at FROM hourly_events) e\n" +
                    "GROUP BY project_id, date_trunc('hour', created_at)", "dt_hour");
            assertNotMapped("CREATE MATERIALIZED VIEW derived_join_mv\n" +
                    "PARTITION BY (project_id, dt_hour)\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT e.project_id, date_trunc('hour', e.created_at) AS dt_hour, count(*) AS cnt\n" +
                    "FROM (SELECT project_id, created_at AS dt FROM hourly_events) h\n" +
                    "  JOIN named_events e ON h.project_id = e.project_id AND h.dt = e.created_at\n" +
                    "GROUP BY e.project_id, date_trunc('hour', e.created_at)", "dt_hour");
            assertNotMapped("CREATE MATERIALIZED VIEW subquery_mv\n" +
                    "PARTITION BY (project_id, dt_hour)\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT project_id, date_trunc('hour', created_at) AS dt_hour, count(*) AS cnt\n" +
                    "FROM hourly_events WHERE campaign_id IN (SELECT campaign_id FROM campaigns)\n" +
                    "GROUP BY project_id, date_trunc('hour', created_at)", "dt_hour");
            // two partition columns of one generated column would get one partition value for two positions, so neither
            // is mapped and the existing checks reject the first
            assertNotMapped("CREATE MATERIALIZED VIEW twice_mv\n" +
                    "PARTITION BY (h1, h2)\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT project_id, date_trunc('hour', created_at) AS h1, date_trunc('hour', created_at) AS h2,\n" +
                    "  count(*) AS cnt\n" +
                    "FROM hourly_events GROUP BY project_id, date_trunc('hour', created_at)", "h1");
            // a column of the mv that is a partition column twice gets one partition value for two positions
            assertNotMapped("CREATE MATERIALIZED VIEW duplicate_key_mv\n" +
                    "PARTITION BY (project_id, project_id, dt_hour)\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT project_id, date_trunc('hour', created_at) AS dt_hour, count(*) AS cnt\n" +
                    "FROM hourly_events GROUP BY project_id, date_trunc('hour', created_at)", "dt_hour");
            // dt and date_trunc('hour', created_at) are both the generated column dt, one partition value for two
            // positions
            assertNotMapped("CREATE MATERIALIZED VIEW dt_twice_mv\n" +
                    "PARTITION BY (pmod, dt, date_trunc('hour', created_at))\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT mod(project_id, 16) AS pmod, dt, created_at, campaign_id FROM named_bucket_events", "pmod");
            // the mv creation would resolve the generated column dt to the output dt
            assertNotMapped("CREATE MATERIALIZED VIEW direct_and_definition_mv\n" +
                    "PARTITION BY (dt, dt_hour)\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT dt, date_trunc('hour', created_at) AS dt_hour, count(*) AS cnt\n" +
                    "FROM named_events GROUP BY dt, date_trunc('hour', created_at)", "dt_hour");
            // created_at of the mv is dt, which the mv creation would resolve by its name to the base table's created_at
            assertNotMapped("CREATE MATERIALIZED VIEW borrowed_name_mv\n" +
                    "PARTITION BY (project_id, created_at, dt_hour)\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT project_id, dt AS created_at, date_trunc('hour', created_at) AS dt_hour, campaign_id\n" +
                    "FROM named_events", "dt_hour");
            // created_at of the mv is received_at, while the mv creation would track the day of the base table's
            // created_at for date_trunc('day', created_at)
            assertNotMapped("CREATE MATERIALIZED VIEW received_mv\n" +
                    "PARTITION BY (pmod, date_trunc('day', created_at))\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT mod(project_id, 16) AS pmod, received_at AS created_at, email FROM received_events", "pmod");
        });
    }

    @Test
    public void testRejectDefinitionsOtherThanDateTruncAndMod() {
        starRocksAssert.withTables(List.of(UNIX_EVENTS, CAST_EVENTS), () -> {
            // the load computes the generated column and the refresh computes the output, in sessions whose time
            // zones may differ
            assertNotMapped("CREATE MATERIALIZED VIEW unix_mv\n" +
                    "PARTITION BY (project_id, dt_str)\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT project_id, from_unixtime(ts) AS dt_str, count(*) AS cnt\n" +
                    "FROM unix_events GROUP BY project_id, from_unixtime(ts)", "dt_str");
            // a CAST is not mapped either: one of a TIME to DATETIME takes the date of the execution
            assertNotMapped("CREATE MATERIALIZED VIEW cast_mv\n" +
                    "PARTITION BY (project_id, dt_day)\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT project_id, date_trunc('day', CAST(created_str AS DATETIME)) AS dt_day, count(*) AS cnt\n" +
                    "FROM cast_events GROUP BY project_id, date_trunc('day', CAST(created_str AS DATETIME))", "dt_day");
        });
    }

    @Test
    public void testRejectDefinitionsOverTwoColumns() {
        starRocksAssert.withTable(TWO_COLUMN_EVENTS, () -> {
            // bucket is the generated partition column mod(project_id, divisor), but the existing checks require each
            // partition expr to refer to one column: the first one to find the ref base table, and each one again
            for (String partitionBy : List.of("(project_id, bucket)", "(bucket, project_id)")) {
                Exception e = Assertions.assertThrows(Exception.class, () -> starRocksAssert.withMaterializedView(
                        "CREATE MATERIALIZED VIEW two_column_mv\n" +
                        "PARTITION BY " + partitionBy + "\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "AS SELECT project_id, mod(project_id, divisor) AS bucket, email FROM two_column_events"));
                Assertions.assertTrue(e.getMessage().contains("must ref to one column"), e.getMessage());
            }
        });
    }

    @Test
    public void testRejectOutputColumnsOfQueryNamingPartitions() {
        starRocksAssert.withTable(HOURLY_EVENTS, () -> {
            addListPartition("hourly_events", "p1_10", "1", "2026-06-01 10:00:00");
            // the refresh by partition would replace the partitions that the query names with its own
            assertNotMapped("CREATE MATERIALIZED VIEW partition_clause_mv\n" +
                    "PARTITION BY (project_id, dt_hour)\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT project_id, date_trunc('hour', created_at) AS dt_hour, count(*) AS cnt\n" +
                    "FROM hourly_events PARTITION (p1_10) GROUP BY project_id, date_trunc('hour', created_at)", "dt_hour");
        });
    }

    @Test
    public void testRejectTwoOutputColumnsOfOneGeneratedColumn() {
        starRocksAssert.withTable(HOUR_IDENTITY_EVENTS, () -> {
            // both are the hour; the substitution by name would bind campaign_id to cid AS (campaign_id) if h were
            // mapped alone
            assertNotMapped("CREATE MATERIALIZED VIEW hour_twice_mv\n" +
                    "PARTITION BY (h, campaign_id)\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT date_trunc('hour', created_at) AS h, date_trunc('hour', created_at) AS campaign_id, email\n" +
                    "FROM hour_identity_events", "h");
        });
    }

    @Test
    public void testRejectModulusDifferingBeyondLong() {
        starRocksAssert.withTable(LARGE_EVENTS, () -> {
            // 18446744073709551632 is 2^64 + 16, whose long value is 16
            assertNotMapped("CREATE MATERIALIZED VIEW large_mod_mv\n" +
                    "PARTITION BY (kmod, dt_day)\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT mod(k, 18446744073709551632) AS kmod, date_trunc('day', created_at) AS dt_day, count(*) AS cnt\n" +
                    "FROM large_events GROUP BY mod(k, 18446744073709551632), date_trunc('day', created_at)", "kmod");
        });
    }

    @Test
    public void testPartitionColumnReplacedByNameCheckedByDefinition() {
        starRocksAssert.withTable(IDENTITY_EVENTS, () -> {
            // campaign_id of the mv is named like the column of the generated column cid, but is campaign_id + 1
            Exception e = Assertions.assertThrows(Exception.class, () -> starRocksAssert.withMaterializedView(
                    "CREATE MATERIALIZED VIEW shifted_mv\n" +
                    "PARTITION BY (project_id, campaign_id)\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS SELECT project_id, campaign_id + 1 AS campaign_id, email FROM identity_events"));
            Assertions.assertTrue(e.getMessage().contains("Materialized view partition function must related with column"),
                    e.getMessage());
        });
    }

    @Test
    public void testPartitionsSyncAndRefreshByPartition() {
        starRocksAssert.withTable(HOURLY_EVENTS, () -> {
            addHourlyPartitions();
            starRocksAssert.withMaterializedView(HOURLY_MV, () -> {
                MaterializedView mv = getMv("hourly_mv");
                OlapTable base = (OlapTable) getTable(DB_NAME, "hourly_events");
                TaskRun taskRun = buildMVTaskRun(mv, DB_NAME);

                // the first refresh creates one mv partition per base partition and reads them all
                ExecPlan execPlan = getMVRefreshExecPlan(taskRun);
                Assertions.assertNotNull(execPlan);
                assertScanPartitions(execPlan.getExplainString(TExplainLevel.NORMAL), "hourly_events", "3/3");
                Assertions.assertEquals(listCells(base), listCells(mv));
                Assertions.assertNull(getMVRefreshExecPlan(taskRun));

                // a load into one hour refreshes that hour only
                executeInsertSql("INSERT INTO hourly_events PARTITION(p1_11) " +
                        "VALUES (1, '2026-06-01 11:40:00', 13, 102, 'd@x.com')");
                execPlan = getMVRefreshExecPlan(taskRun);
                Assertions.assertNotNull(execPlan);
                assertScanPartitions(execPlan.getExplainString(TExplainLevel.NORMAL), "hourly_events", "1/3");
                Assertions.assertEquals(3, mv.getVisiblePartitions().size());

                // a new hour adds its mv partition
                addListPartition("hourly_events", "p3_12", "3", "2026-06-01 12:00:00");
                executeInsertSql("INSERT INTO hourly_events PARTITION(p3_12) " +
                        "VALUES (3, '2026-06-01 12:10:00', 31, 301, 'e@x.com')");
                execPlan = getMVRefreshExecPlan(taskRun);
                Assertions.assertNotNull(execPlan);
                assertScanPartitions(execPlan.getExplainString(TExplainLevel.NORMAL), "hourly_events", "1/4");
                Assertions.assertEquals(listCells(base), listCells(mv));
            });
        });
    }

    @Test
    public void testQueryRewrite() {
        starRocksAssert.withTable(HOURLY_EVENTS, () -> {
            addHourlyPartitions();
            starRocksAssert.withMaterializedView(HOURLY_MV, () -> {
                refreshMV(DB_NAME, getMv("hourly_mv"));
                // a group by and a filter on the hour, which the query and the mv both read as the generated column
                String plan = getFragmentPlan("SELECT project_id, date_trunc('hour', created_at), count(*)\n" +
                        "FROM hourly_events WHERE date_trunc('hour', created_at) = '2026-06-01 10:00:00'\n" +
                        "GROUP BY project_id, date_trunc('hour', created_at)");
                PlanTestBase.assertContains(plan, "TABLE: hourly_mv");
                PlanTestBase.assertNotContains(plan, "TABLE: hourly_events");

                // an approximate distinct count is read from the hll states
                plan = getFragmentPlan("SELECT project_id, campaign_id, approx_count_distinct(email)\n" +
                        "FROM hourly_events WHERE project_id = 1 GROUP BY project_id, campaign_id");
                PlanTestBase.assertContains(plan, "TABLE: hourly_mv");
                PlanTestBase.assertContains(plan, "hll_union_agg");
                PlanTestBase.assertNotContains(plan, "TABLE: hourly_events");
            });
        });
    }

    @Test
    public void testQueryRewriteRollUpToDay() {
        starRocksAssert.withTable(String.format(DAILY_EVENTS, "DUPLICATE"), () -> {
            addListPartition("daily_events", "p1_0601", "1", "2026-06-01");
            addListPartition("daily_events", "p1_0602", "1", "2026-06-02");
            executeInsertSql("INSERT INTO daily_events PARTITION(p1_0601) " +
                    "VALUES (1, '2026-06-01 10:05:00', 1, 11, 'a@x.com'), (17, '2026-06-01 11:05:00', 2, 12, 'b@x.com')");
            executeInsertSql("INSERT INTO daily_events PARTITION(p1_0602) " +
                    "VALUES (1, '2026-06-02 09:05:00', 3, 11, 'a@x.com')");
            starRocksAssert.withMaterializedView(DAILY_MV, () -> {
                refreshMV(DB_NAME, getMv("daily_mv"));
                // the day is the generated partition column the mv outputs as dt_day
                String plan = getFragmentPlan("SELECT date_trunc('day', created_at), approx_count_distinct(email)\n" +
                        "FROM daily_events WHERE mod(project_id, 16) = 1 GROUP BY date_trunc('day', created_at)");
                PlanTestBase.assertContains(plan, "TABLE: daily_mv");
                PlanTestBase.assertNotContains(plan, "TABLE: daily_events");

                // the hour is an ordinary output column of the mv
                plan = getFragmentPlan("SELECT project_id, date_trunc('hour', created_at), count(*)\n" +
                        "FROM daily_events WHERE project_id = 1 GROUP BY project_id, date_trunc('hour', created_at)");
                PlanTestBase.assertContains(plan, "TABLE: daily_mv");
                PlanTestBase.assertNotContains(plan, "TABLE: daily_events");
            });
        });
    }

    @Test
    public void testUnionRewriteWithStalePartition() {
        starRocksAssert.withTable(HOURLY_EVENTS, () -> {
            addHourlyPartitions();
            starRocksAssert.withMaterializedView(HOURLY_MV, () -> {
                refreshMV(DB_NAME, getMv("hourly_mv"));
                executeInsertSql("INSERT INTO hourly_events PARTITION(p1_11) " +
                        "VALUES (1, '2026-06-01 11:40:00', 13, 102, 'd@x.com')");
                // checked consistency: the stale hour is read from the base table, the others from the mv
                String plan = getFragmentPlan("SELECT project_id, date_trunc('hour', created_at), count(*)\n" +
                        "FROM hourly_events GROUP BY project_id, date_trunc('hour', created_at)");
                PlanTestBase.assertContains(plan, "UNION");
                assertScanPartitions(plan, "hourly_mv", "2/3");
                assertScanPartitions(plan, "hourly_events", "1/3");
            });
        });
    }

    @Test
    public void testActiveShowCreateAndReplay() {
        starRocksAssert.withTable(HOURLY_EVENTS, () -> {
            starRocksAssert.withMaterializedView(HOURLY_MV, () -> {
                MaterializedView mv = getMv("hourly_mv");
                String ddl = mv.getMaterializedViewDdlStmt(false, false);
                Assertions.assertTrue(ddl.contains("PARTITION BY (`project_id`,`dt_hour`)"), ddl);

                // ACTIVE analyzes the ddl again
                starRocksAssert.alterMvProperties("ALTER MATERIALIZED VIEW hourly_mv INACTIVE");
                starRocksAssert.alterMvProperties("ALTER MATERIALIZED VIEW hourly_mv ACTIVE");
                mv = getMv("hourly_mv");
                Assertions.assertTrue(mv.isActive(), mv.getInactiveReason());
                Assertions.assertEquals(ddl, mv.getMaterializedViewDdlStmt(false, false));
                assertPartitionMapping(mv, "hourly_events", List.of("project_id", "dt_hour"), HOURLY_REF_COLUMNS);

                // a restart reloads the mv from its image
                Table reloaded = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(mv, Table.class), Table.class);
                Assertions.assertTrue(reloaded instanceof MaterializedView);
                MaterializedView reloadedMv = (MaterializedView) reloaded;
                reloadedMv.onReload();
                Assertions.assertTrue(reloadedMv.isActive(), reloadedMv.getInactiveReason());
                List<Expr> refExprs = reloadedMv.getPartitionRefTableExprs();
                Assertions.assertEquals(2, refExprs.size(), refExprs.toString());
                Assertions.assertTrue(refExprs.get(1) instanceof SlotRef, refExprs.toString());
                Assertions.assertEquals("__generated_partition_column_0", ((SlotRef) refExprs.get(1)).getColumnName());
                assertPartitionMapping(reloadedMv, "hourly_events", List.of("project_id", "dt_hour"),
                        HOURLY_REF_COLUMNS);

                // what SHOW CREATE prints creates the mv again
                String showCreate = starRocksAssert.showCreateTable("SHOW CREATE TABLE hourly_mv");
                Assertions.assertTrue(showCreate.contains("PARTITION BY (`project_id`,`dt_hour`)"), showCreate);
                starRocksAssert.dropMaterializedView("hourly_mv");
                starRocksAssert.withMaterializedView(showCreate);
                assertPartitionMapping(getMv("hourly_mv"), "hourly_events", List.of("project_id", "dt_hour"),
                        HOURLY_REF_COLUMNS);
            });
        });
    }
}
