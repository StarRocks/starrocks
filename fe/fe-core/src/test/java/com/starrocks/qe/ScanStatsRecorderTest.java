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

package com.starrocks.qe;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.connector.iceberg.IcebergTableMORParams;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.proto.PerScanStatsItemPB;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TUniqueId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScanStatsRecorderTest {
    private final int savedMaxScans = Config.audit_scan_stats_max_scans;

    @AfterEach
    public void restoreConfig() {
        Config.audit_scan_stats_max_scans = savedMaxScans;
    }

    private static Table externalTable(Table.TableType type, String catalog, String db, String name) {
        Table table = mock(Table.class);
        when(table.isNativeTableOrMaterializedView()).thenReturn(false);
        when(table.isMaterializedView()).thenReturn(false);
        when(table.getType()).thenReturn(type);
        when(table.getCatalogName()).thenReturn(catalog);
        when(table.getCatalogDBName()).thenReturn(db);
        when(table.getCatalogTableName()).thenReturn(name);
        when(table.getName()).thenReturn(name);
        return table;
    }

    private static Table materializedView(String name) {
        Table table = mock(Table.class);
        when(table.isNativeTableOrMaterializedView()).thenReturn(true);
        when(table.isMaterializedView()).thenReturn(true);
        when(table.getType()).thenReturn(Table.TableType.MATERIALIZED_VIEW);
        when(table.getCatalogName()).thenReturn("default_catalog");
        when(table.getName()).thenReturn(name);
        when(table.mayGetDatabaseId()).thenReturn(Optional.empty());
        return table;
    }

    private static ScanNode scanNode(int id, Table table) {
        ScanNode scanNode = mock(ScanNode.class);
        TupleDescriptor desc = mock(TupleDescriptor.class);
        when(desc.getTable()).thenReturn(table);
        when(scanNode.getDesc()).thenReturn(desc);
        when(scanNode.getId()).thenReturn(new PlanNodeId(id));
        return scanNode;
    }

    private static ExecPlan plan(ScanNode... scanNodes) {
        ExecPlan plan = mock(ExecPlan.class);
        when(plan.getScanNodes()).thenReturn(new ArrayList<>(List.of(scanNodes)));
        return plan;
    }

    private static PQueryStatistics stats(PerScanStatsItemPB... items) {
        PQueryStatistics statistics = new PQueryStatistics();
        statistics.perScanStatsItems = new ArrayList<>(List.of(items));
        return statistics;
    }

    private static PerScanStatsItemPB item(int planNodeId, Long physical, Long logical, Long output, Long remote,
                                           Long cache) {
        PerScanStatsItemPB item = new PerScanStatsItemPB();
        item.planNodeId = planNodeId;
        item.physicalRowsRead = physical;
        item.logicalRowsRead = logical;
        item.outputRows = output;
        item.remoteBytes = remote;
        item.cacheBytes = cache;
        return item;
    }

    private static JsonObject scan(JsonObject attempt, int index) {
        return attempt.getAsJsonArray("scans").get(index).getAsJsonObject();
    }

    @Test
    public void testScansPerPlanNodeWithApplicableCountersOnly() {
        Table orders = externalTable(Table.TableType.ICEBERG, "ice", "sales", "orders");
        Table customers = externalTable(Table.TableType.JDBC, "pg", "public", "customers");
        // JDBC pushdown leaves the remote query in the catalog table name.
        when(customers.getCatalogTableName()).thenReturn("SELECT 1 FROM `public`.`customers`");
        Table events = externalTable(Table.TableType.ELASTICSEARCH, "es", "logs", "events");
        // The same Iceberg table scanned twice is two scans.
        Table visits = externalTable(Table.TableType.HIVE, "hive", "web", "visits");
        ExecPlan plan = plan(scanNode(0, orders), scanNode(2, customers), scanNode(4, orders),
                scanNode(6, events), scanNode(8, materializedView("mv1")), scanNode(10, visits));

        ScanStatsRecorder recorder = new ScanStatsRecorder();
        recorder.recordLastAttempt(0, new TUniqueId(1, 2), 100, 200, plan);
        String json = recorder.toJson(ScanStatsRecorder.AttemptState.OK,
                stats(item(0, 10L, 9L, 3L, 1000L, 50L),
                        item(2, 7L, 7L, 7L, 700L, 0L),
                        item(4, null, null, 0L, null, null),
                        item(6, 5L, 5L, 5L, 500L, 0L),
                        item(8, 4L, 4L, 2L, null, null),
                        item(10, 6L, 6L, 1L, 600L, 60L)),
                true, null, null, 0, 0);

        JsonObject root = JsonParser.parseString(json).getAsJsonObject();
        Assertions.assertEquals(1, root.get("schemaVersion").getAsInt());
        Assertions.assertTrue(root.get("statsComplete").getAsBoolean());
        JsonArray attempts = root.getAsJsonArray("attempts");
        Assertions.assertEquals(1, attempts.size());
        JsonObject attempt = attempts.get(0).getAsJsonObject();
        Assertions.assertEquals("OK", attempt.get("state").getAsString());
        Assertions.assertEquals("SCAN", attempt.get("executionPath").getAsString());
        Assertions.assertEquals(100, attempt.get("startTimeMs").getAsLong());
        Assertions.assertFalse(attempt.has("scansTruncated"));
        Assertions.assertEquals(6, attempt.getAsJsonArray("scans").size());

        JsonObject iceberg = scan(attempt, 0);
        Assertions.assertEquals(0, iceberg.get("scanId").getAsInt());
        Assertions.assertEquals("ice.sales.orders", iceberg.get("table").getAsString());
        Assertions.assertEquals("iceberg", iceberg.get("connector").getAsString());
        Assertions.assertFalse(iceberg.get("isMv").getAsBoolean());
        Assertions.assertEquals(10, iceberg.get("physicalRowsRead").getAsLong());
        Assertions.assertEquals(9, iceberg.get("logicalRowsRead").getAsLong());
        Assertions.assertEquals(3, iceberg.get("outputRows").getAsLong());
        Assertions.assertEquals(1000, iceberg.get("remoteBytes").getAsLong());
        Assertions.assertEquals(50, iceberg.get("cacheBytes").getAsLong());
        Assertions.assertFalse(iceberg.get("bytesEstimated").getAsBoolean());

        JsonObject jdbc = scan(attempt, 1);
        Assertions.assertEquals("pg.public.customers", jdbc.get("table").getAsString());
        Assertions.assertTrue(jdbc.get("bytesEstimated").getAsBoolean());

        // Reported only its zero output rows: its reader never opened, so it read nothing.
        JsonObject unread = scan(attempt, 2);
        Assertions.assertEquals(4, unread.get("scanId").getAsInt());
        Assertions.assertEquals(0, unread.get("physicalRowsRead").getAsLong());
        Assertions.assertEquals(0, unread.get("cacheBytes").getAsLong());

        // A connector whose counters are not defined only names the scan.
        JsonObject es = scan(attempt, 3);
        Assertions.assertEquals("elasticsearch", es.get("connector").getAsString());
        Assertions.assertFalse(es.has("physicalRowsRead"));
        Assertions.assertFalse(es.has("outputRows"));

        // Hive Parquet / ORC scans go through the same file readers as Iceberg.
        JsonObject hive = scan(attempt, 5);
        Assertions.assertEquals("hive", hive.get("connector").getAsString());
        Assertions.assertEquals(6, hive.get("physicalRowsRead").getAsLong());
        Assertions.assertEquals(600, hive.get("remoteBytes").getAsLong());
        Assertions.assertEquals(60, hive.get("cacheBytes").getAsLong());

        JsonObject mv = scan(attempt, 4);
        Assertions.assertEquals("olap", mv.get("connector").getAsString());
        Assertions.assertTrue(mv.get("isMv").getAsBoolean());
        Assertions.assertEquals("default_catalog.mv1", mv.get("table").getAsString());
        Assertions.assertEquals(2, mv.get("outputRows").getAsLong());
        Assertions.assertFalse(mv.has("remoteBytes"));
        Assertions.assertFalse(mv.has("bytesEstimated"));
    }

    @Test
    public void testRetriedAttemptsStayApart() {
        Table orders = externalTable(Table.TableType.ICEBERG, "ice", "sales", "orders");
        ScanStatsRecorder recorder = new ScanStatsRecorder();
        recorder.recordRetriedAttempt(0, new TUniqueId(1, 1), 10, 20, plan(scanNode(0, orders)),
                stats(item(0, 5L, 5L, 5L, 50L, 0L)));
        recorder.recordLastAttempt(1, new TUniqueId(2, 2), 30, 40, plan(scanNode(0, orders)));
        String json = recorder.toJson(ScanStatsRecorder.AttemptState.TIMEOUT,
                stats(item(0, 3L, 3L, 1L, 30L, 0L)), true, null, null, 0, 0);

        JsonObject root = JsonParser.parseString(json).getAsJsonObject();
        Assertions.assertFalse(root.get("statsComplete").getAsBoolean());
        JsonArray attempts = root.getAsJsonArray("attempts");
        Assertions.assertEquals(2, attempts.size());
        JsonObject first = attempts.get(0).getAsJsonObject();
        Assertions.assertEquals(0, first.get("attemptId").getAsInt());
        Assertions.assertEquals("ERR", first.get("state").getAsString());
        Assertions.assertFalse(first.get("statsComplete").getAsBoolean());
        Assertions.assertEquals(5, scan(first, 0).get("physicalRowsRead").getAsLong());
        JsonObject second = attempts.get(1).getAsJsonObject();
        Assertions.assertEquals("TIMEOUT", second.get("state").getAsString());
        Assertions.assertEquals(3, scan(second, 0).get("physicalRowsRead").getAsLong());
        Assertions.assertNotEquals(first.get("executionId").getAsString(),
                second.get("executionId").getAsString());
    }

    @Test
    public void testExecutionPathAndTruncation() {
        ScanStatsRecorder metadataOnly = new ScanStatsRecorder();
        JsonObject attempt = JsonParser.parseString(metadataOnly.toJson(ScanStatsRecorder.AttemptState.OK, null,
                        true, plan(), new TUniqueId(1, 1), 1, 2)).getAsJsonObject()
                .getAsJsonArray("attempts").get(0).getAsJsonObject();
        Assertions.assertEquals("METADATA_ONLY", attempt.get("executionPath").getAsString());
        Assertions.assertEquals(0, attempt.getAsJsonArray("scans").size());

        ScanStatsRecorder feLocal = new ScanStatsRecorder();
        attempt = JsonParser.parseString(feLocal.toJson(ScanStatsRecorder.AttemptState.OK, null, false, plan(),
                new TUniqueId(1, 1), 1, 2)).getAsJsonObject().getAsJsonArray("attempts").get(0).getAsJsonObject();
        Assertions.assertEquals("FE_LOCAL", attempt.get("executionPath").getAsString());

        Config.audit_scan_stats_max_scans = 1;
        Table orders = externalTable(Table.TableType.ICEBERG, "ice", "sales", "orders");
        ScanStatsRecorder truncated = new ScanStatsRecorder();
        attempt = JsonParser.parseString(truncated.toJson(ScanStatsRecorder.AttemptState.ERR, null, true,
                        plan(scanNode(0, orders), scanNode(1, orders)), new TUniqueId(1, 1), 1, 2)).getAsJsonObject()
                .getAsJsonArray("attempts").get(0).getAsJsonObject();
        Assertions.assertTrue(attempt.get("scansTruncated").getAsBoolean());
        Assertions.assertEquals(1, attempt.getAsJsonArray("scans").size());
        Assertions.assertEquals("SCAN", attempt.get("executionPath").getAsString());

        // No plan at all: nothing to record.
        Assertions.assertNull(new ScanStatsRecorder().toJson(ScanStatsRecorder.AttemptState.OK, null, true, null,
                null, 0, 0));
    }

    private static ScanNode morScanNode(int id, Table table, IcebergTableMORParams group,
                                        IcebergMORParams.ScanTaskType type) {
        IcebergScanNode scanNode = mock(IcebergScanNode.class);
        TupleDescriptor desc = mock(TupleDescriptor.class);
        when(desc.getTable()).thenReturn(table);
        when(scanNode.getDesc()).thenReturn(desc);
        when(scanNode.getId()).thenReturn(new PlanNodeId(id));
        when(scanNode.getTableFullMORParams()).thenReturn(group);
        when(scanNode.getMORParams()).thenReturn(IcebergMORParams.of(type, List.of(1)));
        return scanNode;
    }

    @Test
    public void testEqualityDeleteRewriteReportedAsOneScan() {
        Table orders = externalTable(Table.TableType.ICEBERG, "ice", "sales", "orders");
        IcebergTableMORParams group = new IcebergTableMORParams(7, List.of(IcebergMORParams.DATA_FILE_WITHOUT_EQ_DELETE));
        ExecPlan plan = plan(
                morScanNode(1, orders, group, IcebergMORParams.ScanTaskType.DATA_FILE_WITHOUT_EQ_DELETE),
                morScanNode(2, orders, group, IcebergMORParams.ScanTaskType.DATA_FILE_WITH_EQ_DELETE),
                morScanNode(3, orders, group, IcebergMORParams.ScanTaskType.EQ_DELETE),
                scanNode(5, orders));

        ScanStatsRecorder recorder = new ScanStatsRecorder();
        recorder.recordLastAttempt(0, new TUniqueId(1, 2), 1, 2, plan);
        String json = recorder.toJson(ScanStatsRecorder.AttemptState.OK,
                stats(item(1, 10L, 10L, 10L, 100L, 0L),
                        item(2, 20L, 18L, 18L, 200L, 5L),
                        item(3, 4L, 4L, 4L, 40L, 0L),
                        item(5, 1L, 1L, 1L, 10L, 0L)),
                true, null, null, 0, 0);

        JsonObject attempt = JsonParser.parseString(json).getAsJsonObject().getAsJsonArray("attempts").get(0)
                .getAsJsonObject();
        Assertions.assertEquals(2, attempt.getAsJsonArray("scans").size());
        JsonObject merged = scan(attempt, 0);
        Assertions.assertEquals(1, merged.get("scanId").getAsInt());
        Assertions.assertEquals(30, merged.get("physicalRowsRead").getAsLong());
        Assertions.assertEquals(28, merged.get("logicalRowsRead").getAsLong());
        Assertions.assertEquals(300, merged.get("remoteBytes").getAsLong());
        Assertions.assertEquals(5, merged.get("cacheBytes").getAsLong());
        // A scan of the same table outside the rewrite stays a scan of its own.
        Assertions.assertEquals(5, scan(attempt, 1).get("scanId").getAsInt());
    }

    @Test
    public void testScanWithoutReportMakesTheAttemptIncomplete() {
        Table orders = externalTable(Table.TableType.ICEBERG, "ice", "sales", "orders");
        Table events = externalTable(Table.TableType.ELASTICSEARCH, "es", "logs", "events");
        ScanStatsRecorder recorder = new ScanStatsRecorder();
        recorder.recordLastAttempt(0, new TUniqueId(1, 2), 1, 2, plan(scanNode(0, orders), scanNode(1, orders),
                scanNode(2, events)));
        // Scan 1 never reported (a path or a BE that does not collect them); scan 2 has no counters defined.
        String json = recorder.toJson(ScanStatsRecorder.AttemptState.OK, stats(item(0, 3L, 3L, 3L, 30L, 0L)),
                true, null, null, 0, 0);
        JsonObject root = JsonParser.parseString(json).getAsJsonObject();
        Assertions.assertFalse(root.get("statsComplete").getAsBoolean());
        JsonObject attempt = root.getAsJsonArray("attempts").get(0).getAsJsonObject();
        Assertions.assertEquals("OK", attempt.get("state").getAsString());
        Assertions.assertFalse(attempt.get("statsComplete").getAsBoolean());
        Assertions.assertEquals(3, scan(attempt, 0).get("physicalRowsRead").getAsLong());
        Assertions.assertFalse(scan(attempt, 1).has("physicalRowsRead"));
        Assertions.assertFalse(scan(attempt, 1).has("outputRows"));

        // A scan of a connector without counters does not need a report.
        ScanStatsRecorder hiveOnly = new ScanStatsRecorder();
        hiveOnly.recordLastAttempt(0, new TUniqueId(1, 2), 1, 2, plan(scanNode(2, events)));
        root = JsonParser.parseString(hiveOnly.toJson(ScanStatsRecorder.AttemptState.OK, null, true, null, null, 0,
                0)).getAsJsonObject();
        Assertions.assertTrue(root.get("statsComplete").getAsBoolean());
    }

    @Test
    public void testTwoEqualityDeleteRewritesOfOneTableStayApart() {
        Table orders = externalTable(Table.TableType.ICEBERG, "ice", "sales", "orders");
        // Every rewrite of the table carries the table id as its MOR id.
        IcebergTableMORParams left = new IcebergTableMORParams(7, List.of(IcebergMORParams.DATA_FILE_WITHOUT_EQ_DELETE));
        IcebergTableMORParams right = new IcebergTableMORParams(7, List.of(IcebergMORParams.DATA_FILE_WITHOUT_EQ_DELETE));
        ExecPlan plan = plan(
                morScanNode(1, orders, left, IcebergMORParams.ScanTaskType.DATA_FILE_WITHOUT_EQ_DELETE),
                morScanNode(2, orders, left, IcebergMORParams.ScanTaskType.EQ_DELETE),
                morScanNode(3, orders, right, IcebergMORParams.ScanTaskType.DATA_FILE_WITHOUT_EQ_DELETE),
                morScanNode(4, orders, right, IcebergMORParams.ScanTaskType.EQ_DELETE));
        ScanStatsRecorder recorder = new ScanStatsRecorder();
        recorder.recordLastAttempt(0, new TUniqueId(1, 2), 1, 2, plan);
        String json = recorder.toJson(ScanStatsRecorder.AttemptState.OK,
                stats(item(1, 10L, 10L, 10L, 100L, 0L), item(2, 1L, 1L, 1L, 1L, 0L),
                        item(3, 20L, 20L, 20L, 200L, 0L), item(4, 1L, 1L, 1L, 1L, 0L)),
                true, null, null, 0, 0);
        JsonObject attempt = JsonParser.parseString(json).getAsJsonObject().getAsJsonArray("attempts").get(0)
                .getAsJsonObject();
        Assertions.assertEquals(2, attempt.getAsJsonArray("scans").size());
        Assertions.assertEquals(1, scan(attempt, 0).get("scanId").getAsInt());
        Assertions.assertEquals(10, scan(attempt, 0).get("physicalRowsRead").getAsLong());
        Assertions.assertEquals(3, scan(attempt, 1).get("scanId").getAsInt());
        Assertions.assertEquals(20, scan(attempt, 1).get("physicalRowsRead").getAsLong());
    }

    @Test
    public void testTruncationKeepsEqualityDeleteGroupsWhole() {
        Table orders = externalTable(Table.TableType.ICEBERG, "ice", "sales", "orders");
        Table events = externalTable(Table.TableType.ELASTICSEARCH, "es", "logs", "events");
        IcebergTableMORParams group = new IcebergTableMORParams(7, List.of(IcebergMORParams.DATA_FILE_WITHOUT_EQ_DELETE));
        ExecPlan plan = plan(
                morScanNode(1, orders, group, IcebergMORParams.ScanTaskType.DATA_FILE_WITHOUT_EQ_DELETE),
                scanNode(2, events),
                morScanNode(3, orders, group, IcebergMORParams.ScanTaskType.DATA_FILE_WITH_EQ_DELETE),
                morScanNode(4, orders, group, IcebergMORParams.ScanTaskType.EQ_DELETE));
        Config.audit_scan_stats_max_scans = 1;
        ScanStatsRecorder recorder = new ScanStatsRecorder();
        recorder.recordLastAttempt(0, new TUniqueId(1, 2), 1, 2, plan);
        String json = recorder.toJson(ScanStatsRecorder.AttemptState.OK,
                stats(item(1, 10L, 10L, 10L, 100L, 0L), item(3, 5L, 4L, 4L, 50L, 0L), item(4, 1L, 1L, 1L, 1L, 0L)),
                true, null, null, 0, 0);
        JsonObject attempt = JsonParser.parseString(json).getAsJsonObject().getAsJsonArray("attempts").get(0)
                .getAsJsonObject();
        Assertions.assertTrue(attempt.get("scansTruncated").getAsBoolean());
        Assertions.assertEquals(1, attempt.getAsJsonArray("scans").size());
        Assertions.assertEquals(1, scan(attempt, 0).get("scanId").getAsInt());
        // The whole group is counted, including the data-file scan after the dropped hive scan.
        Assertions.assertEquals(15, scan(attempt, 0).get("physicalRowsRead").getAsLong());
    }
}
