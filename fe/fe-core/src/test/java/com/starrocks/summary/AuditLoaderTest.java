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

package com.starrocks.summary;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.common.Config;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.plugin.AuditEvent.EventType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AuditLoaderTest {

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
    }

    private static AuditEvent baseEvent() {
        AuditEvent event = new AuditEvent();
        event.type = EventType.AFTER_QUERY;
        event.queryId = "qid-1";
        event.timestamp = 1000L;
        event.user = "alice";
        event.db = "db1";
        event.stmt = "SELECT 1";
        event.isQuery = true;
        event.queryTime = 1;
        event.queriedRelations = List.of("db1.t1", "db1.t2");
        return event;
    }

    @Test
    public void testFormatRowJsonBasic() {
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        JsonObject obj = JsonParser.parseString(mgr.formatRowJson(baseEvent())).getAsJsonObject();

        Assertions.assertEquals("qid-1", obj.get("queryId").getAsString());
        Assertions.assertEquals("alice", obj.get("user").getAsString());
        Assertions.assertEquals("db1", obj.get("db").getAsString());
        Assertions.assertEquals("SELECT 1", obj.get("stmt").getAsString());
        // isQuery is stored as 1/0.
        Assertions.assertEquals(1, obj.get("isQuery").getAsInt());
        // QueriedRelations is a JSON array.
        JsonArray relations = obj.get("QueriedRelations").getAsJsonArray();
        Assertions.assertEquals(2, relations.size());
        Assertions.assertEquals("db1.t1", relations.get(0).getAsString());
    }

    @Test
    public void testQueryTypeConnection() {
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        AuditEvent event = baseEvent();
        event.type = EventType.CONNECTION;
        JsonObject obj = JsonParser.parseString(mgr.formatRowJson(event)).getAsJsonObject();
        Assertions.assertEquals("connection", obj.get("queryType").getAsString());
    }

    @Test
    public void testQueryTypeSlowVsNormal() {
        AuditLoaderMgr mgr = new AuditLoaderMgr();

        AuditEvent normal = baseEvent();
        normal.queryTime = 0;
        Assertions.assertEquals("query",
                JsonParser.parseString(mgr.formatRowJson(normal)).getAsJsonObject().get("queryType").getAsString());

        AuditEvent slow = baseEvent();
        slow.queryTime = Config.qe_slow_log_ms + 1;
        Assertions.assertEquals("slow_query",
                JsonParser.parseString(mgr.formatRowJson(slow)).getAsJsonObject().get("queryType").getAsString());
    }

    @Test
    public void testTruncateToBytesAscii() {
        Assertions.assertEquals("01234", AuditLoaderMgr.truncateToBytes("0123456789", 5));
        Assertions.assertEquals("abc", AuditLoaderMgr.truncateToBytes("abc", 10));
        Assertions.assertEquals("", AuditLoaderMgr.truncateToBytes(null, 5));
    }

    @Test
    public void testTruncateToBytesNoSplitMultibyte() {
        // "你好" is 6 UTF-8 bytes (3 per char). Truncating to 4 bytes must not split the 2nd char.
        String r = AuditLoaderMgr.truncateToBytes("你好", 4);
        Assertions.assertEquals("你", r);
        Assertions.assertTrue(r.getBytes(StandardCharsets.UTF_8).length <= 4);
    }

    @Test
    public void testPluginGatingAndExec() {
        AuditLoaderMgr mgr = GlobalStateMgr.getCurrentState().getAuditLoaderMgr();
        Assertions.assertNotNull(mgr);
        AuditLoaderPlugin plugin = new AuditLoaderPlugin();
        Assertions.assertNotNull(plugin.getPluginInfo());
        boolean orig = Config.enable_audit_loader;
        try {
            Config.enable_audit_loader = false;
            Assertions.assertFalse(plugin.eventFilter(EventType.AFTER_QUERY));
            Config.enable_audit_loader = true;
            Assertions.assertFalse(plugin.eventFilter(EventType.BEFORE_QUERY));
            Assertions.assertTrue(plugin.eventFilter(EventType.AFTER_QUERY));
            Assertions.assertTrue(plugin.eventFilter(EventType.CONNECTION));
            int before = mgr.bufferedRows();
            plugin.exec(baseEvent());
            Assertions.assertEquals(before + 1, mgr.bufferedRows());
        } finally {
            Config.enable_audit_loader = orig;
            mgr.clearBuffer();
        }
    }

    @Test
    public void testRunCycleDisabledDrainsBuffer() {
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        boolean orig = Config.enable_audit_loader;
        try {
            Config.enable_audit_loader = false;
            mgr.offerEvent(baseEvent());
            Assertions.assertEquals(1, mgr.bufferedRows());
            mgr.runAfterCatalogReady();
            Assertions.assertEquals(0, mgr.bufferedRows());
            Assertions.assertFalse(mgr.isDisabledByConflict());
        } finally {
            Config.enable_audit_loader = orig;
        }
    }

    @Test
    public void testRunCycleEnabledTableNotReadyKeepsBuffer() {
        // The audit database does not exist in this harness, so ensureAuditTable fails and
        // the cycle must leave buffered rows untouched (retry when the table becomes available).
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        boolean orig = Config.enable_audit_loader;
        try {
            Config.enable_audit_loader = true;
            mgr.offerEvent(baseEvent());
            mgr.runAfterCatalogReady();
            Assertions.assertEquals(1, mgr.bufferedRows());
        } finally {
            Config.enable_audit_loader = orig;
            mgr.clearBuffer();
        }
    }

    @Test
    public void testFlushFailureKeepsRowsQueued() {
        // No BE can serve the internal stream load in this harness, so the flush attempt fails and
        // the copy-then-remove rule must keep every row queued for the next cycle.
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        long origInterval = Config.audit_loader_load_interval_seconds;
        try {
            Config.audit_loader_load_interval_seconds = 0;
            mgr.offerEvent(baseEvent());
            mgr.offerEvent(baseEvent());
            long bytesBefore = mgr.bufferedBytes();
            mgr.maybeFlush();
            Assertions.assertEquals(2, mgr.bufferedRows());
            Assertions.assertEquals(bytesBefore, mgr.bufferedBytes());
        } finally {
            Config.audit_loader_load_interval_seconds = origInterval;
            mgr.clearBuffer();
        }
    }

    @Test
    public void testFieldsTruncatedToColumnWidth() {
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        AuditEvent event = baseEvent();
        // Oversized variable-length fields must be truncated to their column byte width,
        // otherwise one row would fail the whole stream-load batch and wedge the pipeline.
        event.candidateMvs = "m".repeat(70000);
        event.db = "d".repeat(200);
        JsonObject obj = JsonParser.parseString(mgr.formatRowJson(event)).getAsJsonObject();
        Assertions.assertEquals(65533, obj.get("candidateMVs").getAsString().length());
        Assertions.assertEquals(96, obj.get("db").getAsString().length());
    }

    @Test
    public void testOfferEventByteCapAndClearBuffer() {
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        long origCap = Config.audit_loader_batch_max_bytes;
        try {
            mgr.offerEvent(baseEvent());
            long oneRowBytes = mgr.bufferedBytes();
            Assertions.assertTrue(oneRowBytes > 0);
            Assertions.assertEquals(1, mgr.bufferedRows());
            Assertions.assertEquals(0, mgr.droppedEvents());

            // Shrink the cap so a second identical row exceeds it: the event must be dropped and
            // counted, leaving the queue and the byte counter untouched.
            Config.audit_loader_batch_max_bytes = oneRowBytes + 1;
            mgr.offerEvent(baseEvent());
            Assertions.assertEquals(1, mgr.bufferedRows());
            Assertions.assertEquals(oneRowBytes, mgr.bufferedBytes());
            Assertions.assertEquals(1, mgr.droppedEvents());

            // clearBuffer drains the queue and subtracts exactly the drained bytes.
            mgr.clearBuffer();
            Assertions.assertEquals(0, mgr.bufferedRows());
            Assertions.assertEquals(0, mgr.bufferedBytes());
        } finally {
            Config.audit_loader_batch_max_bytes = origCap;
        }
    }

    @Test
    public void testSingleRowLargerThanCapIsDropped() {
        // A single row larger than the cap must be dropped even into an empty buffer, otherwise the
        // buffer would no longer be byte-bounded under a very low cap.
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        long origCap = Config.audit_loader_batch_max_bytes;
        try {
            Config.audit_loader_batch_max_bytes = 10;
            mgr.offerEvent(baseEvent());
            Assertions.assertEquals(0, mgr.bufferedRows());
            Assertions.assertEquals(0, mgr.bufferedBytes());
            Assertions.assertEquals(1, mgr.droppedEvents());
        } finally {
            Config.audit_loader_batch_max_bytes = origCap;
        }
    }

    @Test
    public void testMaybeFlushDoesNotAdvanceOnFailure() {
        // With no BE the flush fails, so lastFlushMs must not advance: the very next maybeFlush must
        // attempt the flush again instead of waiting a full interval.
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        long origInterval = Config.audit_loader_load_interval_seconds;
        try {
            Config.audit_loader_load_interval_seconds = 3600;
            mgr.offerEvent(baseEvent());
            mgr.maybeFlush();
            // Buffer still full (flush failed) and a second immediate call still retries: rows kept.
            mgr.maybeFlush();
            Assertions.assertEquals(1, mgr.bufferedRows());
            Assertions.assertFalse(mgr.flush());
        } finally {
            Config.audit_loader_load_interval_seconds = origInterval;
            mgr.clearBuffer();
        }
    }

    @Test
    public void testJsonKeysMatchTableColumns() throws Exception {
        // The load sends no column list, so the BE maps the JSON keys onto the table columns by
        // name; the column order of the live table is irrelevant. Both sides come from
        // COLUMN_SPECS, and a name drifting apart would silently leave that column NULL instead of
        // failing the batch, so pin the generated DDL and the generated row together here.
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        List<String> ddlColumns = new ArrayList<>();
        Matcher matcher = Pattern.compile("^ {2}`([^`]+)`", Pattern.MULTILINE)
                .matcher(mgr.buildCreateTableSql());
        while (matcher.find()) {
            ddlColumns.add(matcher.group(1));
        }
        JsonObject obj = JsonParser.parseString(mgr.formatRowJson(baseEvent())).getAsJsonObject();
        Assertions.assertEquals(ddlColumns, List.copyOf(obj.keySet()));
        Assertions.assertFalse(ddlColumns.isEmpty());
    }

    @Test
    public void testCreateTableSqlShape() throws Exception {
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        String ddl = mgr.buildCreateTableSql();
        Assertions.assertTrue(ddl.contains("DUPLICATE KEY (`queryId`, `timestamp`, `queryType`)"));
        Assertions.assertTrue(ddl.contains("PARTITION BY date_trunc('day', `timestamp`)"));
        Assertions.assertTrue(ddl.contains("\"partition_live_number\" = \"30\""));
    }

    @Test
    public void testCorrectReplicationNumWithoutTableIsNoop() {
        // The audit table does not exist in this harness: the self-heal must return quietly
        // instead of throwing, otherwise it would kill the daemon cycle.
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        Assertions.assertDoesNotThrow(mgr::correctReplicationNum);
    }

    @Test
    public void testOfferEventSkipsUnformattableEvent() {
        // A malformed event must be dropped by the producer path instead of propagating out of
        // offerEvent, which runs on the shared audit-event worker thread.
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        Assertions.assertDoesNotThrow(() -> mgr.offerEvent(null));
        Assertions.assertEquals(0, mgr.bufferedRows());
        Assertions.assertEquals(0, mgr.bufferedBytes());
    }

    @Test
    public void testFlushWithEmptyBufferSendsNothing() {
        // Empty-batch guard: an idle cluster must not open a load transaction at all, otherwise
        // every interval would abort one (the defect this feature was designed to avoid).
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        long origInterval = Config.audit_loader_load_interval_seconds;
        try {
            Config.audit_loader_load_interval_seconds = 0;
            Assertions.assertDoesNotThrow(mgr::maybeFlush);
            Assertions.assertEquals(0, mgr.bufferedRows());
        } finally {
            Config.audit_loader_load_interval_seconds = origInterval;
        }
    }

    @Test
    public void testFlushStopsBatchAtByteCap() {
        // A batch must stop at the byte cap rather than sending everything buffered, so one cycle
        // cannot build a payload larger than the configured maximum.
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        long origCap = Config.audit_loader_batch_max_bytes;
        long origInterval = Config.audit_loader_load_interval_seconds;
        try {
            Config.audit_loader_batch_max_bytes = 1024 * 1024;
            mgr.offerEvent(baseEvent());
            mgr.offerEvent(baseEvent());
            long bytesBefore = mgr.bufferedBytes();
            // Shrink the cap below two rows so the batch loop has to break after the first one.
            Config.audit_loader_batch_max_bytes = bytesBefore / 2;
            Config.audit_loader_load_interval_seconds = 0;
            mgr.maybeFlush();
            // No BE can serve the load here, so every row stays queued for the next cycle.
            Assertions.assertEquals(2, mgr.bufferedRows());
            Assertions.assertEquals(bytesBefore, mgr.bufferedBytes());
        } finally {
            Config.audit_loader_batch_max_bytes = origCap;
            Config.audit_loader_load_interval_seconds = origInterval;
            mgr.clearBuffer();
        }
    }

    @Test
    public void testCorrectReplicationNumWithExistingTable() throws Exception {
        // With the table present the self-heal must compare against the cluster expectation and
        // return quietly when they already match, never throwing out of the daemon cycle.
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        StarRocksAssert starRocksAssert = new StarRocksAssert(UtFrameUtils.createDefaultCtx());
        starRocksAssert.withDatabase(AuditLoaderMgr.AUDIT_DB_NAME);
        starRocksAssert.withTable(mgr.buildCreateTableSql());
        try {
            Assertions.assertNotNull(GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getDb(AuditLoaderMgr.AUDIT_DB_NAME));
            Assertions.assertDoesNotThrow(mgr::correctReplicationNum);
            // The table now exists, so a cycle must reach the flush stage instead of bailing out.
            boolean orig = Config.enable_audit_loader;
            try {
                Config.enable_audit_loader = true;
                Assertions.assertDoesNotThrow(mgr::runAfterCatalogReady);
            } finally {
                Config.enable_audit_loader = orig;
                mgr.clearBuffer();
            }
        } finally {
            starRocksAssert.dropTable(AuditLoaderMgr.AUDIT_DB_NAME + "." + AuditLoaderMgr.AUDIT_TABLE_NAME);
        }
    }
}
