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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.common.Config;
import com.starrocks.common.util.DigitalVersion;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.persist.UninstallPluginLog;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.plugin.AuditEvent.EventType;
import com.starrocks.plugin.PluginInfo;
import com.starrocks.plugin.PluginInfo.PluginType;
import com.starrocks.plugin.PluginMgr;
import com.starrocks.plugin.PluginTestUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AuditLoaderTest {
    // Scratch plugin_dir for the conflict test, so installing its zip cannot collide with the
    // directory PluginMgrTest manages.
    private static final String CONFLICT_PLUGIN_DIR = "target_audit_conflict";

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

    /**
     * An event whose serialized row is about {@code stmtBytes} long, for the cap tests: the cap
     * offerEvent/flush actually enforce never drops below
     * {@link AuditLoaderMgr#MIN_SAFE_BATCH_MAX_BYTES}, so a row only bumps against it if the row
     * itself is built on that scale. Lowering the config instead has no effect.
     */
    private static AuditEvent eventWithStmtBytes(int stmtBytes) {
        AuditEvent event = baseEvent();
        event.stmt = "s".repeat(stmtBytes);
        return event;
    }

    /**
     * An event whose serialized row exceeds {@link AuditLoaderMgr#MIN_SAFE_BATCH_MAX_BYTES}. stmt
     * and the wide text columns are truncated to their column widths, which together stay just
     * under the floor by construction, so the excess has to come from QueriedRelations: it is the
     * one column whose size the floor deliberately does not account for.
     */
    private static AuditEvent eventLargerThanFloor() {
        AuditEvent event = baseEvent();
        event.stmt = "s".repeat(1048576);
        event.candidateMvs = "c".repeat(70000);
        event.hitMVs = "h".repeat(70000);
        List<String> relations = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            relations.add("db1.relation_padded_to_sixty_characters_for_bulk_" + i);
        }
        event.queriedRelations = relations;
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
            // eventFilter() gates on the shared singleton's conflict flag, and that singleton is a
            // live daemon: a sibling test that installs an AUDIT plugin can make it latch the
            // conflict on one of its own cycles. This test is about the Config/event-type gating,
            // so state the precondition instead of inheriting whatever the suite left behind.
            mgr.setDisabledByConflict(false);
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
    public void testDetectConflictWithExternalAuditPluginInstalledAndUninstalled() throws Exception {
        // Exercises detectConflict() itself (through runAfterCatalogReady()), not the
        // @VisibleForTesting setDisabledByConflict() setter: install a real dynamic AUDIT plugin
        // the same way PluginMgr restores one from the edit log, and verify the builtin loader
        // goes inert while it is installed and recovers as soon as it is uninstalled again.
        //
        // The plugin has to be backed by a real zip. A loader is only visible to
        // getAllDynamicPluginInfo() once its getPluginInfo() succeeds, and for a bare in-memory
        // PluginInfo (no source) that call throws on "empty plugin source path" every time and the
        // loader is then silently skipped, so detectConflict() would never see the plugin at all.
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        PluginMgr pluginMgr = GlobalStateMgr.getCurrentState().getPluginMgr();
        String pluginName = "test_conflict_audit_plugin";
        PluginInfo conflictingPlugin = new PluginInfo(pluginName, PluginType.AUDIT,
                "test external audit plugin", DigitalVersion.CURRENT_PLUGIN_VERSION, DigitalVersion.JDK_1_8_0,
                null, null, PluginTestUtil.getTestPathString("auditdemo.zip"));
        boolean origEnabled = Config.enable_audit_loader;
        String origPluginDir = Config.plugin_dir;
        try {
            // Installing extracts the zip under plugin_dir, so point it at a scratch directory
            // instead of whatever the surrounding suite left configured.
            Config.plugin_dir = PluginTestUtil.getTestPathString(CONFLICT_PLUGIN_DIR);
            FileUtils.deleteQuietly(PluginTestUtil.getTestFile(CONFLICT_PLUGIN_DIR));
            Files.createDirectories(PluginTestUtil.getTestPath(CONFLICT_PLUGIN_DIR));

            Config.enable_audit_loader = true;
            Assertions.assertFalse(mgr.isDisabledByConflict());

            pluginMgr.replayLoadDynamicPlugin(conflictingPlugin);
            mgr.offerEvent(baseEvent());
            mgr.runAfterCatalogReady();
            Assertions.assertTrue(mgr.isDisabledByConflict());
            // Superseded: whatever was buffered is dropped instead of growing unboundedly.
            Assertions.assertEquals(0, mgr.bufferedRows());

            pluginMgr.replayUninstallPlugin(new UninstallPluginLog(pluginName));
            mgr.runAfterCatalogReady();
            Assertions.assertFalse(mgr.isDisabledByConflict());
        } finally {
            Config.enable_audit_loader = origEnabled;
            // Safe even if the plugin was already removed above: PluginMgr swallows and logs a
            // "does not exist" failure instead of throwing.
            pluginMgr.replayUninstallPlugin(new UninstallPluginLog(pluginName));
            FileUtils.deleteQuietly(PluginTestUtil.getTestFile(CONFLICT_PLUGIN_DIR));
            Config.plugin_dir = origPluginDir;
            // The live singleton daemon may have latched the conflict on one of its own cycles
            // while the plugin above was installed; it would otherwise only recover on its next
            // cycle, leaving whatever test runs in between gated off.
            GlobalStateMgr.getCurrentState().getAuditLoaderMgr().setDisabledByConflict(false);
            mgr.clearBuffer();
        }
    }

    /**
     * The row is built by streaming into a JsonWriter rather than via a JsonObject tree. Gson still
     * owns the escaping, and this pins that down: every one of these values must survive a
     * serialize/parse round trip unchanged, because a row that escapes badly is malformed JSON that
     * the stream load rejects -- and only for the rows that happen to contain such a character.
     *
     * <p>Verified byte-for-byte against the previous tree-based implementation when the writer was
     * introduced; this keeps that guarantee from silently regressing.
     */
    @Test
    public void testFormatRowJsonEscapesNastyValues() {
        AuditLoaderMgr mgr = new AuditLoaderMgr();

        // Built from char codes rather than embedded literally, so the real control characters
        // reach the writer without a raw NUL byte sitting in this source file. They are exactly
        // where an escaping bug would show up.
        String nul = String.valueOf((char) 0x00);
        String lowCtl = "" + (char) 0x01 + (char) 0x0b + (char) 0x1f;
        String lineAndParaSep = "" + (char) 0x2028 + (char) 0x2029;
        String del = String.valueOf((char) 0x7f);

        List<String> nasty = List.of(
                "SELECT 1",
                "SELECT \"quoted\" FROM t",
                "SELECT 'single' FROM t",
                "a\\b backslash",
                "line\nbreak\r\nand\ttab",
                "\b\f control",
                "nul[" + nul + "] low[" + lowCtl + "]",
                "unicode \u4e2d\u6587 emoji \ud83d\ude80 ok", // CJK plus a surrogate pair
                "seps[" + lineAndParaSep + "]",
                "html <script>&amp;</script> = ' \"",
                "trailing backslash \\",
                "del[" + del + "]",
                "");

        for (String value : nasty) {
            AuditEvent event = baseEvent();
            // stmt and QueriedRelations both have byte widths far above these values, so anything
            // that comes back different is an escaping fault rather than truncation.
            event.stmt = value;
            event.queriedRelations = List.of(value, "db1.t1");
            JsonObject obj = JsonParser.parseString(mgr.formatRowJson(event)).getAsJsonObject();
            Assertions.assertEquals(value, obj.get("stmt").getAsString(), "stmt did not survive: " + value);
            Assertions.assertEquals(value, obj.get("QueriedRelations").getAsJsonArray().get(0).getAsString(),
                    "relation did not survive: " + value);
        }

        // A null string column is written as JSON null, matching what the external audit loader
        // plugin puts in these nullable columns; see testNullTextColumnsAreWrittenAsJsonNull.
        // A null relation list stays an empty array rather than null, since the column is an ARRAY.
        AuditEvent nulls = baseEvent();
        nulls.stmt = null;
        nulls.candidateMvs = null;
        nulls.queriedRelations = null;
        JsonObject nullObj = JsonParser.parseString(mgr.formatRowJson(nulls)).getAsJsonObject();
        Assertions.assertTrue(nullObj.get("stmt").isJsonNull());
        Assertions.assertTrue(nullObj.get("candidateMVs").isJsonNull());
        Assertions.assertEquals(0, nullObj.get("QueriedRelations").getAsJsonArray().size());
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
            // The enforced cap is floored at MIN_SAFE_BATCH_MAX_BYTES, so the second row can only
            // be made to exceed it by using rows over half the floor rather than by shrinking the
            // config (which would simply be clamped back up).
            Config.audit_loader_batch_max_bytes = AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES;
            mgr.offerEvent(eventWithStmtBytes(800000));
            long oneRowBytes = mgr.bufferedBytes();
            Assertions.assertTrue(oneRowBytes > AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES / 2);
            Assertions.assertEquals(1, mgr.bufferedRows());
            Assertions.assertEquals(0, mgr.droppedEvents());

            // A second row of the same size no longer fits under the cap: it must be dropped and
            // counted, leaving the queue and the byte counter untouched.
            mgr.offerEvent(eventWithStmtBytes(800000));
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
        // buffer would no longer be byte-bounded. The cap cannot be lowered under a row (it is
        // floored at MIN_SAFE_BATCH_MAX_BYTES), so this uses a row that exceeds the floor itself.
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        long origCap = Config.audit_loader_batch_max_bytes;
        try {
            Config.audit_loader_batch_max_bytes = AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES;
            mgr.offerEvent(eventLargerThanFloor());
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

    @Test
    public void testWatermarkTriggersFlushBeforeInterval() {
        // The early-flush threshold must sit below the byte cap. offerEvent only admits a row while
        // the buffer stays within the cap, so a threshold equal to the cap could never be reached
        // and every event arriving after the buffer filled up would be dropped until the interval.
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        long origCap = Config.audit_loader_batch_max_bytes;
        long origInterval = Config.audit_loader_load_interval_seconds;
        try {
            // A long interval so only the buffer size can make a flush due.
            Config.audit_loader_load_interval_seconds = 3600;
            // The enforced cap is floored at MIN_SAFE_BATCH_MAX_BYTES, so the watermark sits around
            // half a megabyte: the buffer has to be filled on that scale to reach it.
            Config.audit_loader_batch_max_bytes = AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES;
            mgr.offerEvent(baseEvent());
            Assertions.assertFalse(mgr.shouldFlushNow(), "a nearly empty buffer must wait");

            mgr.offerEvent(eventWithStmtBytes(800000));
            long buffered = mgr.bufferedBytes();

            // Exactly half the cap: this is the case a "buffer >= cap" threshold would miss, and
            // offerEvent never lets the buffer go past the cap, so it has to be enough.
            Config.audit_loader_batch_max_bytes = buffered * 2;
            Assertions.assertTrue(mgr.shouldFlushNow(), "half the cap must trigger a flush");

            // Just under half: the threshold has to be a real one, not always true.
            Config.audit_loader_batch_max_bytes = buffered * 2 + 2;
            Assertions.assertFalse(mgr.shouldFlushNow(), "below the watermark must still wait");
        } finally {
            Config.audit_loader_batch_max_bytes = origCap;
            Config.audit_loader_load_interval_seconds = origInterval;
            mgr.clearBuffer();
        }
    }

    @Test
    public void testEmptyBufferIsNeverFlushDue() {
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        long origInterval = Config.audit_loader_load_interval_seconds;
        try {
            Config.audit_loader_load_interval_seconds = 0;
            Assertions.assertFalse(mgr.shouldFlushNow());
        } finally {
            Config.audit_loader_load_interval_seconds = origInterval;
        }
    }

    @Test
    public void testRepeatedlyFailingBatchIsDiscarded() {
        // No BE can serve the load here, so every attempt fails. A batch the table keeps rejecting
        // must eventually be dropped and counted, otherwise it blocks everything queued behind it.
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        long origInterval = Config.audit_loader_load_interval_seconds;
        try {
            Config.audit_loader_load_interval_seconds = 0;
            mgr.offerEvent(baseEvent());
            long droppedBefore = mgr.droppedEvents();
            int attempts = 0;
            while (mgr.bufferedRows() > 0 && attempts < 100) {
                mgr.flush();
                attempts++;
            }
            Assertions.assertEquals(0, mgr.bufferedRows(), "a permanently failing batch must not stay queued");
            Assertions.assertEquals(0, mgr.bufferedBytes(), "the byte counter must follow the queue");
            Assertions.assertEquals(droppedBefore + 1, mgr.droppedEvents(), "the loss must be counted");
            // It has to survive several retries first, not be thrown away on the first failure.
            Assertions.assertTrue(attempts > 1, "the batch must be retried before being discarded");
        } finally {
            Config.audit_loader_load_interval_seconds = origInterval;
            mgr.clearBuffer();
        }
    }

    @Test
    public void testUtf8LengthMatchesJdkEncoding() {
        // offerEvent sizes the buffer with utf8Length and the request body is encoded by the JDK,
        // so the two must agree exactly or the byte cap drifts away from what is actually sent.
        // Lone surrogates are built here rather than written as source escapes: Gson's JsonWriter
        // passes them through, and the JDK encoder replaces each with a single-byte '?'.
        String highSurrogate = String.valueOf((char) 0xD83D);
        String lowSurrogate = String.valueOf((char) 0xDE80);
        String[] samples = {
                "",
                "plain ascii",
                "\u00e9\u00fc\u00f1", // e-acute, u-umlaut, n-tilde: 2 UTF-8 bytes each
                "\u4e2d\u6587\u5ba1\u8ba1", // CJK ideographs: 3 UTF-8 bytes each
                "emoji " + highSurrogate + lowSurrogate,
                "lone high " + highSurrogate,
                "lone low " + lowSurrogate + " tail",
                highSurrogate + lowSurrogate + highSurrogate,
                "{\"stmt\":\"SELECT '\u4e2d\u6587' " + highSurrogate + lowSurrogate + "\"}", // CJK inside JSON
        };
        for (String sample : samples) {
            Assertions.assertEquals(sample.getBytes(StandardCharsets.UTF_8).length,
                    AuditLoaderMgr.utf8Length(sample), "utf8Length must match the JDK encoder for: " + sample);
        }
    }

    @Test
    public void testTruncateToBytesReturnsSameInstanceWhenItFits() {
        // The fast path must not rebuild a value that already fits: every VARCHAR column of every
        // audit event goes through here.
        String fits = "\u4e2d\u6587"; // CJK ideographs: 6 UTF-8 bytes, fits in 64
        Assertions.assertSame(fits, AuditLoaderMgr.truncateToBytes(fits, 64));
        Assertions.assertEquals("", AuditLoaderMgr.truncateToBytes(null, 64));
    }

    @Test
    public void testStatusReportIsSilentWhenIdleAndThrottledOtherwise() {
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        try {
            // Nothing buffered, nothing dropped, nothing failing: an idle FE must not pay a line
            // per interval, and the throttle slot must not be consumed either.
            Assertions.assertFalse(mgr.reportStatus(), "an idle loader must stay silent");
            Assertions.assertFalse(mgr.reportStatus(), "an idle loader must stay silent");

            // Something to say: exactly one line, then throttled until the interval elapses.
            mgr.offerEvent(baseEvent());
            Assertions.assertTrue(mgr.reportStatus(), "a non-empty buffer must be reported");
            Assertions.assertFalse(mgr.reportStatus(), "the status line must be throttled");

            // Reporting must not disturb the buffer it describes.
            Assertions.assertEquals(1, mgr.bufferedRows());
            Assertions.assertTrue(mgr.bufferedBytes() > 0);
        } finally {
            mgr.clearBuffer();
        }
    }

    @Test
    public void testBatchMaxBytesIsFlooredAtOneWorstCaseRow() {
        // audit_loader_batch_max_bytes is mutable, so an operator can set it below what a single
        // worst-case row needs. Without the floor offerEvent would reject every such row outright,
        // silently losing exactly the large statements an audit trail most needs to keep.
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        long orig = Config.audit_loader_batch_max_bytes;
        try {
            Config.audit_loader_batch_max_bytes = 1024;
            Assertions.assertEquals(AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES, mgr.effectiveBatchMaxBytes());
            Config.audit_loader_batch_max_bytes = AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES - 1;
            Assertions.assertEquals(AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES, mgr.effectiveBatchMaxBytes());
            // At or above the floor the configured value is honoured unchanged.
            Config.audit_loader_batch_max_bytes = AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES;
            Assertions.assertEquals(AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES, mgr.effectiveBatchMaxBytes());
            Config.audit_loader_batch_max_bytes = 64L * 1024 * 1024;
            Assertions.assertEquals(64L * 1024 * 1024, mgr.effectiveBatchMaxBytes());

            // The behaviour the floor exists for: a worst-case row is still admitted under a tiny cap.
            Config.audit_loader_batch_max_bytes = 1024;
            mgr.offerEvent(eventWithStmtBytes(1048576));
            Assertions.assertEquals(1, mgr.bufferedRows(), "a worst-case row must survive a tiny cap");
            Assertions.assertEquals(0, mgr.droppedEvents());
        } finally {
            Config.audit_loader_batch_max_bytes = orig;
            mgr.clearBuffer();
        }
    }

    @Test
    public void testNonFiniteCostsDoNotBreakTheRow() {
        // CostEstimate.INFINITE reaches planCpuCosts/planMemCosts straight from the optimizer, so
        // this is normal operation rather than corrupt input. A lenient writer emits it as a bare
        // Infinity token, which is not JSON: the BE then rejects the whole payload, and a parse
        // error is not something max_filter_ratio can absorb, so one such value costs the batch.
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        AuditEvent event = baseEvent();
        event.planCpuCosts = Double.POSITIVE_INFINITY;
        event.planMemCosts = Double.NaN;
        String row = mgr.formatRowJson(event);
        Assertions.assertFalse(row.contains("Infinity"), row);
        Assertions.assertFalse(row.contains("NaN"), row);
        JsonObject obj = JsonParser.parseString(row).getAsJsonObject();
        Assertions.assertEquals(0, obj.get("planCpuCosts").getAsDouble());
        Assertions.assertEquals(0, obj.get("planMemCosts").getAsDouble());

        // A finite value must still be written unchanged.
        event.planCpuCosts = 12.5;
        obj = JsonParser.parseString(mgr.formatRowJson(event)).getAsJsonObject();
        Assertions.assertEquals(12.5, obj.get("planCpuCosts").getAsDouble());
    }

    @Test
    public void testNullTextColumnsAreWrittenAsJsonNull() {
        // These columns are nullable and the external audit loader plugin writes NULL into them.
        // Writing "" instead would make IS NULL miss exactly the rows this feature produced, on a
        // table the two are meant to share.
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        AuditEvent event = baseEvent();
        event.candidateMvs = null;
        event.hitMVs = null;
        JsonObject obj = JsonParser.parseString(mgr.formatRowJson(event)).getAsJsonObject();
        Assertions.assertTrue(obj.get("candidateMVs").isJsonNull(), "candidateMVs must be JSON null");
        Assertions.assertTrue(obj.get("hitMvs").isJsonNull(), "hitMvs must be JSON null");
        // A present value is still written as a string.
        event.candidateMvs = "mv1";
        obj = JsonParser.parseString(mgr.formatRowJson(event)).getAsJsonObject();
        Assertions.assertEquals("mv1", obj.get("candidateMVs").getAsString());
    }

    @Test
    public void testTimestampFormattingIsZoneDriven() {
        // The formatting itself takes an explicit zone, so this needs no global state: rows from
        // every FE land in one table and are read back with SQL, and the JVM default would drift
        // per process and scatter rows across the wrong daily partitions.
        long epoch = 1700000000000L;
        Assertions.assertEquals("2023-11-14 22:13:20",
                AuditLoaderMgr.formatTimestamp(epoch, ZoneId.of("UTC")));
        Assertions.assertEquals("2023-11-15 06:13:20",
                AuditLoaderMgr.formatTimestamp(epoch, ZoneId.of("Asia/Shanghai")));
        Assertions.assertEquals("2023-11-14 17:13:20",
                AuditLoaderMgr.formatTimestamp(epoch, ZoneId.of("America/New_York")));
    }

    @Test
    public void testTimestampUsesClusterTimeZoneNotJvmDefault() {
        // The wiring check for the above: the row has to be stamped with the cluster's zone. Kept
        // deliberately small, since it is the only part that has to touch the shared time_zone.
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        AuditEvent event = baseEvent();
        event.timestamp = 1700000000000L;
        SessionVariable clusterDefault =
                GlobalStateMgr.getCurrentState().getVariableMgr().getDefaultSessionVariable();
        ConnectContext ctx = ConnectContext.get();
        String origCluster = clusterDefault.getTimeZone();
        String origSession = ctx == null ? null : ctx.getSessionVariable().getTimeZone();
        // Pick a zone the JVM is definitely not in, so a reverted implementation cannot pass by
        // coincidence on a machine that happens to run in the configured zone.
        String foreign = ZoneId.systemDefault().getId().equals("UTC") ? "Asia/Tokyo" : "UTC";
        try {
            setTimeZone(clusterDefault, ctx, foreign);
            String written = JsonParser.parseString(mgr.formatRowJson(event)).getAsJsonObject()
                    .get("timestamp").getAsString();
            Assertions.assertEquals(AuditLoaderMgr.formatTimestamp(event.timestamp, ZoneId.of(foreign)), written);
        } finally {
            setTimeZone(clusterDefault, ctx, origCluster);
            if (ctx != null && origSession != null) {
                ctx.getSessionVariable().setTimeZone(origSession);
            }
        }
    }

    private static void setTimeZone(SessionVariable clusterDefault, ConnectContext ctx, String zone) {
        clusterDefault.setTimeZone(zone);
        if (ctx != null) {
            ctx.getSessionVariable().setTimeZone(zone);
        }
    }

    @Test
    public void testFlushIntervalCorrectsNegativeConfig() {
        // The counterpart to testBatchMaxBytesIsFlooredAtOneWorstCaseRow for the other mutable knob.
        // Zero is a legitimate setting -- it asks for a flush on every daemon cycle, which the cycle
        // length already bounds -- so only negatives, which have no defined meaning, are corrected.
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        long orig = Config.audit_loader_load_interval_seconds;
        try {
            Config.audit_loader_load_interval_seconds = -5;
            Assertions.assertEquals(0, mgr.effectiveFlushIntervalMs());
            Config.audit_loader_load_interval_seconds = 0;
            Assertions.assertEquals(0, mgr.effectiveFlushIntervalMs());
            Config.audit_loader_load_interval_seconds = 60;
            Assertions.assertEquals(60_000, mgr.effectiveFlushIntervalMs());

            // The behaviour the correction exists for: a negative interval must leave the flush
            // decision well defined rather than making it depend on a negative comparison.
            Config.audit_loader_load_interval_seconds = -5;
            mgr.offerEvent(baseEvent());
            Assertions.assertTrue(mgr.shouldFlushNow(), "a corrected interval must still make a flush due");
        } finally {
            Config.audit_loader_load_interval_seconds = orig;
            mgr.clearBuffer();
        }
    }

    @Test
    public void testEventWithoutTimestampFallsBackToNow() {
        // LogUtil builds connection events without ever calling setTimestamp, so every one of them
        // arrives with the AuditEvent default of -1. The row still has to carry a usable time: the
        // column is DATETIME NOT NULL and a 1970 value would put the row in a partition of its own
        // and make the audit trail unreadable. Note the fallback stamps the moment the row is
        // serialized, not the moment the connection happened, which is the closest thing available.
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        ZoneId zone = TimeUtils.getTimeZone().toZoneId();

        for (long missing : new long[] {-1L, 0L}) {
            AuditEvent event = baseEvent();
            event.type = EventType.CONNECTION;
            event.timestamp = missing;
            String before = Instant.now().atZone(zone).format(formatter);
            String written = JsonParser.parseString(mgr.formatRowJson(event)).getAsJsonObject()
                    .get("timestamp").getAsString();
            String after = Instant.now().atZone(zone).format(formatter);
            // The format sorts lexicographically in chronological order, so this brackets "now".
            Assertions.assertTrue(before.compareTo(written) <= 0 && written.compareTo(after) <= 0,
                    "timestamp " + written + " is not between " + before + " and " + after
                            + " for input " + missing);
        }

        // A real timestamp is still written verbatim rather than being replaced by now.
        AuditEvent stamped = baseEvent();
        stamped.timestamp = 1700000000000L;
        String written = JsonParser.parseString(mgr.formatRowJson(stamped)).getAsJsonObject()
                .get("timestamp").getAsString();
        Assertions.assertEquals(Instant.ofEpochMilli(1700000000000L).atZone(zone).format(formatter), written);
    }

    @Test
    public void testSlowQueryClassificationAtThreshold() {
        // The judgment is strictly greater than qe_slow_log_ms, so the equal case belongs to
        // "query". Nothing guards that boundary today: flipping it to >= would turn every query
        // that lands exactly on the threshold into a slow_query without any test going red.
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        long orig = Config.qe_slow_log_ms;
        try {
            Config.qe_slow_log_ms = 1000;
            Assertions.assertEquals("query", queryTypeOf(mgr, 999));
            Assertions.assertEquals("query", queryTypeOf(mgr, 1000), "the threshold itself is not slow");
            Assertions.assertEquals("slow_query", queryTypeOf(mgr, 1001));
        } finally {
            Config.qe_slow_log_ms = orig;
        }
    }

    private static String queryTypeOf(AuditLoaderMgr mgr, long queryTime) {
        AuditEvent event = baseEvent();
        event.queryTime = queryTime;
        return JsonParser.parseString(mgr.formatRowJson(event)).getAsJsonObject()
                .get("queryType").getAsString();
    }

    @Test
    public void testEveryVarcharColumnTruncatesToItsOwnWidth() throws Exception {
        // Driven by the generated DDL rather than a hand-written list, so a column added to
        // COLUMN_SPECS is covered here the day it appears instead of silently going unchecked.
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        AuditEvent event = baseEvent();
        String oversized = "W".repeat(200000);
        for (Field field : AuditEvent.class.getFields()) {
            if (field.getType() == String.class && !Modifier.isStatic(field.getModifiers())) {
                field.set(event, oversized);
            }
        }
        JsonObject obj = JsonParser.parseString(mgr.formatRowJson(event)).getAsJsonObject();
        Matcher matcher = Pattern.compile("^ {2}`([^`]+)` VARCHAR\\((\\d+)\\)", Pattern.MULTILINE)
                .matcher(mgr.buildCreateTableSql());
        int checked = 0;
        while (matcher.find()) {
            String column = matcher.group(1);
            int width = Integer.parseInt(matcher.group(2));
            JsonElement value = obj.get(column);
            Assertions.assertNotNull(value, "column missing from the row: " + column);
            if (value.isJsonNull()) {
                continue;
            }
            int bytes = value.getAsString().getBytes(StandardCharsets.UTF_8).length;
            Assertions.assertTrue(bytes <= width,
                    column + " wrote " + bytes + " bytes into a varchar(" + width + ")");
            checked++;
        }
        Assertions.assertTrue(checked >= 10, "expected the DDL to yield varchar columns, got " + checked);
    }

    @Test
    public void testEmptyRelationListStaysAnEmptyArray() {
        // The mirror image of testNullTextColumnsAreWrittenAsJsonNull: this column is an ARRAY, so
        // "nothing was queried" is an empty array, not null. A blanket "write null when absent"
        // change would quietly break it.
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        AuditEvent event = baseEvent();
        event.queriedRelations = List.of();
        JsonElement empty = JsonParser.parseString(mgr.formatRowJson(event)).getAsJsonObject()
                .get("QueriedRelations");
        Assertions.assertFalse(empty.isJsonNull(), "an empty relation list must not become null");
        Assertions.assertEquals(0, empty.getAsJsonArray().size());
    }

    @Test
    public void testMixedBatchWithNonFiniteRowStaysParseable() {
        // The single-row test proves the value is neutralized; this proves the consequence that
        // actually matters, since the BE parses the whole payload and one bad token would take the
        // entire batch down rather than the row that produced it.
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        StringBuilder payload = new StringBuilder("[");
        for (int i = 0; i < 5; i++) {
            AuditEvent event = baseEvent();
            event.queryId = "qid-" + i;
            if (i == 2) {
                event.planCpuCosts = Double.POSITIVE_INFINITY;
                event.planMemCosts = Double.NEGATIVE_INFINITY;
            }
            payload.append(i > 0 ? "," : "").append(mgr.formatRowJson(event));
        }
        payload.append("]");
        JsonArray rows = JsonParser.parseString(payload.toString()).getAsJsonArray();
        Assertions.assertEquals(5, rows.size());
        Assertions.assertEquals(0,
                rows.get(2).getAsJsonObject().get("planCpuCosts").getAsDouble());
        Assertions.assertEquals("qid-4", rows.get(4).getAsJsonObject().get("queryId").getAsString());
    }

    @Test
    public void testFailureCounterResetsSoTheNextBatchKeepsItsBudget() {
        // A counter left over from an earlier batch would cut the retry budget of an unrelated one,
        // discarding it sooner than MAX_BATCH_FLUSH_RETRY promises. That degradation is silent.
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        long origInterval = Config.audit_loader_load_interval_seconds;
        try {
            Config.audit_loader_load_interval_seconds = 0;
            mgr.offerEvent(baseEvent());
            mgr.flush();
            Assertions.assertTrue(mgr.consecutiveFlushFailures() > 0, "no BE here, so the flush must fail");
            mgr.clearBuffer();
            Assertions.assertEquals(0, mgr.consecutiveFlushFailures(), "clearing must reset the budget");
        } finally {
            Config.audit_loader_load_interval_seconds = origInterval;
            mgr.clearBuffer();
        }
    }

    @Test
    public void testStmtIdBeyondIntRange() {
        // stmtId is BIGINT because it comes from a process-lifetime counter; a long-lived FE goes
        // past INT range, and the value has to survive serialization intact.
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        AuditEvent event = baseEvent();
        event.stmtId = 3_000_000_000L;
        Assertions.assertEquals(3_000_000_000L,
                JsonParser.parseString(mgr.formatRowJson(event)).getAsJsonObject().get("stmtId").getAsLong());
    }

    @Test
    public void testIsQueryMapsToTinyInt() {
        // The column is TINYINT, so the boolean has to arrive as 1/0 rather than true/false.
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        AuditEvent event = baseEvent();
        event.isQuery = true;
        Assertions.assertEquals(1,
                JsonParser.parseString(mgr.formatRowJson(event)).getAsJsonObject().get("isQuery").getAsInt());
        event.isQuery = false;
        Assertions.assertEquals(0,
                JsonParser.parseString(mgr.formatRowJson(event)).getAsJsonObject().get("isQuery").getAsInt());
    }

    @Test
    public void testFlushReportsFailureEvenAfterAPartialSuccess() {
        // No BE can serve the load here, so every batch fails and flush() must say so. The caller
        // only advances the flush clock on a clean run: reporting success because some earlier
        // batch landed would leave the failed one at the head waiting a whole interval, stretching
        // the time to give up on it far past what MAX_BATCH_FLUSH_RETRY promises.
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        long origInterval = Config.audit_loader_load_interval_seconds;
        try {
            Config.audit_loader_load_interval_seconds = 0;
            mgr.offerEvent(baseEvent());
            mgr.offerEvent(baseEvent());
            Assertions.assertFalse(mgr.flush(), "a failed batch must be reported as failure");
            Assertions.assertEquals(2, mgr.bufferedRows(), "the rows stay queued for the retry");
        } finally {
            Config.audit_loader_load_interval_seconds = origInterval;
            mgr.clearBuffer();
        }
    }

    @Test
    public void testClearBufferCountsWhatItThrowsAway() {
        // Switching the feature off, or an external AUDIT plugin taking over, drops up to a whole
        // buffer. Every other way an event is lost here is counted, so this one has to be too:
        // otherwise droppedEvents and the status line would under-report the real loss.
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        long before = mgr.droppedEvents();
        mgr.offerEvent(baseEvent());
        mgr.offerEvent(baseEvent());
        mgr.offerEvent(baseEvent());
        Assertions.assertEquals(before, mgr.droppedEvents(), "buffering alone loses nothing");
        mgr.clearBuffer();
        Assertions.assertEquals(before + 3, mgr.droppedEvents(), "the discarded rows must be counted");
        Assertions.assertEquals(0, mgr.bufferedRows());
        Assertions.assertEquals(0, mgr.bufferedBytes());
        // An empty buffer must not inflate the counter.
        mgr.clearBuffer();
        Assertions.assertEquals(before + 3, mgr.droppedEvents());
    }

    @Test
    public void testConfigKnobsAreBoundedAtBothEnds() {
        // The floor keeps a worst-case row admissible; the ceiling keeps the batch within what the
        // flush path can actually assemble in memory. Leaving either end open would let a single
        // mutable config put the loader into a state it has no defined behaviour for.
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        long origCap = Config.audit_loader_batch_max_bytes;
        long origInterval = Config.audit_loader_load_interval_seconds;
        try {
            Config.audit_loader_batch_max_bytes = 8L * 1024 * 1024 * 1024;
            Assertions.assertEquals(AuditLoaderMgr.MAX_SAFE_BATCH_MAX_BYTES, mgr.effectiveBatchMaxBytes());
            Config.audit_loader_batch_max_bytes = AuditLoaderMgr.MAX_SAFE_BATCH_MAX_BYTES;
            Assertions.assertEquals(AuditLoaderMgr.MAX_SAFE_BATCH_MAX_BYTES, mgr.effectiveBatchMaxBytes());
            Config.audit_loader_batch_max_bytes = 64L * 1024 * 1024;
            Assertions.assertEquals(64L * 1024 * 1024, mgr.effectiveBatchMaxBytes());

            Config.audit_loader_load_interval_seconds = Long.MAX_VALUE / 100;
            long capped = mgr.effectiveFlushIntervalMs();
            Assertions.assertTrue(capped > 0, "a huge interval must not wrap around to a negative");
            Assertions.assertEquals(86400L * 1000, capped);
        } finally {
            Config.audit_loader_batch_max_bytes = origCap;
            Config.audit_loader_load_interval_seconds = origInterval;
        }
    }
}
