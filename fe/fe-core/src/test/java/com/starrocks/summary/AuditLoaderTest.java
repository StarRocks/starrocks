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
import com.google.gson.stream.JsonReader;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
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
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.sun.management.ThreadMXBean;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.starrocks.summary.AuditLoaderMgr.FlushResult.DISCARDED;
import static com.starrocks.summary.AuditLoaderMgr.FlushResult.EMPTY;
import static com.starrocks.summary.AuditLoaderMgr.FlushResult.RETRY;
import static com.starrocks.summary.AuditLoaderMgr.FlushResult.SUCCESS;

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

    // Use JSON escaping to exceed the effective minimum without exceeding stmt's 1 MiB width.
    // Include multibyte text so these fixtures distinguish UTF-8 bytes from Java characters.
    private static AuditEvent eventWithRowBytes(int rowBytes) {
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        AuditEvent event = baseEvent();
        event.stmt = String.valueOf((char) 0).repeat((rowBytes - 1024) / 6) + "中文🚀\"\\\n";
        int current = mgr.formatRowJson(event).getBytes(StandardCharsets.UTF_8).length;
        Assertions.assertTrue(current <= rowBytes);
        event.stmt += "s".repeat(rowBytes - current);
        Assertions.assertEquals(rowBytes, mgr.formatRowJson(event).getBytes(StandardCharsets.UTF_8).length);
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
    public void testMissingTableKeepsRowsWithoutSpendingRetryBudget() {
        AtomicLong clock = new AtomicLong();
        ReadyAuditLoader mgr = new ReadyAuditLoader(clock::get);
        mgr.tableReady = false;
        AtomicInteger calls = mockLoadResults(loadResult(1, 1, 0));
        boolean origEnabled = Config.enable_audit_loader;
        long origInterval = Config.audit_loader_load_interval_seconds;
        try {
            Config.enable_audit_loader = true;
            Config.audit_loader_load_interval_seconds = 0;
            mgr.offerEvent(baseEvent());
            long bytesBefore = mgr.bufferedBytes();
            for (int i = 0; i < 5; i++) {
                mgr.runAfterCatalogReady();
                clock.addAndGet(TimeUnit.SECONDS.toNanos(5));
            }
            Assertions.assertEquals(0, calls.get());
            Assertions.assertEquals(1, mgr.bufferedRows());
            Assertions.assertEquals(bytesBefore, mgr.bufferedBytes());
            Assertions.assertEquals(0, mgr.consecutiveFlushFailures());
            Assertions.assertEquals(0, mgr.droppedEvents());
            mgr.tableReady = true;
            mgr.runAfterCatalogReady();
            Assertions.assertEquals(1, calls.get());
            Assertions.assertEquals(0, mgr.bufferedRows());
            Assertions.assertEquals(0, mgr.bufferedBytes());
        } finally {
            Config.enable_audit_loader = origEnabled;
            Config.audit_loader_load_interval_seconds = origInterval;
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

        // Missing nullable text uses JSON null so it remains distinguishable from empty text;
        // see testNullTextColumnsAreWrittenAsJsonNull. This is the builtin loader's contract.
        // A missing relation list follows the builtin contract: an empty JSON array.
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
        AtomicInteger calls = mockLoadResults(new AuditStreamLoader.Response(500, "rejected"));
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        long origInterval = Config.audit_loader_load_interval_seconds;
        try {
            Config.audit_loader_load_interval_seconds = 0;
            mgr.offerEvent(baseEvent());
            mgr.offerEvent(baseEvent());
            long bytesBefore = mgr.bufferedBytes();
            mgr.maybeFlush();
            Assertions.assertEquals(1, calls.get());
            Assertions.assertEquals(2, mgr.bufferedRows());
            Assertions.assertEquals(bytesBefore, mgr.bufferedBytes());
        } finally {
            Config.audit_loader_load_interval_seconds = origInterval;
            mgr.clearBuffer();
        }
    }

    @Test
    public void testResponseBodyTimeoutKeepsRowsUntilRecovery() {
        List<String> payloads = new ArrayList<>();
        new MockUp<AuditStreamLoader>() {
            @Mock
            public AuditStreamLoader.Response loadBatch(String label, String payload) throws IOException {
                payloads.add(payload);
                if (payloads.size() == 1) {
                    throw new HttpTimeoutException("response body stalled");
                }
                return new AuditStreamLoader.Response(200, "OK");
            }
        };
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        mgr.offerEvent(baseEvent());
        long bytesBefore = mgr.bufferedBytes();
        Assertions.assertEquals(RETRY, mgr.flush());
        Assertions.assertEquals(1, mgr.bufferedRows());
        Assertions.assertEquals(bytesBefore, mgr.bufferedBytes());
        Assertions.assertEquals(1, mgr.consecutiveFlushFailures());
        Assertions.assertEquals(0, mgr.droppedEvents());

        Assertions.assertEquals(SUCCESS, mgr.flush());
        Assertions.assertEquals(2, payloads.size());
        Assertions.assertEquals(payloads.get(0), payloads.get(1));
        Assertions.assertEquals(0, mgr.bufferedRows());
        Assertions.assertEquals(0, mgr.bufferedBytes());
        Assertions.assertEquals(0, mgr.consecutiveFlushFailures());
        Assertions.assertEquals(0, mgr.droppedEvents());
    }

    private static AuditStreamLoader.Response loadResult(long total, long loaded, long filtered) {
        return new AuditStreamLoader.Response(200, "OK", new AuditStreamLoader.LoadStats(total, loaded, filtered,
                "http://be/errors/1"));
    }

    private static AtomicInteger mockLoadResults(AuditStreamLoader.Response... responses) {
        AtomicInteger calls = new AtomicInteger();
        new MockUp<AuditStreamLoader>() {
            @Mock
            public AuditStreamLoader.Response loadBatch(String label, String payload) {
                return responses[Math.min(calls.getAndIncrement(), responses.length - 1)];
            }
        };
        return calls;
    }


    private static class ReadyAuditLoader extends AuditLoaderMgr {
        private final AtomicInteger controlChecks = new AtomicInteger();
        private final AtomicInteger maintenanceChecks = new AtomicInteger();
        private boolean tableReady = true;

        ReadyAuditLoader() {
            this(System::nanoTime);
        }

        ReadyAuditLoader(LongSupplier nanoTime) {
            super(nanoTime);
        }

        @Override
        boolean ensureAuditTable() {
            return tableReady;
        }

        @Override
        protected void runAfterCatalogReady() {
            controlChecks.incrementAndGet();
            super.runAfterCatalogReady();
        }

        @Override
        void correctReplicationNum() {
            maintenanceChecks.incrementAndGet();
        }
    }

    private static void offerRows(AuditLoaderMgr mgr, int count) {
        for (int i = 0; i < count; i++) {
            mgr.offerEvent(baseEvent());
        }
    }

    private static void expireLogThrottle(AuditLoaderMgr mgr, String name) throws Exception {
        Field field = AuditLoaderMgr.class.getDeclaredField(name);
        field.setAccessible(true);
        Object throttle = field.get(mgr);
        Field lastFire = throttle.getClass().getDeclaredField("lastFireMs");
        lastFire.setAccessible(true);
        ((AtomicLong) lastFire.get(throttle)).set(0);
    }

    private static class LoadLogCollector extends AbstractAppender implements AutoCloseable {
        private final List<LogEvent> events = new CopyOnWriteArrayList<>();
        private final Logger logger = (Logger) LogManager.getLogger(AuditLoaderMgr.class);
        private final Level previousLevel = logger.getLevel();

        LoadLogCollector() {
            super("audit-loader-load-test", null, null);
            start();
            logger.addAppender(this);
            logger.setLevel(Level.DEBUG);
        }

        @Override
        public void append(LogEvent event) {
            events.add(event.toImmutable());
        }

        List<String> messages(Level level, String prefix) {
            return events.stream().filter(event -> event.getLevel() == level)
                    .map(event -> event.getMessage().getFormattedMessage())
                    .filter(message -> message.startsWith(prefix)).toList();
        }

        @Override
        public void close() {
            logger.removeAppender(this);
            stop();
            logger.setLevel(previousLevel);
        }
    }

    @Test
    public void testDropReasonsHaveIndependentWarningThrottles() throws Exception {
        long originalCap = Config.audit_loader_batch_max_bytes;
        int cap = (int) AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES;
        try {
            Config.audit_loader_batch_max_bytes = cap;
            AuditEvent fillsBuffer = eventWithRowBytes(cap - 2);
            AuditEvent oversized = eventWithRowBytes(cap);
            long smallRowBytes = new AuditLoaderMgr().formatRowJson(baseEvent()).getBytes(StandardCharsets.UTF_8).length;
            for (boolean fullFirst : new boolean[] {true, false}) {
                AuditLoaderMgr mgr = new AuditLoaderMgr();
                try (LoadLogCollector logs = new LoadLogCollector()) {
                    mgr.offerEvent(fillsBuffer);
                    mgr.offerEvent(fullFirst ? baseEvent() : oversized);
                    mgr.offerEvent(fullFirst ? oversized : baseEvent());
                    mgr.offerEvent(baseEvent());
                    mgr.offerEvent(oversized);
                    Assertions.assertEquals(1, logs.messages(Level.WARN, "audit loader buffer is full:").size());
                    Assertions.assertEquals(1, logs.messages(Level.WARN, "audit loader dropped oversized event:").size());
                    Assertions.assertEquals(4, mgr.droppedEvents(), "log throttling must not suppress accounting");
                    Assertions.assertEquals(1, mgr.bufferedRows());
                    Assertions.assertEquals(cap - 2, mgr.bufferedBytes());
                    String fullWarning = logs.messages(Level.WARN, "audit loader buffer is full:").get(0);
                    Assertions.assertTrue(fullWarning.contains("buffered_bytes=" + (cap - 2)));
                    Assertions.assertTrue(fullWarning.contains("row_bytes=" + smallRowBytes));
                    Assertions.assertTrue(fullWarning.contains("batch_limit_bytes=" + cap));

                    expireLogThrottle(mgr, "bufferFullWarnThrottle");
                    mgr.offerEvent(baseEvent());
                    mgr.offerEvent(oversized);
                    Assertions.assertEquals(2, logs.messages(Level.WARN, "audit loader buffer is full:").size());
                    Assertions.assertEquals(1, logs.messages(Level.WARN, "audit loader dropped oversized event:").size());
                    expireLogThrottle(mgr, "oversizedRowWarnThrottle");
                    mgr.offerEvent(oversized);
                    mgr.offerEvent(baseEvent());
                    Assertions.assertEquals(2, logs.messages(Level.WARN, "audit loader buffer is full:").size());
                    Assertions.assertEquals(2, logs.messages(Level.WARN, "audit loader dropped oversized event:").size());
                    Assertions.assertEquals(8, mgr.droppedEvents());
                    Assertions.assertEquals(cap - 2, mgr.bufferedBytes());
                }
            }
        } finally {
            Config.audit_loader_batch_max_bytes = originalCap;
        }
    }

    @Test
    public void testFilteredSuccessDequeuesWholeBatchAndCountsLossOnce() {
        AtomicInteger calls = mockLoadResults(new AuditStreamLoader.Response(500, "retry"), loadResult(20, 19, 1));
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        offerRows(mgr, 20);
        Assertions.assertEquals(RETRY, mgr.flush());
        Assertions.assertEquals(1, mgr.consecutiveFlushFailures());
        Assertions.assertEquals(0, mgr.droppedEvents());
        Assertions.assertEquals(SUCCESS, mgr.flush());
        Assertions.assertEquals(0, mgr.bufferedRows());
        Assertions.assertEquals(0, mgr.bufferedBytes());
        Assertions.assertEquals(0, mgr.consecutiveFlushFailures());
        Assertions.assertEquals(1, mgr.filteredEvents());
        Assertions.assertEquals(1, mgr.droppedEvents());
        Assertions.assertEquals(0, mgr.batchesWithUnknownStatistics());
        Assertions.assertEquals(EMPTY, mgr.flush(), "a committed batch must never be retried");
        Assertions.assertEquals(2, calls.get());
        Assertions.assertEquals(1, mgr.droppedEvents());
    }

    @Test
    public void testFilteringLogsAreThrottledButEveryBatchIsCounted() throws Exception {
        AtomicInteger calls = mockLoadResults(loadResult(20, 19, 1));
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        try (LoadLogCollector logs = new LoadLogCollector()) {
            for (int i = 0; i < 10; i++) {
                offerRows(mgr, 20);
                Assertions.assertEquals(SUCCESS, mgr.flush());
            }
            Assertions.assertEquals(10, calls.get());
            Assertions.assertEquals(10, mgr.filteredEvents());
            Assertions.assertEquals(10, mgr.droppedEvents());
            Assertions.assertEquals(10, logs.messages(Level.DEBUG, "audit loader load succeeded:").size());
            List<String> warnings = logs.messages(Level.WARN, "audit loader load succeeded:");
            Assertions.assertEquals(1, warnings.size());
            Assertions.assertTrue(warnings.get(0).contains("submitted=20, total=20, loaded=19, filtered=1"));
            Assertions.assertTrue(warnings.get(0).contains("error_url=http://be/errors/1"));
            offerRows(mgr, 3);
            mgr.clearBuffer();
            Assertions.assertEquals(13, mgr.droppedEvents());
            Assertions.assertEquals(10, mgr.filteredEvents(), "filtering is a subset of the total losses");
            Assertions.assertTrue(mgr.reportStatus(), "filtering must be visible even after the queue is empty");
            Assertions.assertFalse(mgr.reportStatus());
            String status = logs.messages(Level.WARN, "audit loader status:").get(0);
            Assertions.assertTrue(status.contains("dropped 13 events since the last status line (13 total)"));
            Assertions.assertTrue(status.contains("filtered 10 events since the last status line (10 total"));

            expireLogThrottle(mgr, "loadStatsWarnThrottle");
            logs.logger.setLevel(Level.INFO);
            offerRows(mgr, 20);
            Assertions.assertEquals(SUCCESS, mgr.flush());
            Assertions.assertEquals(2, logs.messages(Level.WARN, "audit loader load succeeded:").size());
            Assertions.assertEquals(10, logs.messages(Level.DEBUG, "audit loader load succeeded:").size());
            Assertions.assertFalse(mgr.reportStatus(), "batch warnings and status reports have separate throttles");
            expireLogThrottle(mgr, "statusReportThrottle");
            Assertions.assertTrue(mgr.reportStatus());
            status = logs.messages(Level.WARN, "audit loader status:").get(1);
            Assertions.assertTrue(status.contains("filtered 1 events since the last status line (11 total"));
            Assertions.assertTrue(status.contains("dropped 1 events since the last status line (14 total)"));
        }
    }

    @Test
    public void testHealthyBatchLogsOnlyAtDebugLevel() {
        mockLoadResults(loadResult(20, 20, 0));
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        try (LoadLogCollector logs = new LoadLogCollector()) {
            offerRows(mgr, 20);
            Assertions.assertEquals(SUCCESS, mgr.flush());
            Assertions.assertEquals(1, logs.messages(Level.DEBUG, "audit loader load succeeded:").size());
            Assertions.assertTrue(logs.messages(Level.WARN, "audit loader load succeeded:").isEmpty());
            logs.logger.setLevel(Level.INFO);
            offerRows(mgr, 20);
            Assertions.assertEquals(SUCCESS, mgr.flush());
            Assertions.assertEquals(1, logs.messages(Level.DEBUG, "audit loader load succeeded:").size());
            Assertions.assertEquals(0, mgr.filteredEvents());
            Assertions.assertEquals(0, mgr.droppedEvents());
            Assertions.assertEquals(0, mgr.batchesWithUnknownStatistics());
            Assertions.assertFalse(mgr.reportStatus());
        }
    }

    @Test
    public void testUnknownStatisticsStaySuccessfulAndAppearInStatus() {
        AtomicInteger calls = mockLoadResults(AuditStreamLoader.parseResponse(200,
                "{\"Status\":\"Success\",\"Message\":\"OK\",\"NumberTotalRows\":20,\"NumberLoadedRows\":19}"));
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        try (LoadLogCollector logs = new LoadLogCollector()) {
            for (int i = 0; i < 2; i++) {
                offerRows(mgr, 20);
                Assertions.assertEquals(SUCCESS, mgr.flush());
            }
            Assertions.assertEquals(2, calls.get());
            Assertions.assertEquals(0, mgr.bufferedRows());
            Assertions.assertEquals(0, mgr.droppedEvents(), "unknown filtering must not be guessed from total-loaded");
            Assertions.assertEquals(2, mgr.batchesWithUnknownStatistics());
            List<String> warnings = logs.messages(Level.WARN, "audit loader load succeeded:");
            Assertions.assertEquals(1, warnings.size());
            Assertions.assertTrue(warnings.get(0).contains("filtered=unknown"));
            Assertions.assertTrue(warnings.get(0).contains("row_statistics_incomplete=true"));
            Assertions.assertTrue(mgr.reportStatus());
            Assertions.assertFalse(mgr.reportStatus());
            String status = logs.messages(Level.WARN, "audit loader status:").get(0);
            Assertions.assertTrue(status.contains("2 loads with unknown row statistics since the last status line (2 total)"));
        }
    }

    @Test
    public void testValidFilteredCountSurvivesOtherMissingStatistics() {
        for (AuditStreamLoader.LoadStats stats : new AuditStreamLoader.LoadStats[] {
                new AuditStreamLoader.LoadStats(null, null, 1L, null),
                new AuditStreamLoader.LoadStats(null, 19L, 1L, null),
                new AuditStreamLoader.LoadStats(1L, null, 1L, null)}) {
            mockLoadResults(new AuditStreamLoader.Response(200, "OK", stats));
            AuditLoaderMgr mgr = new AuditLoaderMgr();
            offerRows(mgr, 20);
            Assertions.assertEquals(SUCCESS, mgr.flush());
            Assertions.assertEquals(1, mgr.filteredEvents());
            Assertions.assertEquals(1, mgr.droppedEvents());
            Assertions.assertEquals(1, mgr.batchesWithUnknownStatistics());
        }
    }

    @Test
    public void testInvalidBatchStatisticsDoNotOvercountOrRetry() {
        AtomicReference<AuditStreamLoader.Response> response = new AtomicReference<>();
        AtomicInteger calls = new AtomicInteger();
        new MockUp<AuditStreamLoader>() {
            @Mock
            public AuditStreamLoader.Response loadBatch(String label, String payload) {
                calls.incrementAndGet();
                return response.get();
            }
        };
        for (AuditStreamLoader.Response invalid : new AuditStreamLoader.Response[] {
                loadResult(20, 19, -1), loadResult(20, 19, 21), loadResult(20, 19, Long.MAX_VALUE),
                loadResult(20, 20, 1), new AuditStreamLoader.Response(200, "OK"),
                new AuditStreamLoader.Response(200, "OK", new AuditStreamLoader.LoadStats(null, 20L, 1L, null)),
                new AuditStreamLoader.Response(200, "OK", new AuditStreamLoader.LoadStats(1L, null, 2L, null)),
                new AuditStreamLoader.Response(200, "OK", new AuditStreamLoader.LoadStats(1L, 2L, null, null)),
                new AuditStreamLoader.Response(200, "OK", new AuditStreamLoader.LoadStats(21L, null, 1L, null)),
                new AuditStreamLoader.Response(200, "OK", new AuditStreamLoader.LoadStats(null, -1L, 1L, null)),
                new AuditStreamLoader.Response(200, "OK", new AuditStreamLoader.LoadStats(null, Long.MAX_VALUE, 1L, null)),
                AuditStreamLoader.parseResponse(200, "{\"Status\":\"Success\",\"NumberLoadedRows\":\"20\","
                        + "\"NumberFilteredRows\":1}"),
                AuditStreamLoader.parseResponse(200, "{\"Status\":\"Success\",\"NumberLoadedRows\":1e40,"
                        + "\"NumberFilteredRows\":1}")}) {
            int previousCalls = calls.get();
            response.set(invalid);
            AuditLoaderMgr mgr = new AuditLoaderMgr();
            offerRows(mgr, 20);
            Assertions.assertEquals(SUCCESS, mgr.flush());
            Assertions.assertEquals(0, mgr.bufferedRows());
            Assertions.assertEquals(0, mgr.droppedEvents());
            Assertions.assertEquals(0, mgr.filteredEvents());
            Assertions.assertEquals(1, mgr.batchesWithUnknownStatistics());
            Assertions.assertEquals(0, mgr.consecutiveFlushFailures());
            Assertions.assertEquals(EMPTY, mgr.flush());
            Assertions.assertEquals(previousCalls + 1, calls.get(), "invalid statistics must not retry committed rows");
        }
    }

    @Test
    public void testContradictoryStatisticsLogTheReasonAndRawCounts() {
        mockLoadResults(new AuditStreamLoader.Response(200, "OK", new AuditStreamLoader.LoadStats(null, 20L, 1L, null)));
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        try (LoadLogCollector logs = new LoadLogCollector()) {
            offerRows(mgr, 20);
            Assertions.assertEquals(SUCCESS, mgr.flush());
            String warning = logs.messages(Level.WARN, "audit loader load succeeded:").get(0);
            Assertions.assertTrue(warning.contains("statistics_issue=loaded_plus_filtered_exceeds_limit"));
            Assertions.assertTrue(warning.contains("loadedRows=20, filteredRows=1"));
            Assertions.assertTrue(warning.contains("filtered=unknown"));
            Assertions.assertEquals(0, mgr.droppedEvents());
        }
    }

    @Test
    public void testPublishTimeoutDoesNotRetryOrCountTheCommittedBatchAsLost() {
        AtomicInteger calls = mockLoadResults(AuditStreamLoader.parseResponse(200,
                "{\"Status\":\"Publish Timeout\",\"NumberTotalRows\":20,\"NumberLoadedRows\":20,\"NumberFilteredRows\":0}"));
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        try (LoadLogCollector logs = new LoadLogCollector()) {
            for (int i = 0; i < 2; i++) {
                offerRows(mgr, 20);
                Assertions.assertEquals(SUCCESS, mgr.flush());
                Assertions.assertEquals(EMPTY, mgr.flush());
            }
            Assertions.assertEquals(2, calls.get());
            Assertions.assertEquals(0, mgr.bufferedRows());
            Assertions.assertEquals(0, mgr.bufferedBytes());
            Assertions.assertEquals(0, mgr.droppedEvents());
            Assertions.assertEquals(0, mgr.filteredEvents());
            Assertions.assertEquals(0, mgr.batchesWithUnknownStatistics());
            Assertions.assertEquals(2, mgr.batchesWithPublishTimeout());
            Assertions.assertEquals(0, mgr.consecutiveFlushFailures());
            Assertions.assertEquals(1, logs.messages(Level.WARN, "audit loader load committed, awaiting visibility:").size());
            Assertions.assertTrue(logs.messages(Level.DEBUG, "audit loader load succeeded:").isEmpty());
            Assertions.assertTrue(mgr.reportStatus());
            Assertions.assertTrue(logs.messages(Level.WARN, "audit loader status:").get(0)
                    .contains("2 loads committed with publish timeout since the last status line (2 total)"));
            Assertions.assertFalse(mgr.reportStatus());
        }
    }

    @Test
    public void testPublishTimeoutCountsOnlyTrustedFilteringAndClearsPreviousFailures() {
        AuditStreamLoader.Response[] committed = {
                AuditStreamLoader.parseResponse(200, "{\"Status\":\"Publish Timeout\",\"NumberTotalRows\":20,"
                        + "\"NumberLoadedRows\":19,\"NumberFilteredRows\":1}"),
                AuditStreamLoader.parseResponse(200, "{\"Status\":\"Publish Timeout\",\"NumberLoadedRows\":20,"
                        + "\"NumberFilteredRows\":1}"),
                AuditStreamLoader.parseResponse(200, "{\"Status\":\"Publish Timeout\"}")};
        for (int i = 0; i < committed.length; i++) {
            AtomicReference<AuditStreamLoader.Response> response =
                    new AtomicReference<>(new AuditStreamLoader.Response(500, "fail"));
            new MockUp<AuditStreamLoader>() {
                @Mock
                public AuditStreamLoader.Response loadBatch(String label, String payload) {
                    return response.get();
                }
            };
            AuditLoaderMgr mgr = new AuditLoaderMgr();
            offerRows(mgr, 20);
            Assertions.assertEquals(RETRY, mgr.flush());
            response.set(committed[i]);
            Assertions.assertEquals(SUCCESS, mgr.flush());
            Assertions.assertEquals(EMPTY, mgr.flush());
            Assertions.assertEquals(i == 0 ? 1 : 0, mgr.filteredEvents());
            Assertions.assertEquals(i == 0 ? 1 : 0, mgr.droppedEvents());
            Assertions.assertEquals(i == 0 ? 0 : 1, mgr.batchesWithUnknownStatistics());
            Assertions.assertEquals(1, mgr.batchesWithPublishTimeout());
            Assertions.assertEquals(0, mgr.consecutiveFlushFailures());
            Assertions.assertEquals(0, mgr.bufferedRows());
        }
    }

    @Test
    public void testFailedLoadStatisticsAreNotCountedBeforeDiscard() {
        AtomicInteger calls = mockLoadResults(new AuditStreamLoader.Response(500, "rejected",
                new AuditStreamLoader.LoadStats(20L, 19L, 1L, null)));
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        offerRows(mgr, 20);
        for (int i = 0; i < 2; i++) {
            Assertions.assertEquals(RETRY, mgr.flush());
            Assertions.assertEquals(20, mgr.bufferedRows());
            Assertions.assertEquals(0, mgr.filteredEvents());
            Assertions.assertEquals(0, mgr.droppedEvents());
        }
        Assertions.assertEquals(DISCARDED, mgr.flush());
        Assertions.assertEquals(3, calls.get());
        Assertions.assertEquals(0, mgr.bufferedRows());
        Assertions.assertEquals(20, mgr.droppedEvents(), "exhausted retries discard the whole batch exactly once");
        Assertions.assertEquals(0, mgr.filteredEvents());
        Assertions.assertEquals(0, mgr.batchesWithUnknownStatistics());
    }

    @Test
    public void testWarehouseColumnKeeps256Utf8Bytes() throws Exception {
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        Assertions.assertTrue(mgr.buildCreateTableSql().contains("`warehouse` VARCHAR(256)"));
        AuditEvent event = baseEvent();
        for (int length : new int[] {40, 64, 128, 256}) {
            event.warehouse = "w".repeat(length);
            String written = JsonParser.parseString(mgr.formatRowJson(event)).getAsJsonObject()
                    .get("warehouse").getAsString();
            Assertions.assertEquals(event.warehouse, written, "warehouse length=" + length);
        }
        event.warehouse = "w".repeat(257);
        Assertions.assertEquals("w".repeat(256), JsonParser.parseString(mgr.formatRowJson(event)).getAsJsonObject()
                .get("warehouse").getAsString());

        String multibyte = "中".repeat(84) + "🚀";
        Assertions.assertEquals(256, multibyte.getBytes(StandardCharsets.UTF_8).length);
        event.warehouse = multibyte + "tail";
        Assertions.assertEquals(multibyte, JsonParser.parseString(mgr.formatRowJson(event)).getAsJsonObject()
                .get("warehouse").getAsString());
        event.warehouse = "w".repeat(255) + "🚀";
        Assertions.assertEquals("w".repeat(255), JsonParser.parseString(mgr.formatRowJson(event)).getAsJsonObject()
                .get("warehouse").getAsString(), "truncation must not split a four-byte character");
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
        // Admission must reject a row larger than the effective cap even into an empty buffer.
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
    public void testRetryBackoffOverridesWatermarkAndFlushInterval() {
        AtomicLong clock = new AtomicLong();
        AuditLoaderMgr mgr = new AuditLoaderMgr(clock::get);
        AtomicInteger calls = mockLoadResults(new AuditStreamLoader.Response(500, "rejected"));
        long origCap = Config.audit_loader_batch_max_bytes;
        long origInterval = Config.audit_loader_load_interval_seconds;
        try {
            Config.audit_loader_batch_max_bytes = AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES;
            Config.audit_loader_load_interval_seconds = 3600;
            mgr.offerEvent(eventWithStmtBytes((int) AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES / 2));
            mgr.maybeFlush();
            Assertions.assertEquals(1, calls.get());
            Assertions.assertEquals(TimeUnit.SECONDS.toNanos(5), mgr.nextFlushDelayNanos());
            Config.audit_loader_load_interval_seconds = 0;
            mgr.maybeFlush();
            clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(4999));
            mgr.maybeFlush();
            Assertions.assertEquals(1, calls.get(), "neither watermark nor interval=0 can bypass backoff");

            // Drop below the watermark without changing the failing head. It still retries at 5s.
            Config.audit_loader_batch_max_bytes *= 2;
            Config.audit_loader_load_interval_seconds = 3600;
            clock.incrementAndGet();
            Assertions.assertFalse(mgr.shouldFlushNow());
            clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(1) - 1);
            mgr.maybeFlush();
            Assertions.assertEquals(2, calls.get(), "a failed small batch must not wait another interval");
            clock.addAndGet(TimeUnit.SECONDS.toNanos(5));
            mgr.maybeFlush();
            Assertions.assertEquals(3, calls.get());
            Assertions.assertEquals(0, mgr.bufferedRows());
            Assertions.assertEquals(1, mgr.droppedEvents());
            Assertions.assertEquals(0, mgr.consecutiveFlushFailures());

            Config.audit_loader_batch_max_bytes = AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES;
            mgr.offerEvent(eventWithStmtBytes((int) AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES / 2));
            mgr.maybeFlush();
            Assertions.assertEquals(3, calls.get(), "discarding a batch must also retain the cooldown");
            clock.addAndGet(TimeUnit.SECONDS.toNanos(5));
            mgr.maybeFlush();
            Assertions.assertEquals(4, calls.get());
            Assertions.assertEquals(1, mgr.consecutiveFlushFailures(), "the next batch gets a fresh budget");
        } finally {
            Config.audit_loader_batch_max_bytes = origCap;
            Config.audit_loader_load_interval_seconds = origInterval;
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
    public void testCorrectReplicationNumWithoutTableDoesNotExecuteDdl() {
        AtomicInteger calls = new AtomicInteger();
        new MockUp<SimpleExecutor>() {
            @Mock
            public void executeDDL(String sql) {
                calls.incrementAndGet();
            }
        };
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        mgr.correctReplicationNum();
        Assertions.assertEquals(0, calls.get(), "maintenance must not issue ALTER statements for a missing table");
    }

    // Execute real FE ALTER statements against the expression-partitioned audit table. Only the
    // requested one-off failure is injected, so assertions check metadata as well as DDL selection.
    private static class ReplicationMaintenanceFixture implements AutoCloseable {
        private final AuditLoaderMgr mgr = new AuditLoaderMgr();
        private final StarRocksAssert starRocksAssert = new StarRocksAssert(UtFrameUtils.createDefaultCtx());
        private final List<String> statements = new ArrayList<>();
        private final OlapTable table;
        private final int expected = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                .getSystemTableExpectedReplicationNum();
        private boolean failPartitionsOnce;
        private boolean failDefaultOnce;

        ReplicationMaintenanceFixture(boolean partitionsMatch, boolean defaultMatches) throws Exception {
            starRocksAssert.withDatabase(AuditLoaderMgr.AUDIT_DB_NAME);
            starRocksAssert.withTable(mgr.buildCreateTableSql());
            table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(AuditLoaderMgr.AUDIT_DB_NAME, AuditLoaderMgr.AUDIT_TABLE_NAME);
            Assertions.assertFalse(table.getPartitions().isEmpty(), "the shadow partition must be present");
            // Seed divergent metadata without requiring extra BEs. Each real ALTER converges to the
            // cluster's supported value, so the ordinary FE validation remains enabled.
            for (Partition partition : table.getPartitions()) {
                table.getPartitionInfo().setReplicationNum(partition.getId(),
                        (short) (partitionsMatch ? expected : expected + 1));
            }
            table.setReplicationNum((short) (defaultMatches ? expected : expected + 1));
            new MockUp<SimpleExecutor>() {
                @Mock
                public void executeDDL(Invocation invocation, String sql) {
                    if (sql.startsWith("ALTER TABLE `" + AuditLoaderMgr.AUDIT_DB_NAME + "`.`"
                            + AuditLoaderMgr.AUDIT_TABLE_NAME + "`")) {
                        statements.add(sql);
                        if (sql.equals(partitionsDdl()) && failPartitionsOnce) {
                            failPartitionsOnce = false;
                            throw new IllegalStateException("injected partition DDL failure");
                        }
                        if (sql.equals(defaultDdl()) && failDefaultOnce) {
                            failDefaultOnce = false;
                            throw new IllegalStateException("injected default DDL failure");
                        }
                    }
                    invocation.proceed(sql);
                }
            };
        }

        String partitionsDdl() {
            return String.format("ALTER TABLE `%s`.`%s` MODIFY PARTITION(*) SET ('replication_num'='%d')",
                    AuditLoaderMgr.AUDIT_DB_NAME, AuditLoaderMgr.AUDIT_TABLE_NAME, expected);
        }

        String defaultDdl() {
            return String.format("ALTER TABLE `%s`.`%s` SET ('default.replication_num'='%d')",
                    AuditLoaderMgr.AUDIT_DB_NAME, AuditLoaderMgr.AUDIT_TABLE_NAME, expected);
        }

        void assertConverged() {
            for (Partition partition : table.getPartitions()) {
                Assertions.assertEquals(expected, table.getPartitionInfo().getReplicationNum(partition.getId()));
            }
            Assertions.assertEquals(expected, table.getDefaultReplicationNum().intValue());
        }

        @Override
        public void close() throws Exception {
            starRocksAssert.dropTable(AuditLoaderMgr.AUDIT_DB_NAME + "." + AuditLoaderMgr.AUDIT_TABLE_NAME);
        }
    }

    @Test
    public void testReplicationMaintenanceRetriesDefaultAfterPartialSuccess() throws Exception {
        try (ReplicationMaintenanceFixture fixture = new ReplicationMaintenanceFixture(false, false);
                LoadLogCollector logs = new LoadLogCollector()) {
            fixture.failDefaultOnce = true;
            fixture.mgr.correctReplicationNum();
            Assertions.assertEquals(fixture.expected, fixture.table.getPartitionInfo().getMinReplicationNum());
            Assertions.assertEquals(fixture.expected + 1, fixture.table.getDefaultReplicationNum().intValue());
            fixture.mgr.correctReplicationNum();
            fixture.assertConverged();
            Assertions.assertEquals(List.of(fixture.partitionsDdl(), fixture.defaultDdl(), fixture.defaultDdl()),
                    fixture.statements, "the next cycle must retry only the unfinished default change");
            Assertions.assertTrue(logs.messages(Level.WARN, "failed to change replication_num of audit table")
                    .stream().anyMatch(message -> message.contains("stage=default")));
            fixture.mgr.correctReplicationNum();
            Assertions.assertEquals(3, fixture.statements.size(), "converged metadata needs no further ALTER");
        }
    }

    @Test
    public void testReplicationMaintenanceRetriesFailedPartitionChange() throws Exception {
        try (ReplicationMaintenanceFixture fixture = new ReplicationMaintenanceFixture(false, false);
                LoadLogCollector logs = new LoadLogCollector()) {
            fixture.failPartitionsOnce = true;
            fixture.mgr.correctReplicationNum();
            Assertions.assertEquals(fixture.expected + 1, fixture.table.getPartitionInfo().getMinReplicationNum());
            Assertions.assertEquals(fixture.expected + 1, fixture.table.getDefaultReplicationNum().intValue());
            fixture.mgr.correctReplicationNum();
            fixture.assertConverged();
            Assertions.assertEquals(List.of(fixture.partitionsDdl(), fixture.partitionsDdl(), fixture.defaultDdl()),
                    fixture.statements);
            Assertions.assertTrue(logs.messages(Level.WARN, "failed to change replication_num of audit table")
                    .stream().anyMatch(message -> message.contains("stage=partitions")));
        }
    }

    @Test
    public void testReplicationMaintenanceUpdatesOnlyDefault() throws Exception {
        try (ReplicationMaintenanceFixture fixture = new ReplicationMaintenanceFixture(true, false)) {
            fixture.mgr.correctReplicationNum();
            fixture.assertConverged();
            Assertions.assertEquals(List.of(fixture.defaultDdl()), fixture.statements);
        }
    }

    @Test
    public void testReplicationMaintenanceUpdatesOnlyPartitions() throws Exception {
        try (ReplicationMaintenanceFixture fixture = new ReplicationMaintenanceFixture(false, true)) {
            fixture.mgr.correctReplicationNum();
            fixture.assertConverged();
            Assertions.assertEquals(List.of(fixture.partitionsDdl()), fixture.statements);
        }
    }

    @Test
    public void testReplicationMaintenanceDoesNothingOnceBothMatch() throws Exception {
        try (ReplicationMaintenanceFixture fixture = new ReplicationMaintenanceFixture(true, true)) {
            fixture.mgr.correctReplicationNum();
            fixture.mgr.correctReplicationNum();
            fixture.assertConverged();
            Assertions.assertTrue(fixture.statements.isEmpty());
        }
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
        AtomicInteger calls = mockLoadResults(loadResult(0, 0, 0));
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        long origInterval = Config.audit_loader_load_interval_seconds;
        try {
            Config.audit_loader_load_interval_seconds = 0;
            mgr.maybeFlush();
            Assertions.assertEquals(EMPTY, mgr.flush());
            Assertions.assertEquals(0, calls.get(), "an empty queue must not create a load transaction");
            Assertions.assertEquals(0, mgr.bufferedRows());
        } finally {
            Config.audit_loader_load_interval_seconds = origInterval;
        }
    }

    @Test
    public void testFlushStopsAfterOneBatchAtTheEffectiveByteCap() {
        List<String> payloads = new ArrayList<>();
        new MockUp<AuditStreamLoader>() {
            @Mock
            public AuditStreamLoader.Response loadBatch(String label, String payload) {
                payloads.add(payload);
                return loadResult(1, 1, 0);
            }
        };
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        long origCap = Config.audit_loader_batch_max_bytes;
        try {
            Config.audit_loader_batch_max_bytes = 3 * AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES;
            AuditEvent first = eventWithStmtBytes((int) AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES * 3 / 5);
            AuditEvent second = eventWithStmtBytes((int) AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES * 3 / 5);
            second.queryId = "qid-2";
            mgr.offerEvent(first);
            mgr.offerEvent(second);
            long remainingBytes = mgr.formatRowJson(second).getBytes(StandardCharsets.UTF_8).length;
            Config.audit_loader_batch_max_bytes = AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES;

            Assertions.assertEquals(SUCCESS, mgr.flush());
            Assertions.assertEquals(1, payloads.size());
            Assertions.assertTrue(("[" + mgr.formatRowJson(first) + "]").equals(payloads.get(0)),
                    "the first payload must contain exactly the first row");
            Assertions.assertEquals(1, mgr.bufferedRows(), "one call must leave the second batch queued");
            Assertions.assertEquals(remainingBytes, mgr.bufferedBytes());
            Assertions.assertEquals(0, mgr.droppedEvents());
            Assertions.assertEquals(SUCCESS, mgr.flush());
            Assertions.assertEquals(2, payloads.size());
            Assertions.assertTrue(("[" + mgr.formatRowJson(second) + "]").equals(payloads.get(1)),
                    "the second payload must contain exactly the remaining row");
            Assertions.assertEquals(0, mgr.bufferedRows());
            Assertions.assertEquals(0, mgr.bufferedBytes());
        } finally {
            Config.audit_loader_batch_max_bytes = origCap;
        }
    }

    @Test
    public void testSingleRowRequestIncludesBracketsAtAdmissionBoundary() {
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        int cap = (int) AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES;
        List<String> payloads = new ArrayList<>();
        new MockUp<AuditStreamLoader>() {
            @Mock
            public AuditStreamLoader.Response loadBatch(String label, String payload) {
                payloads.add(payload);
                return loadResult(1, 1, 0);
            }
        };
        long origCap = Config.audit_loader_batch_max_bytes;
        try (LoadLogCollector logs = new LoadLogCollector()) {
            Config.audit_loader_batch_max_bytes = cap;
            AuditEvent exact = eventWithRowBytes(cap - 2);
            mgr.offerEvent(exact);
            Assertions.assertEquals(cap - 2, mgr.bufferedBytes());
            Assertions.assertEquals(SUCCESS, mgr.flush());
            Assertions.assertEquals(cap, payloads.get(0).getBytes(StandardCharsets.UTF_8).length);
            Assertions.assertTrue(exact.stmt.equals(JsonParser.parseString(payloads.get(0)).getAsJsonArray()
                    .get(0).getAsJsonObject().get("stmt").getAsString()), "the SQL must survive JSON encoding");
            Assertions.assertEquals(0, mgr.bufferedBytes(), "brackets must not be subtracted from row bytes");

            AuditEvent tooLarge = eventWithRowBytes(cap - 1);
            mgr.offerEvent(tooLarge);
            mgr.offerEvent(tooLarge);
            Assertions.assertEquals(0, mgr.bufferedRows());
            Assertions.assertEquals(0, mgr.bufferedBytes());
            Assertions.assertEquals(2, mgr.droppedEvents());
            Assertions.assertEquals(0, mgr.consecutiveFlushFailures());
            Assertions.assertEquals(EMPTY, mgr.flush());
            Assertions.assertEquals(1, payloads.size(), "local rejections must not make HTTP requests");
            List<String> warnings = logs.messages(Level.WARN, "audit loader dropped oversized event:");
            Assertions.assertEquals(1, warnings.size(), "size warnings must be throttled independently of counting");
            Assertions.assertTrue(warnings.get(0).contains("row_bytes=" + (cap - 1)));
            Assertions.assertTrue(warnings.get(0).contains("request_bytes=" + (cap + 1)));
            Assertions.assertTrue(warnings.get(0).contains("batch_limit_bytes=" + cap));
            Assertions.assertTrue(warnings.get(0).contains("dropped_total=1"));
            Assertions.assertTrue(warnings.get(0).length() < 250, "size warnings must not include SQL text");
        } finally {
            Config.audit_loader_batch_max_bytes = origCap;
        }
    }

    @Test
    public void testCompleteRequestBudgetIncludesCommaAndBrackets() {
        int cap = 2 * (int) AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES;
        List<String> payloads = new ArrayList<>();
        new MockUp<AuditStreamLoader>() {
            @Mock
            public AuditStreamLoader.Response loadBatch(String label, String payload) {
                payloads.add(payload);
                int rows = JsonParser.parseString(payload).getAsJsonArray().size();
                return loadResult(rows, rows, 0);
            }
        };
        long origCap = Config.audit_loader_batch_max_bytes;
        try {
            Config.audit_loader_batch_max_bytes = cap;
            // Even when all row bytes fit the queue, the request may exceed its budget by 1..3 bytes.
            for (int excess = 0; excess <= 3; excess++) {
                payloads.clear();
                AuditLoaderMgr mgr = new AuditLoaderMgr();
                AuditEvent first = eventWithRowBytes(cap / 2);
                AuditEvent second = eventWithRowBytes(cap - cap / 2 - 3 + excess);
                second.queryId = "qid-2";
                String firstJson = mgr.formatRowJson(first);
                String secondJson = mgr.formatRowJson(second);
                long secondBytes = secondJson.getBytes(StandardCharsets.UTF_8).length;
                mgr.offerEvent(first);
                mgr.offerEvent(second);
                Assertions.assertEquals(cap - 3 + excess, mgr.bufferedBytes());
                Assertions.assertEquals(SUCCESS, mgr.flush());
                if (excess == 0) {
                    Assertions.assertEquals(cap, payloads.get(0).getBytes(StandardCharsets.UTF_8).length);
                    Assertions.assertTrue(("[" + firstJson + "," + secondJson + "]").equals(payloads.get(0)));
                } else {
                    Assertions.assertTrue(("[" + firstJson + "]").equals(payloads.get(0)));
                    Assertions.assertEquals(1, mgr.bufferedRows());
                    Assertions.assertEquals(secondBytes, mgr.bufferedBytes());
                    Assertions.assertEquals(SUCCESS, mgr.flush());
                    Assertions.assertTrue(("[" + secondJson + "]").equals(payloads.get(1)));
                }
                for (String payload : payloads) {
                    Assertions.assertTrue(payload.getBytes(StandardCharsets.UTF_8).length <= cap);
                }
                Assertions.assertEquals(0, mgr.bufferedRows());
                Assertions.assertEquals(0, mgr.bufferedBytes());
                Assertions.assertEquals(0, mgr.droppedEvents());
            }
        } finally {
            Config.audit_loader_batch_max_bytes = origCap;
        }
    }

    @Test
    public void testCapacityReductionDropsOnlyOneOversizedHeadPerCycle() {
        ReadyAuditLoader mgr = new ReadyAuditLoader();
        int cap = (int) AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES;
        AtomicInteger calls = mockLoadResults(loadResult(1, 1, 0));
        long origCap = Config.audit_loader_batch_max_bytes;
        long origInterval = Config.audit_loader_load_interval_seconds;
        boolean origEnabled = Config.enable_audit_loader;
        try (LoadLogCollector logs = new LoadLogCollector()) {
            Config.enable_audit_loader = true;
            Config.audit_loader_load_interval_seconds = 0;
            Config.audit_loader_batch_max_bytes = 3L * cap;
            mgr.offerEvent(eventWithRowBytes(cap - 1));
            mgr.offerEvent(eventWithRowBytes(cap + 10));
            mgr.offerEvent(baseEvent());
            long tailBytes = mgr.formatRowJson(baseEvent()).getBytes(StandardCharsets.UTF_8).length;
            Config.audit_loader_batch_max_bytes = cap;

            mgr.runAfterCatalogReady();
            Assertions.assertEquals(1, mgr.controlChecks.get());
            Assertions.assertEquals(0, calls.get());
            Assertions.assertEquals(2, mgr.bufferedRows());
            Assertions.assertEquals(cap + 10 + tailBytes, mgr.bufferedBytes());
            Assertions.assertEquals(1, mgr.droppedEvents());
            Assertions.assertEquals(0, mgr.consecutiveFlushFailures());
            Assertions.assertTrue(mgr.shouldFlushNow(), "the next oversized head must not wait for the interval");

            mgr.runAfterCatalogReady();
            Assertions.assertEquals(2, mgr.controlChecks.get());
            Assertions.assertEquals(0, calls.get());
            Assertions.assertEquals(1, mgr.bufferedRows());
            Assertions.assertEquals(tailBytes, mgr.bufferedBytes());
            Assertions.assertEquals(2, mgr.droppedEvents());
            mgr.runAfterCatalogReady();
            Assertions.assertEquals(3, mgr.controlChecks.get());
            Assertions.assertEquals(1, calls.get());
            Assertions.assertEquals(0, mgr.bufferedRows());
            Assertions.assertEquals(0, mgr.bufferedBytes());
            Assertions.assertEquals(2, mgr.droppedEvents());
            Assertions.assertEquals(1, logs.messages(Level.WARN, "audit loader dropped oversized event:").size());
            Assertions.assertEquals(0, logs.messages(Level.ERROR, "audit loader discarded").size());
        } finally {
            Config.audit_loader_batch_max_bytes = origCap;
            Config.audit_loader_load_interval_seconds = origInterval;
            Config.enable_audit_loader = origEnabled;
        }
    }

    @Test
    public void testCapacityReductionKeepsTheAlreadySelectedRetryBatch() {
        AtomicLong clock = new AtomicLong();
        AuditLoaderMgr mgr = new AuditLoaderMgr(clock::get);
        int cap = (int) AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES;
        AtomicInteger calls = mockLoadResults(new AuditStreamLoader.Response(500, "rejected"));
        long origCap = Config.audit_loader_batch_max_bytes;
        long origInterval = Config.audit_loader_load_interval_seconds;
        try {
            Config.audit_loader_load_interval_seconds = 0;
            Config.audit_loader_batch_max_bytes = 4L * cap;
            mgr.offerEvent(eventWithRowBytes(cap - 1));
            mgr.offerEvent(eventWithRowBytes(cap / 2));
            mgr.maybeFlush();
            Config.audit_loader_batch_max_bytes = cap;
            clock.addAndGet(TimeUnit.SECONDS.toNanos(4));
            mgr.maybeFlush();
            Assertions.assertEquals(1, calls.get());
            clock.addAndGet(TimeUnit.SECONDS.toNanos(1));
            mgr.maybeFlush();
            Assertions.assertEquals(2, calls.get());
            Assertions.assertEquals(2, mgr.bufferedRows());
            Assertions.assertEquals(0, mgr.droppedEvents());
            clock.addAndGet(TimeUnit.SECONDS.toNanos(5));
            mgr.maybeFlush();
            Assertions.assertEquals(3, calls.get());
            Assertions.assertEquals(0, mgr.bufferedRows());
            Assertions.assertEquals(0, mgr.bufferedBytes());
            Assertions.assertEquals(2, mgr.droppedEvents());
            Assertions.assertEquals(0, mgr.consecutiveFlushFailures());
        } finally {
            Config.audit_loader_batch_max_bytes = origCap;
            Config.audit_loader_load_interval_seconds = origInterval;
        }
    }

    @Test
    public void testRetryKeepsRowsAndLabelWhileNewEventsAndCapacityChangesWait() {
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        List<String> labels = new ArrayList<>();
        List<String> payloads = new ArrayList<>();
        new MockUp<AuditStreamLoader>() {
            @Mock
            public AuditStreamLoader.Response loadBatch(String label, String payload) {
                labels.add(label);
                payloads.add(payload);
                return labels.size() == 1 ? new AuditStreamLoader.Response(500, "failed") : loadResult(1, 1, 0);
            }
        };
        long cap = Config.audit_loader_batch_max_bytes;
        try {
            AuditEvent first = baseEvent();
            first.queryId = "first";
            mgr.offerEvent(first);
            Assertions.assertEquals(RETRY, mgr.flush());
            AuditEvent later = baseEvent();
            later.queryId = "later";
            mgr.offerEvent(later);
            Config.audit_loader_batch_max_bytes = AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES;
            Assertions.assertEquals(SUCCESS, mgr.flush());
            Assertions.assertEquals(labels.get(0), labels.get(1));
            Assertions.assertEquals(payloads.get(0), payloads.get(1));
            Assertions.assertEquals(1, mgr.bufferedRows());
            Assertions.assertEquals(mgr.formatRowJson(later).getBytes(StandardCharsets.UTF_8).length, mgr.bufferedBytes());
            Assertions.assertEquals(SUCCESS, mgr.flush());
            Assertions.assertNotEquals(labels.get(1), labels.get(2));
            Assertions.assertTrue(payloads.get(2).contains("\"queryId\":\"later\""));
            Assertions.assertFalse(payloads.get(2).contains("\"queryId\":\"first\""));
            Assertions.assertEquals(0, mgr.bufferedBytes());
        } finally {
            Config.audit_loader_batch_max_bytes = cap;
        }
    }

    @Test
    public void testExhaustedSnapshotDoesNotDiscardNewArrivalsOrTheirAttemptBudget() {
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        List<String> labels = new ArrayList<>();
        List<String> payloads = new ArrayList<>();
        new MockUp<AuditStreamLoader>() {
            @Mock
            public AuditStreamLoader.Response loadBatch(String label, String payload) {
                labels.add(label);
                payloads.add(payload);
                return new AuditStreamLoader.Response(500, "failed");
            }
        };
        mgr.offerEvent(baseEvent());
        Assertions.assertEquals(RETRY, mgr.flush());
        AuditEvent later = baseEvent();
        later.queryId = "later";
        mgr.offerEvent(later);
        Assertions.assertEquals(RETRY, mgr.flush());
        Assertions.assertEquals(DISCARDED, mgr.flush());
        Assertions.assertEquals(1, mgr.bufferedRows());
        Assertions.assertEquals(1, mgr.droppedEvents());
        Assertions.assertEquals(0, mgr.consecutiveFlushFailures());
        Assertions.assertEquals(1, labels.stream().distinct().count());
        Assertions.assertEquals(1, payloads.stream().distinct().count());
        Assertions.assertEquals(RETRY, mgr.flush());
        Assertions.assertEquals(RETRY, mgr.flush());
        Assertions.assertEquals(DISCARDED, mgr.flush());
        Assertions.assertEquals(2, labels.stream().distinct().count());
        Assertions.assertEquals(2, mgr.droppedEvents());
        Assertions.assertEquals(0, mgr.bufferedBytes());
    }

    @Test
    public void testClearingTheBufferReleasesThePendingSnapshot() {
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        List<String> labels = new ArrayList<>();
        List<String> payloads = new ArrayList<>();
        new MockUp<AuditStreamLoader>() {
            @Mock
            public AuditStreamLoader.Response loadBatch(String label, String payload) {
                labels.add(label);
                payloads.add(payload);
                return new AuditStreamLoader.Response(500, "failed");
            }
        };
        mgr.offerEvent(baseEvent());
        Assertions.assertEquals(RETRY, mgr.flush());
        mgr.clearBuffer();
        Assertions.assertEquals(EMPTY, mgr.flush());
        AuditEvent later = baseEvent();
        later.queryId = "later";
        mgr.offerEvent(later);
        Assertions.assertEquals(RETRY, mgr.flush());
        Assertions.assertNotEquals(labels.get(0), labels.get(1));
        Assertions.assertTrue(payloads.get(1).contains("\"queryId\":\"later\""));
        Assertions.assertEquals(1, mgr.consecutiveFlushFailures());
    }

    @Test
    public void testRunningLabelDoesNotConfirmCommitAndExhaustionReportsUncertainty() {
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        new MockUp<AuditStreamLoader>() {
            @Mock
            public AuditStreamLoader.Response loadBatch(String label, String payload) {
                return AuditStreamLoader.parseResponse(200, "{\"Status\":\"Label Already Exists\","
                        + "\"ExistingJobStatus\":\"RUNNING\",\"Label\":\"" + label + "\"}", label);
            }
        };
        mgr.offerEvent(baseEvent());
        try (LoadLogCollector logs = new LoadLogCollector()) {
            Assertions.assertEquals(RETRY, mgr.flush());
            Assertions.assertEquals(RETRY, mgr.flush());
            Assertions.assertEquals(1, mgr.bufferedRows());
            Assertions.assertEquals(0, mgr.batchesAlreadyCommitted());
            Assertions.assertEquals(DISCARDED, mgr.flush());
            Assertions.assertTrue(logs.messages(Level.ERROR, "audit loader discarded").get(0)
                    .contains("commit_result_unknown=true"));
            Assertions.assertEquals(1, mgr.droppedEvents());
        }
    }

    @Test
    public void testPreviouslyCommittedBatchHasItsOwnCumulativeStatus() {
        String body = "{\"Status\":\"Label Already Exists\",\"Label\":\"ours\",\"ExistingJobStatus\":\"FINISHED\"}";
        mockLoadResults(AuditStreamLoader.parseResponse(200, body, "ours"));
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        try (LoadLogCollector logs = new LoadLogCollector()) {
            offerRows(mgr, 20);
            Assertions.assertEquals(SUCCESS, mgr.flush());
            Assertions.assertEquals(1, mgr.batchesAlreadyCommitted());
            Assertions.assertEquals(1, mgr.batchesWithUnknownStatistics());
            Assertions.assertEquals(0, mgr.batchesWithPublishTimeout());
            Assertions.assertEquals(0, mgr.filteredEvents());
            Assertions.assertEquals(0, mgr.droppedEvents());
            Assertions.assertEquals(1, logs.messages(Level.WARN, "audit loader load previously committed:").size());
            Assertions.assertTrue(mgr.reportStatus());
            Assertions.assertTrue(logs.messages(Level.WARN, "audit loader status:").get(0)
                    .contains("1 loads confirmed previously committed since the last status line (1 total)"));
            Assertions.assertFalse(mgr.reportStatus());
        }
    }

    @Test
    public void testTimestampExtractionSkipsLargeSqlWithoutAnObjectTree() throws Exception {
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        Method extract = AuditLoaderMgr.class.getDeclaredMethod("eventTimestamp", String.class);
        extract.setAccessible(true);
        String small = mgr.formatRowJson(baseEvent());
        String expected = (String) extract.invoke(null, small);
        AuditEvent large = baseEvent();
        large.stmt = "\0".repeat(1048576);
        String row = mgr.formatRowJson(large);
        for (int i = 0; i < 10; i++) {
            extract.invoke(null, small);
        }
        java.lang.management.ThreadMXBean platformBean = ManagementFactory.getThreadMXBean();
        Assumptions.assumeTrue(platformBean instanceof ThreadMXBean);
        ThreadMXBean bean = (ThreadMXBean) platformBean;
        Assumptions.assumeTrue(bean.isThreadAllocatedMemorySupported() && bean.isThreadAllocatedMemoryEnabled());
        long id = Thread.currentThread().getId();
        long before = bean.getThreadAllocatedBytes(id);
        Assertions.assertEquals(expected, extract.invoke(null, row));
        long allocated = bean.getThreadAllocatedBytes(id) - before;
        Assertions.assertTrue(allocated < 65536, "timestamp extraction allocated " + allocated + " bytes");
        Assertions.assertEquals("unknown", extract.invoke(null, "{}"));
        Assertions.assertEquals("unknown", extract.invoke(null, "not-json"));
        Assertions.assertEquals("unknown", extract.invoke(null, "{\"timestamp\":null}"));
        Assertions.assertEquals(expected, extract.invoke(null, "{\"stmt\":\"\\\"\\n中文\",\"timestamp\":\"" + expected + "\"}"));
    }

    @Test
    public void testSingleRowDiscardReadsTheTimestampOnlyOnce() {
        mockLoadResults(new AuditStreamLoader.Response(500, "failed"));
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        mgr.offerEvent(baseEvent());
        AtomicInteger reads = new AtomicInteger();
        Thread owner = Thread.currentThread();
        new MockUp<JsonReader>() {
            @Mock
            public String nextString(Invocation invocation) throws IOException {
                if (Thread.currentThread() == owner) {
                    reads.incrementAndGet();
                }
                return invocation.proceed();
            }
        };
        Assertions.assertEquals(RETRY, mgr.flush());
        Assertions.assertEquals(RETRY, mgr.flush());
        Assertions.assertEquals(DISCARDED, mgr.flush());
        Assertions.assertEquals(1, reads.get());
    }

    @Test
    public void testCapacityChangeDuringRequestAppliesToNextBatch() {
        int cap = (int) AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES;
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        List<Integer> requestBytes = new ArrayList<>();
        new MockUp<AuditStreamLoader>() {
            @Mock
            public AuditStreamLoader.Response loadBatch(String label, String payload) {
                requestBytes.add(payload.getBytes(StandardCharsets.UTF_8).length);
                Config.audit_loader_batch_max_bytes = cap;
                return loadResult(1, 1, 0);
            }
        };
        long origCap = Config.audit_loader_batch_max_bytes;
        try {
            Config.audit_loader_batch_max_bytes = 2L * cap;
            mgr.offerEvent(eventWithRowBytes(2 * cap - 2));
            Assertions.assertEquals(SUCCESS, mgr.flush());
            Assertions.assertEquals(2 * cap, requestBytes.get(0));
            Assertions.assertEquals(0, mgr.bufferedBytes());
            mgr.offerEvent(eventWithRowBytes(cap - 2));
            Assertions.assertEquals(SUCCESS, mgr.flush());
            Assertions.assertEquals(cap, requestBytes.get(1));
            Assertions.assertEquals(0, mgr.bufferedBytes());
            Assertions.assertEquals(0, mgr.droppedEvents());
        } finally {
            Config.audit_loader_batch_max_bytes = origCap;
        }
    }

    @Test
    public void testRunCycleLoadsIntoAnExistingAuditTable() throws Exception {
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        AtomicInteger calls = mockLoadResults(loadResult(1, 1, 0));
        StarRocksAssert starRocksAssert = new StarRocksAssert(UtFrameUtils.createDefaultCtx());
        starRocksAssert.withDatabase(AuditLoaderMgr.AUDIT_DB_NAME);
        starRocksAssert.withTable(mgr.buildCreateTableSql());
        boolean origEnabled = Config.enable_audit_loader;
        long origInterval = Config.audit_loader_load_interval_seconds;
        try {
            Config.enable_audit_loader = true;
            Config.audit_loader_load_interval_seconds = 0;
            mgr.offerEvent(baseEvent());
            mgr.runAfterCatalogReady();
            Assertions.assertEquals(1, calls.get(), "an existing table must allow the cycle to reach the loader");
            Assertions.assertEquals(0, mgr.bufferedRows());
            Assertions.assertEquals(0, mgr.bufferedBytes());
            Assertions.assertEquals(0, mgr.droppedEvents());
        } finally {
            Config.enable_audit_loader = origEnabled;
            Config.audit_loader_load_interval_seconds = origInterval;
            mgr.clearBuffer();
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
        AtomicInteger calls = mockLoadResults(new AuditStreamLoader.Response(500, "rejected"));
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        try (LoadLogCollector logs = new LoadLogCollector()) {
            // Each batch gets one initial attempt and two retries, including after a discard.
            for (int batch = 0; batch < 2; batch++) {
                mgr.offerEvent(baseEvent());
                long bytesBefore = mgr.bufferedBytes();
                for (int attempt = 1; attempt <= 2; attempt++) {
                    Assertions.assertEquals(RETRY, mgr.flush());
                    Assertions.assertEquals(batch * 3 + attempt, calls.get());
                    Assertions.assertEquals(1, mgr.bufferedRows());
                    Assertions.assertEquals(bytesBefore, mgr.bufferedBytes());
                    Assertions.assertEquals(attempt, mgr.consecutiveFlushFailures());
                    Assertions.assertEquals(batch, mgr.droppedEvents());
                }
                Assertions.assertEquals(DISCARDED, mgr.flush());
                Assertions.assertEquals((batch + 1) * 3, calls.get());
                Assertions.assertEquals(0, mgr.bufferedRows());
                Assertions.assertEquals(0, mgr.bufferedBytes());
                Assertions.assertEquals(0, mgr.consecutiveFlushFailures());
                Assertions.assertEquals(batch + 1, mgr.droppedEvents());

                Assertions.assertEquals(EMPTY, mgr.flush());
                Assertions.assertEquals((batch + 1) * 3, calls.get(), "an empty queue must not submit again");
                Assertions.assertEquals(batch + 1, mgr.droppedEvents(), "each discarded row is counted once");
            }
            List<String> errors = logs.messages(Level.ERROR, "audit loader discarded ");
            Assertions.assertEquals(2, errors.size());
            Assertions.assertTrue(errors.stream().allMatch(message -> message.endsWith("after 3 failed attempts")));
        }
    }

    @Test
    public void testSuccessOnLastAttemptResetsFailureBudget() {
        AuditStreamLoader.Response rejected = new AuditStreamLoader.Response(500, "rejected");
        AtomicInteger calls = mockLoadResults(rejected, rejected, loadResult(1, 1, 0), rejected);
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        mgr.offerEvent(baseEvent());
        long bytesBefore = mgr.bufferedBytes();
        for (int attempt = 1; attempt <= 2; attempt++) {
            Assertions.assertEquals(RETRY, mgr.flush());
            Assertions.assertEquals(attempt, mgr.consecutiveFlushFailures());
            Assertions.assertEquals(1, mgr.bufferedRows());
            Assertions.assertEquals(bytesBefore, mgr.bufferedBytes());
            Assertions.assertEquals(0, mgr.droppedEvents());
        }
        Assertions.assertEquals(SUCCESS, mgr.flush(), "the final allowed attempt may still succeed");
        Assertions.assertEquals(3, calls.get());
        Assertions.assertEquals(0, mgr.consecutiveFlushFailures());
        Assertions.assertEquals(0, mgr.bufferedRows());
        Assertions.assertEquals(0, mgr.bufferedBytes());
        Assertions.assertEquals(0, mgr.droppedEvents());

        mgr.offerEvent(baseEvent());
        for (int attempt = 1; attempt <= 2; attempt++) {
            Assertions.assertEquals(RETRY, mgr.flush());
            Assertions.assertEquals(attempt, mgr.consecutiveFlushFailures());
            Assertions.assertEquals(1, mgr.bufferedRows());
            Assertions.assertEquals(bytesBefore, mgr.bufferedBytes());
            Assertions.assertEquals(0, mgr.droppedEvents());
        }
        Assertions.assertEquals(DISCARDED, mgr.flush());
        Assertions.assertEquals(6, calls.get());
        Assertions.assertEquals(0, mgr.consecutiveFlushFailures());
        Assertions.assertEquals(0, mgr.bufferedRows());
        Assertions.assertEquals(0, mgr.bufferedBytes());
        Assertions.assertEquals(1, mgr.droppedEvents());
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
    public void testBatchMaxBytesIsClampedToSupportedMinimum() {
        // audit_loader_batch_max_bytes is mutable, so an operator can set it below what a single
        // typical large row needs. The floor preserves room for a full-width ASCII statement;
        // JSON escaping or many queried relations can still produce a row larger than the floor.
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

            // A full-width ASCII statement is still admitted under a tiny configured cap.
            Config.audit_loader_batch_max_bytes = 1024;
            mgr.offerEvent(eventWithStmtBytes(1048576));
            Assertions.assertEquals(1, mgr.bufferedRows(), "a full-width ASCII statement must survive a tiny cap");
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
        // Preserve missing text as JSON null for SQL IS NULL queries. The external plugin uses
        // string concatenation for text fields, so this test does not assert identical null
        // semantics between the plugin and builtin loader. Empty text is a separate value.
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
        // The counterpart to testBatchMaxBytesIsClampedToSupportedMinimum for the other mutable knob.
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
        // Unlike nullable text, the builtin contract represents an absent relation list as an
        // empty array. A blanket "write null when absent" change would break that behavior.
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
    public void testClearResetsBothRetryBudgetAndCooldown() {
        AtomicLong clock = new AtomicLong();
        AuditLoaderMgr mgr = new AuditLoaderMgr(clock::get);
        AtomicInteger calls = mockLoadResults(new AuditStreamLoader.Response(500, "rejected"));
        long origInterval = Config.audit_loader_load_interval_seconds;
        try {
            Config.audit_loader_load_interval_seconds = 0;
            mgr.offerEvent(baseEvent());
            mgr.maybeFlush();
            Assertions.assertEquals(1, calls.get());
            Assertions.assertEquals(1, mgr.consecutiveFlushFailures());
            mgr.clearBuffer();
            Assertions.assertEquals(0, mgr.consecutiveFlushFailures());
            mgr.offerEvent(baseEvent());
            Assertions.assertTrue(mgr.shouldFlushNow(), "clearing must remove the previous failure's cooldown");
            for (int attempt = 1; attempt <= 3; attempt++) {
                mgr.maybeFlush();
                Assertions.assertEquals(attempt + 1, calls.get());
                if (attempt < 3) {
                    Assertions.assertEquals(1, mgr.bufferedRows());
                    Assertions.assertEquals(1, mgr.droppedEvents(), "only the explicitly cleared row was lost");
                }
                clock.addAndGet(TimeUnit.SECONDS.toNanos(5));
            }
            Assertions.assertEquals(0, mgr.bufferedRows());
            Assertions.assertEquals(0, mgr.bufferedBytes());
            Assertions.assertEquals(2, mgr.droppedEvents());
        } finally {
            Config.audit_loader_load_interval_seconds = origInterval;
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
    public void testControlChecksRunBeforeLoadingRowsArrivingDuringARequest() {
        ReadyAuditLoader mgr = new ReadyAuditLoader();
        AtomicInteger calls = new AtomicInteger();
        AuditEvent later = eventWithStmtBytes((int) AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES / 2);
        later.queryId = "qid-later";
        new MockUp<AuditStreamLoader>() {
            @Mock
            public AuditStreamLoader.Response loadBatch(String label, String payload) {
                if (calls.incrementAndGet() == 1) {
                    mgr.offerEvent(later);
                    Config.enable_audit_loader = false;
                }
                return loadResult(1, 1, 0);
            }
        };
        boolean origEnabled = Config.enable_audit_loader;
        long origCap = Config.audit_loader_batch_max_bytes;
        try {
            Config.enable_audit_loader = true;
            Config.audit_loader_batch_max_bytes = AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES;
            // A small first row leaves enough in-flight headroom to admit the later large row.
            mgr.offerEvent(baseEvent());
            long origInterval = Config.audit_loader_load_interval_seconds;
            try {
                Config.audit_loader_load_interval_seconds = 0;
                mgr.runAfterCatalogReady();
            } finally {
                Config.audit_loader_load_interval_seconds = origInterval;
            }
            Assertions.assertEquals(1, calls.get());
            Assertions.assertEquals(1, mgr.bufferedRows());
            Assertions.assertEquals(mgr.formatRowJson(later).getBytes(StandardCharsets.UTF_8).length, mgr.bufferedBytes());
            mgr.runAfterCatalogReady();
            Assertions.assertEquals(1, calls.get(), "the disable check must precede a second request");
            Assertions.assertEquals(2, mgr.controlChecks.get());
            Assertions.assertEquals(0, mgr.bufferedRows());
            Assertions.assertEquals(1, mgr.droppedEvents());
        } finally {
            Config.enable_audit_loader = origEnabled;
            Config.audit_loader_batch_max_bytes = origCap;
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
        // The floor accommodates a typical large row; the ceiling keeps the batch within what the
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

    @Test
    public void testSuccessfulSmallBatchWaitsForIntervalWhileHighWatermarkRunsImmediately() {
        AtomicLong clock = new AtomicLong();
        AuditLoaderMgr mgr = new AuditLoaderMgr(clock::get);
        AtomicInteger calls = mockLoadResults(loadResult(1, 1, 0));
        long origInterval = Config.audit_loader_load_interval_seconds;
        long origCap = Config.audit_loader_batch_max_bytes;
        try {
            Config.audit_loader_load_interval_seconds = 60;
            Config.audit_loader_batch_max_bytes = AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES;
            mgr.offerEvent(baseEvent());
            clock.addAndGet(TimeUnit.SECONDS.toNanos(59));
            mgr.maybeFlush();
            Assertions.assertEquals(0, calls.get());
            clock.addAndGet(TimeUnit.SECONDS.toNanos(1));
            mgr.maybeFlush();
            Assertions.assertEquals(1, calls.get());
            mgr.offerEvent(baseEvent());
            Assertions.assertEquals(TimeUnit.SECONDS.toNanos(60), mgr.nextFlushDelayNanos());
            mgr.offerEvent(eventWithStmtBytes((int) AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES / 2));
            Assertions.assertTrue(mgr.shouldFlushNow(), "a successful load must not force 5s idle at high watermark");
        } finally {
            Config.audit_loader_load_interval_seconds = origInterval;
            Config.audit_loader_batch_max_bytes = origCap;
        }
    }

    @Test
    public void testZeroIntervalKeepsSmallLoadsAtPollingCadence() {
        AtomicLong clock = new AtomicLong();
        AuditLoaderMgr mgr = new AuditLoaderMgr(clock::get);
        AtomicInteger calls = mockLoadResults(loadResult(1, 1, 0));
        long origInterval = Config.audit_loader_load_interval_seconds;
        try {
            Config.audit_loader_load_interval_seconds = 0;
            mgr.offerEvent(baseEvent());
            mgr.maybeFlush();
            Assertions.assertEquals(1, calls.get());
            mgr.offerEvent(baseEvent());
            mgr.maybeFlush();
            clock.addAndGet(TimeUnit.SECONDS.toNanos(4));
            mgr.maybeFlush();
            Assertions.assertEquals(1, calls.get(), "continuous small arrivals must not cause a request per event");
            clock.addAndGet(TimeUnit.SECONDS.toNanos(1));
            mgr.maybeFlush();
            Assertions.assertEquals(2, calls.get());
        } finally {
            Config.audit_loader_load_interval_seconds = origInterval;
        }
    }

    @Test
    public void testInFlightRowsRemainChargedAndOnlySubmittedRowsAreRemoved() {
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        AuditEvent large = eventWithStmtBytes((int) AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES * 3 / 5);
        AuditEvent later = baseEvent();
        later.queryId = "later";
        long origCap = Config.audit_loader_batch_max_bytes;
        AtomicInteger calls = new AtomicInteger();
        try {
            Config.audit_loader_batch_max_bytes = AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES;
            mgr.offerEvent(large);
            long inFlightBytes = mgr.bufferedBytes();
            long laterBytes = mgr.formatRowJson(later).getBytes(StandardCharsets.UTF_8).length;
            new MockUp<AuditStreamLoader>() {
                @Mock
                public AuditStreamLoader.Response loadBatch(String label, String payload) {
                    calls.incrementAndGet();
                    Assertions.assertEquals(1, mgr.bufferedRows());
                    Assertions.assertEquals(inFlightBytes, mgr.bufferedBytes());
                    mgr.offerEvent(large); // Cannot fit while the original large row is in flight.
                    Assertions.assertEquals(1, mgr.droppedEvents());
                    mgr.offerEvent(later);
                    Assertions.assertEquals(2, mgr.bufferedRows());
                    Assertions.assertEquals(inFlightBytes + laterBytes, mgr.bufferedBytes());
                    return loadResult(1, 1, 0);
                }
            };
            Assertions.assertEquals(SUCCESS, mgr.flush());
            Assertions.assertEquals(1, calls.get());
            Assertions.assertEquals(1, mgr.bufferedRows());
            Assertions.assertEquals(laterBytes, mgr.bufferedBytes());
            Assertions.assertEquals(1, mgr.droppedEvents());
        } finally {
            Config.audit_loader_batch_max_bytes = origCap;
        }
    }

    @Test
    public void testMaintenanceFollowsElapsedTimeRatherThanNumberOfBatches() {
        AtomicLong clock = new AtomicLong();
        ReadyAuditLoader mgr = new ReadyAuditLoader(clock::get);
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return true;
            }
        };
        boolean origEnabled = Config.enable_audit_loader;
        try {
            Config.enable_audit_loader = true;
            for (int i = 0; i < 100; i++) {
                mgr.runAfterCatalogReady();
            }
            Assertions.assertEquals(0, mgr.maintenanceChecks.get());
            clock.addAndGet(TimeUnit.MINUTES.toNanos(1));
            mgr.runAfterCatalogReady();
            Assertions.assertEquals(1, mgr.maintenanceChecks.get());
            for (int i = 0; i < 100; i++) {
                mgr.runAfterCatalogReady();
            }
            Assertions.assertEquals(1, mgr.maintenanceChecks.get());
            clock.addAndGet(TimeUnit.MINUTES.toNanos(1));
            mgr.runAfterCatalogReady();
            Assertions.assertEquals(2, mgr.maintenanceChecks.get());
        } finally {
            Config.enable_audit_loader = origEnabled;
        }
    }

    @Test
    @Timeout(10)
    public void testWatermarkWakeupBeforeAndDuringWait() throws Exception {
        boolean origEnabled = Config.enable_audit_loader;
        long origCap = Config.audit_loader_batch_max_bytes;
        long origInterval = Config.audit_loader_load_interval_seconds;
        ReadyAuditLoader mgr = new ReadyAuditLoader();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        Thread waiter = new Thread(() -> {
            entered.countDown();
            mgr.awaitNextCycle(5000);
            finished.countDown();
        });
        try {
            Config.enable_audit_loader = true;
            Config.audit_loader_batch_max_bytes = AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES;
            Config.audit_loader_load_interval_seconds = 3600;
            mgr.runAfterCatalogReady();
            waiter.start();
            Assertions.assertTrue(entered.await(1, TimeUnit.SECONDS));
            mgr.offerEvent(baseEvent());
            Assertions.assertFalse(finished.await(50, TimeUnit.MILLISECONDS), "a small row must not end the wait");
            mgr.offerEvent(eventWithStmtBytes((int) AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES / 2));
            Assertions.assertTrue(finished.await(1, TimeUnit.SECONDS), "crossing the watermark must wake the waiter");
            // The permit can precede entry into the wait; checking the queue must prevent a lost wakeup.
            Assertions.assertTimeoutPreemptively(Duration.ofSeconds(1), () -> mgr.awaitNextCycle(5000));
        } finally {
            mgr.setStop();
            waiter.join(1000);
            Config.enable_audit_loader = origEnabled;
            Config.audit_loader_batch_max_bytes = origCap;
            Config.audit_loader_load_interval_seconds = origInterval;
        }
    }

    @Test
    @Timeout(10)
    public void testDisabledAndMissingTableDoNotSpinAtHighWatermark() throws Exception {
        boolean origEnabled = Config.enable_audit_loader;
        long origCap = Config.audit_loader_batch_max_bytes;
        try {
            Config.audit_loader_batch_max_bytes = AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES;
            for (boolean enabled : List.of(false, true)) {
                ReadyAuditLoader mgr = new ReadyAuditLoader();
                mgr.tableReady = false;
                Config.enable_audit_loader = enabled;
                mgr.runAfterCatalogReady();
                mgr.offerEvent(eventWithStmtBytes((int) AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES / 2));
                CountDownLatch entered = new CountDownLatch(1);
                CountDownLatch finished = new CountDownLatch(1);
                Thread waiter = new Thread(() -> {
                    entered.countDown();
                    mgr.awaitNextCycle(5000);
                    finished.countDown();
                });
                try {
                    waiter.start();
                    Assertions.assertTrue(entered.await(1, TimeUnit.SECONDS));
                    Assertions.assertFalse(finished.await(50, TimeUnit.MILLISECONDS), "an unavailable path must poll");
                    mgr.setStop();
                    Assertions.assertTrue(finished.await(1, TimeUnit.SECONDS), "stop must wake an unavailable path");
                } finally {
                    mgr.setStop();
                    waiter.join(1000);
                }
            }
        } finally {
            Config.enable_audit_loader = origEnabled;
            Config.audit_loader_batch_max_bytes = origCap;
        }
    }

    @Test
    @Timeout(10)
    public void testDaemonLoadsHighWatermarkWithoutAnExtraFiveSecondSleep() throws Exception {
        ReadyAuditLoader mgr = new ReadyAuditLoader();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isReady(Invocation invocation) {
                return Thread.currentThread() == mgr || (Boolean) invocation.proceed();
            }
        };
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch loaded = new CountDownLatch(2);
        new MockUp<AuditStreamLoader>() {
            @Mock
            public AuditStreamLoader.Response loadBatch(String label, String payload) {
                if (calls.incrementAndGet() == 1) {
                    mgr.offerEvent(eventWithStmtBytes((int) AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES / 2));
                }
                loaded.countDown();
                return loadResult(1, 1, 0);
            }
        };
        boolean origEnabled = Config.enable_audit_loader;
        long origCap = Config.audit_loader_batch_max_bytes;
        long origInterval = Config.audit_loader_load_interval_seconds;
        try {
            Config.enable_audit_loader = true;
            Config.audit_loader_batch_max_bytes = AuditLoaderMgr.MIN_SAFE_BATCH_MAX_BYTES;
            Config.audit_loader_load_interval_seconds = 0;
            mgr.offerEvent(baseEvent());
            mgr.start();
            Assertions.assertTrue(loaded.await(2, TimeUnit.SECONDS), "the second high-watermark batch must run promptly");
        } finally {
            mgr.setStop();
            mgr.join(2000);
            Config.enable_audit_loader = origEnabled;
            Config.audit_loader_batch_max_bytes = origCap;
            Config.audit_loader_load_interval_seconds = origInterval;
        }
        Assertions.assertFalse(mgr.isAlive());
        Assertions.assertFalse(mgr.isRunning());
        Assertions.assertEquals(2, calls.get());
        Assertions.assertTrue(mgr.controlChecks.get() >= 2, "every request must have its own control checks");
        Assertions.assertEquals(0, mgr.bufferedRows());
        Assertions.assertEquals(0, mgr.droppedEvents());
    }

    @Test
    @Timeout(10)
    public void testStopAndInterruptExitWhileCatalogIsNotReady() throws Exception {
        AtomicReference<ReadyAuditLoader> current = new AtomicReference<>();
        AtomicReference<CountDownLatch> checked = new AtomicReference<>();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isReady(Invocation invocation) {
                if (Thread.currentThread() == current.get()) {
                    checked.get().countDown();
                    return false;
                }
                return invocation.proceed();
            }
        };
        for (boolean interrupt : List.of(false, true)) {
            ReadyAuditLoader mgr = new ReadyAuditLoader();
            current.set(mgr);
            checked.set(new CountDownLatch(1));
            try {
                mgr.setStop();
                mgr.start();
                Assertions.assertTrue(checked.get().await(1, TimeUnit.SECONDS));
                if (interrupt) {
                    mgr.interrupt();
                } else {
                    mgr.setStop();
                }
                mgr.join(1000);
                Assertions.assertFalse(mgr.isAlive());
                Assertions.assertFalse(mgr.isRunning());
                Assertions.assertEquals(0, mgr.controlChecks.get());
            } finally {
                mgr.setStop();
                mgr.join(1000);
            }
        }
    }

    @Test
    public void testMultibyteAndEscapedRowsKeepTheirExactPayloadAndByteAccounting() {
        AuditLoaderMgr mgr = new AuditLoaderMgr();
        AuditEvent first = baseEvent();
        first.stmt = "SELECT '中文🚀'\n\t\\\"";
        AuditEvent second = baseEvent();
        second.stmt = "SELECT 'éü'";
        String firstJson = mgr.formatRowJson(first);
        String secondJson = mgr.formatRowJson(second);
        AtomicReference<String> actual = new AtomicReference<>();
        new MockUp<AuditStreamLoader>() {
            @Mock
            public AuditStreamLoader.Response loadBatch(String label, String payload) {
                actual.set(payload);
                return loadResult(2, 2, 0);
            }
        };
        mgr.offerEvent(first);
        mgr.offerEvent(second);
        Assertions.assertEquals(firstJson.getBytes(StandardCharsets.UTF_8).length
                + secondJson.getBytes(StandardCharsets.UTF_8).length, mgr.bufferedBytes());
        Assertions.assertEquals(SUCCESS, mgr.flush());
        Assertions.assertEquals("[" + firstJson + "," + secondJson + "]", actual.get());
        JsonArray rows = JsonParser.parseString(actual.get()).getAsJsonArray();
        Assertions.assertEquals(first.stmt, rows.get(0).getAsJsonObject().get("stmt").getAsString());
        Assertions.assertEquals(second.stmt, rows.get(1).getAsJsonObject().get("stmt").getAsString());
        Assertions.assertEquals(0, mgr.bufferedBytes());
    }

    @Test
    public void testTruncationMatchesJdkEncoderAtEveryCharacterBoundary() throws Exception {
        String high = String.valueOf((char) 0xD83D);
        String low = String.valueOf((char) 0xDE80);
        List<String> samples = List.of("", "ascii", "éü", "中文", high + low,
                "a" + high + low + "b", high + "x" + low, "中" + high + "tail", low + high + low + "x");
        for (String sample : samples) {
            int length = sample.getBytes(StandardCharsets.UTF_8).length;
            for (int cap = 0; cap <= length + 1; cap++) {
                String actual = AuditLoaderMgr.truncateToBytes(sample, cap);
                if (length <= cap) {
                    Assertions.assertSame(sample, actual);
                } else {
                    ByteBuffer encoded = ByteBuffer.allocate(cap);
                    StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPLACE)
                            .onUnmappableCharacter(CodingErrorAction.REPLACE).encode(CharBuffer.wrap(sample), encoded, true);
                    String expected = new String(encoded.array(), 0, encoded.position(), StandardCharsets.UTF_8);
                    Assertions.assertEquals(expected, actual, "cap=" + cap + ", sample=" + sample);
                }
            }
        }
    }

    @Test
    public void testTruncatingLargeSqlDoesNotAllocateItsFullUtf8Encoding() {
        java.lang.management.ThreadMXBean platformBean = ManagementFactory.getThreadMXBean();
        Assumptions.assumeTrue(platformBean instanceof ThreadMXBean);
        ThreadMXBean bean = (ThreadMXBean) platformBean;
        Assumptions.assumeTrue(bean.isThreadAllocatedMemorySupported() && bean.isThreadAllocatedMemoryEnabled());
        int limit = 1024 * 1024;
        String sql = "s".repeat(8 * limit);
        AuditLoaderMgr.truncateToBytes("warmup", 2);
        long threadId = Thread.currentThread().getId();
        long before = bean.getThreadAllocatedBytes(threadId);
        String actual = AuditLoaderMgr.truncateToBytes(sql, limit);
        long allocated = bean.getThreadAllocatedBytes(threadId) - before;
        Assertions.assertEquals(limit, actual.length());
        Assertions.assertTrue(allocated < 2L * limit, "truncation allocated " + allocated + " bytes for a 1MiB prefix");
    }

}
