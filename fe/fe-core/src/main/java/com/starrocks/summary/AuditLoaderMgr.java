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

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.AutoInferUtil;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.plugin.PluginInfo;
import com.starrocks.plugin.PluginInfo.PluginType;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.server.GlobalStateMgr;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Builtin audit loader manager. Runs on every FE (leader/follower/observer): each FE buffers its own
 * audit events locally (fed by {@link AuditLoaderPlugin#exec}) and periodically flushes them into the
 * internal table {@code starrocks_audit_db__.starrocks_audit_tbl__} via an internal (credential-free)
 * stream load.
 * The actual load transaction always commits on the leader; followers only originate the stream load.
 *
 * <p>Reliability rules (to avoid the known QueryHistoryMgr defects):
 * <ul>
 *   <li>Never send an empty batch: the JSON array is built fresh per flush, and an empty batch returns
 *       early without calling the loader (QueryHistoryMgr sent a bare "]" for empty batches).</li>
 *   <li>Copy-then-remove: rows are removed from the queue only after the stream load succeeds; on failure
 *       they stay queued for the next cycle (no clear-before-confirm data loss).</li>
 *   <li>Table not ready: skip the flush without touching the queue.</li>
 *   <li>Overload: the queue is byte-bounded; events beyond the cap are dropped and counted.</li>
 * </ul>
 */
public class AuditLoaderMgr extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(AuditLoaderMgr.class);

    // Database and table holding the audit rows. The names match the ones used by the external
    // auditloader plugin so operators keep the same workflow.
    public static final String AUDIT_DB_NAME = "starrocks_audit_db__";
    public static final String AUDIT_TABLE_NAME = "starrocks_audit_tbl__";

    private static final long DAEMON_INTERVAL_MS = 5000;

    // Warn about dropped events at most once per this interval, to avoid log flooding.
    private static final long DROP_WARN_INTERVAL_MS = 60000;

    // Table maintenance (replication self-heal) only needs to run occasionally, not on every cycle.
    private static final long MAINTAIN_EVERY_N_CYCLES = 12;

    // Byte width of the stmt VARCHAR column. The stmt value is truncated to this many UTF-8 bytes so
    // an oversized statement is stored truncated instead of being silently dropped by the stream load
    // (a value exceeding the column width fails the row).
    private static final int STMT_MAX_BYTES = 1048576;

    /**
     * Single source of truth for the audit table columns: both the CREATE TABLE statement and the
     * JSON row are derived from this list, and for VARCHAR columns the value is truncated to
     * exactly the declared column width. Adding a column here is therefore a one-place change.
     *
     * <p>The load sends no explicit column list, so the JSON keys are mapped to the table columns
     * by name and a key without a matching column is ignored instead of failing the batch.
     *
     * <p>NOTE: schema evolution is deliberately not implemented. An existing table is never
     * altered, so on a cluster upgraded from a version without a column added here, that column
     * stays absent and its values are silently discarded on every batch. The audit pipeline keeps
     * running, but the new field only starts being collected once the table is altered or dropped
     * and recreated. Adding the evolution step means diffing these names against the live schema
     * and issuing ADD COLUMN for the missing ones (add only, never drop or modify, and skip
     * shadow columns while a schema change is in flight).
     */
    private record ColumnSpec(String name, String sqlType, String comment,
                              Function<AuditEvent, JsonElement> extractor) {
    }

    private static ColumnSpec varchar(String name, int maxBytes, String comment,
                                      Function<AuditEvent, String> getter) {
        // The truncation width is the declared column width by construction: a value longer than
        // the column fails the whole stream-load batch.
        return new ColumnSpec(name, "VARCHAR(" + maxBytes + ")", comment,
                event -> new JsonPrimitive(truncateToBytes(getter.apply(event), maxBytes)));
    }

    private static ColumnSpec number(String name, String sqlType, String comment,
                                     Function<AuditEvent, Number> getter) {
        return new ColumnSpec(name, sqlType, comment, event -> new JsonPrimitive(getter.apply(event)));
    }

    // Byte width shared by the wide text columns (materialized view lists and referenced
    // relations). It matches the table the external audit loader plugin creates, so operators
    // moving over from that plugin keep the schema they already have.
    private static final int WIDE_TEXT_MAX_BYTES = 65533;

    private static final List<ColumnSpec> COLUMN_SPECS = List.of(
            varchar("queryId", 64, "Unique query id", event -> event.queryId),
            new ColumnSpec("timestamp", "DATETIME NOT NULL", "Query start time",
                    event -> new JsonPrimitive(formatTimestamp(event.timestamp))),
            varchar("queryType", 12, "Query type: query, slow_query or connection",
                    AuditLoaderMgr::resolveQueryType),
            varchar("clientIp", 64, "Client host and port; an IPv6 host-port string can exceed 32 characters",
                    event -> event.clientIp),
            varchar("user", 64, "Login user", event -> event.user),
            varchar("authorizedUser", 64, "User identity", event -> event.authorizedUser),
            varchar("resourceGroup", 64, "Resource group", event -> event.resourceGroup),
            varchar("catalog", 32, "Catalog", event -> event.catalog),
            varchar("db", 96, "Database", event -> event.db),
            varchar("state", 8, "Query state: EOF, ERR or OK", event -> event.state),
            varchar("errorCode", 512, "Error code", event -> event.errorCode),
            number("queryTime", "BIGINT", "Query latency in milliseconds", event -> event.queryTime),
            number("scanBytes", "BIGINT", "Scanned bytes", event -> event.scanBytes),
            number("scanRows", "BIGINT", "Scanned rows", event -> event.scanRows),
            number("returnRows", "BIGINT", "Returned rows", event -> event.returnRows),
            number("cpuCostNs", "BIGINT", "CPU cost in nanoseconds", event -> event.cpuCostNs),
            number("memCostBytes", "BIGINT", "Memory cost in bytes", event -> event.memCostBytes),
            number("stmtId", "INT", "Incremental statement id", event -> event.stmtId),
            number("isQuery", "TINYINT", "Whether it is a query (1 or 0)", event -> event.isQuery ? 1 : 0),
            varchar("feIp", 128, "FE IP that executed the statement", event -> event.feIp),
            varchar("stmt", STMT_MAX_BYTES, "Original SQL statement", event -> event.stmt),
            varchar("digest", 32, "Slow SQL fingerprint", event -> event.digest),
            number("planCpuCosts", "DOUBLE", "Planning CPU cost in nanoseconds", event -> event.planCpuCosts),
            number("planMemCosts", "DOUBLE", "Planning memory cost in bytes", event -> event.planMemCosts),
            number("pendingTimeMs", "BIGINT", "Time pending in queue in milliseconds", event -> event.pendingTimeMs),
            varchar("candidateMVs", WIDE_TEXT_MAX_BYTES, "Candidate materialized views", event -> event.candidateMvs),
            varchar("hitMvs", WIDE_TEXT_MAX_BYTES, "Hit materialized views", event -> event.hitMVs),
            new ColumnSpec("QueriedRelations", "ARRAY<VARCHAR(" + WIDE_TEXT_MAX_BYTES + ")>",
                    "Tables and views referenced", AuditLoaderMgr::relationsArray),
            varchar("warehouse", 32, "Warehouse name", event -> event.warehouse));

    private static final DateTimeFormatter DATETIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // Buffered rows already serialized to JSON. Producer: the audit event worker thread (offerEvent).
    // Consumer: this daemon thread (flush). A concurrent queue plus an atomic byte counter keeps the
    // cross-thread access safe.
    private final ConcurrentLinkedQueue<String> rowQueue = new ConcurrentLinkedQueue<>();
    private final AtomicLong bufferBytes = new AtomicLong(0);
    private final AtomicLong droppedCount = new AtomicLong(0);

    private volatile boolean disabledByConflict = false;

    private long lastFlushMs = System.currentTimeMillis();
    private long lastDropWarnMs = 0;
    private long cycleCount = 0;

    public AuditLoaderMgr() {
        super("AuditLoader", DAEMON_INTERVAL_MS);
    }

    public boolean isDisabledByConflict() {
        return disabledByConflict;
    }

    @VisibleForTesting
    long bufferedBytes() {
        return bufferBytes.get();
    }

    @VisibleForTesting
    int bufferedRows() {
        return rowQueue.size();
    }

    @VisibleForTesting
    long droppedEvents() {
        return droppedCount.get();
    }

    /**
     * Buffer one audit event. Must be lightweight and non-blocking: it only serializes the event to a
     * JSON row and appends it to the bounded queue. Called from the single audit-event worker thread.
     */
    public void offerEvent(AuditEvent event) {
        String row;
        try {
            row = formatRowJson(event);
        } catch (Throwable t) {
            LOG.warn("failed to format audit event, skip it", t);
            return;
        }
        long rowBytes = row.getBytes(StandardCharsets.UTF_8).length;
        long cap = Config.audit_loader_batch_max_bytes;
        // Byte-bounded, never blocking. A single row larger than the cap can never fit a
        // cap-bounded batch, so drop it outright; otherwise drop only when adding it would exceed
        // the cap and the buffer is not already empty (always admit at least one row so the loader
        // can make progress). This keeps the buffer bounded by the cap.
        if (rowBytes > cap || (bufferBytes.get() + rowBytes > cap && !rowQueue.isEmpty())) {
            long dropped = droppedCount.incrementAndGet();
            long now = System.currentTimeMillis();
            if (now - lastDropWarnMs >= DROP_WARN_INTERVAL_MS) {
                lastDropWarnMs = now;
                LOG.warn("audit loader buffer is full or event too large ({} bytes cap), "
                        + "dropped {} events so far", cap, dropped);
            }
            return;
        }
        rowQueue.offer(row);
        bufferBytes.addAndGet(rowBytes);
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            disabledByConflict = detectConflict();
            if (!Config.enable_audit_loader || disabledByConflict) {
                // Disabled or superseded by an external dynamic AUDIT plugin: drop whatever is buffered
                // so it does not sit around indefinitely, and do not write anything.
                clearBuffer();
                return;
            }
            if (!ensureAuditTable()) {
                // Table not ready yet (leader has not created it). Skip the flush WITHOUT touching the
                // queue so buffered rows survive until the table exists.
                return;
            }
            if (++cycleCount % MAINTAIN_EVERY_N_CYCLES == 0 && GlobalStateMgr.getCurrentState().isLeader()) {
                correctReplicationNum();
            }
            maybeFlush();
        } catch (Throwable t) {
            LOG.warn("audit loader cycle failed", t);
        }
    }

    /**
     * A dynamic (externally installed) AUDIT plugin means an external auditloader may be running.
     * Stay inert to avoid importing audit data twice. This is deliberately conservative: any dynamic
     * AUDIT plugin disables the builtin loader, regardless of its name or target table.
     */
    private boolean detectConflict() {
        try {
            for (PluginInfo info : GlobalStateMgr.getCurrentState().getPluginMgr().getAllDynamicPluginInfo()) {
                if (info.getType() == PluginType.AUDIT) {
                    return true;
                }
            }
        } catch (Throwable t) {
            LOG.warn("failed to detect audit plugin conflict, treat as no conflict", t);
        }
        return false;
    }

    @VisibleForTesting
    void clearBuffer() {
        // Drain row by row and subtract exactly what is removed. A blanket clear()+set(0) could race
        // with a producer that offers a row in between, leaving bufferBytes permanently out of sync
        // with the queue (a negative counter would make the byte cap too lenient afterwards).
        String row;
        while ((row = rowQueue.poll()) != null) {
            bufferBytes.addAndGet(-row.getBytes(StandardCharsets.UTF_8).length);
        }
    }

    /**
     * Ensure the audit table exists. Table creation is a metadata write, only valid on the leader;
     * followers just report whether it already exists and otherwise wait for the leader to create it.
     *
     * <p>NOTE: the database and the table are recreated within one daemon cycle after a DROP, which
     * takes the name back before an operator can run RECOVER TABLE / RECOVER DATABASE: the recover
     * then fails because an object with the same name already exists, and the dropped data stays
     * unreachable in the recycle bin until {@code Config.catalog_trash_expire_second} elapses.
     * Turn {@code Config.enable_audit_loader} off before recovering a dropped audit table. The
     * internal statistics database behaves the same way.
     *
     * <p>Creating the database or the table is intentionally not logged, only a failure is, at
     * WARN. Nothing here reports that an object was recreated, so an operator looking into an
     * audit table that reappeared after a DROP has to read its create time from
     * information_schema rather than the FE log.
     */
    private boolean ensureAuditTable() {
        if (auditTableExists()) {
            return true;
        }
        if (!GlobalStateMgr.getCurrentState().isLeader()) {
            return false;
        }
        // The audit database is owned by this feature (unlike the internal _statistics_ database),
        // so it has to be created here as well before the table can be created.
        if (!ensureAuditDatabase()) {
            return false;
        }
        try {
            SimpleExecutor.getRepoExecutor().executeDDL(buildCreateTableSql());
        } catch (Throwable t) {
            LOG.warn("failed to create audit table {}.{}", AUDIT_DB_NAME, AUDIT_TABLE_NAME, t);
            return false;
        }
        return auditTableExists();
    }

    /**
     * Keep the audit table replication factor in line with the cluster, so a table created while
     * only one BE was up gets more replicas after the cluster grows.
     *
     * <p>In shared-data mode this never issues an ALTER: both the expected value
     * ({@code getSystemTableExpectedReplicationNum}) and the value used at creation time
     * ({@code AutoInferUtil.calDefaultReplicationNum}) are 1, so they always match.
     */
    @VisibleForTesting
    void correctReplicationNum() {
        Table table = getAuditTable();
        if (!(table instanceof OlapTable olapTable)) {
            return;
        }
        int expected = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                .getSystemTableExpectedReplicationNum();
        int current = olapTable.getPartitionInfo().getMinReplicationNum();
        if (current == expected) {
            return;
        }
        try {
            // The audit table is range-partitioned, so a plain SET would leave the existing
            // partitions untouched: change both those and the default for future partitions.
            SimpleExecutor.getRepoExecutor().executeDDL(String.format(
                    "ALTER TABLE `%s`.`%s` MODIFY PARTITION(*) SET ('replication_num'='%d')",
                    AUDIT_DB_NAME, AUDIT_TABLE_NAME, expected));
            SimpleExecutor.getRepoExecutor().executeDDL(String.format(
                    "ALTER TABLE `%s`.`%s` SET ('default.replication_num'='%d')",
                    AUDIT_DB_NAME, AUDIT_TABLE_NAME, expected));
            LOG.info("changed replication_num of audit table {}.{} from {} to {}",
                    AUDIT_DB_NAME, AUDIT_TABLE_NAME, current, expected);
        } catch (Throwable t) {
            LOG.warn("failed to change replication_num of audit table {}.{} from {} to {}",
                    AUDIT_DB_NAME, AUDIT_TABLE_NAME, current, expected, t);
        }
    }

    private Table getAuditTable() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(AUDIT_DB_NAME);
        if (db == null) {
            return null;
        }
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), AUDIT_TABLE_NAME);
    }

    private boolean ensureAuditDatabase() {
        if (GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(AUDIT_DB_NAME) != null) {
            return true;
        }
        try {
            SimpleExecutor.getRepoExecutor().executeDDL("CREATE DATABASE IF NOT EXISTS `" + AUDIT_DB_NAME + "`");
        } catch (Throwable t) {
            LOG.warn("failed to create audit database {}", AUDIT_DB_NAME, t);
            return false;
        }
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(AUDIT_DB_NAME) != null;
    }

    private boolean auditTableExists() {
        return getAuditTable() != null;
    }

    @VisibleForTesting
    void maybeFlush() {
        long now = System.currentTimeMillis();
        boolean intervalReached = now - lastFlushMs >= Config.audit_loader_load_interval_seconds * 1000;
        boolean bufferLarge = bufferBytes.get() >= Config.audit_loader_batch_max_bytes;
        if (rowQueue.isEmpty() || (!intervalReached && !bufferLarge)) {
            return;
        }
        // Only advance lastFlushMs when a batch actually landed. A failed flush must be retried on
        // the next daemon cycle instead of waiting a full interval.
        if (flush()) {
            lastFlushMs = System.currentTimeMillis();
        }
    }

    /**
     * Flush buffered rows in byte-bounded batches. For each batch: build the JSON array from a copy of
     * the queue head, stream load it, and only on success remove those rows from the queue. On failure
     * stop and retry on the next cycle (the rows stay queued).
     */
    @VisibleForTesting
    boolean flush() {
        long batchMaxBytes = Config.audit_loader_batch_max_bytes;
        boolean flushedAny = false;
        while (!rowQueue.isEmpty()) {
            // Collect one batch by copying references from the head, without removing yet.
            List<String> batch = new ArrayList<>();
            long batchBytes = 0;
            Iterator<String> it = rowQueue.iterator();
            while (it.hasNext()) {
                String row = it.next();
                long rowBytes = row.getBytes(StandardCharsets.UTF_8).length;
                if (!batch.isEmpty() && batchBytes + rowBytes > batchMaxBytes) {
                    break;
                }
                batch.add(row);
                batchBytes += rowBytes;
            }
            // Empty-batch guard: never send an empty payload to the stream load.
            if (batch.isEmpty()) {
                return flushedAny;
            }

            StringBuilder sb = new StringBuilder(batch.size() * 64 + 2);
            sb.append('[');
            for (int i = 0; i < batch.size(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(batch.get(i));
            }
            sb.append(']');

            boolean ok;
            try {
                StreamLoader loader = new StreamLoader(AUDIT_DB_NAME, AUDIT_TABLE_NAME);
                StreamLoader.Response response = loader.loadBatch("audit_loader", sb.toString());
                ok = response != null && response.status() == HttpStatus.SC_OK;
                if (!ok) {
                    LOG.warn("audit loader flush failed, batch rows[{}], response[{}]", batch.size(), response);
                }
            } catch (Throwable t) {
                if (t instanceof InterruptedException) {
                    // Restore the interrupt flag so the daemon can still react to shutdown.
                    Thread.currentThread().interrupt();
                }
                LOG.warn("audit loader flush failed, batch rows[{}]", batch.size(), t);
                ok = false;
            }

            if (!ok) {
                // Keep the batch queued and retry next cycle.
                return flushedAny;
            }
            // Success: remove exactly the flushed rows from the head (single consumer, FIFO).
            for (int i = 0; i < batch.size(); i++) {
                rowQueue.poll();
            }
            bufferBytes.addAndGet(-batchBytes);
            flushedAny = true;
            LOG.debug("audit loader flushed {} rows", batch.size());
        }
        return flushedAny;
    }

    @VisibleForTesting
    String formatRowJson(AuditEvent event) {
        JsonObject obj = new JsonObject();
        for (ColumnSpec spec : COLUMN_SPECS) {
            obj.add(spec.name(), spec.extractor().apply(event));
        }
        return obj.toString();
    }

    private static JsonElement relationsArray(AuditEvent event) {
        JsonArray relations = new JsonArray();
        if (event.queriedRelations != null) {
            for (String relation : event.queriedRelations) {
                relations.add(truncateToBytes(relation, WIDE_TEXT_MAX_BYTES));
            }
        }
        return relations;
    }

    private static String resolveQueryType(AuditEvent event) {
        if (event.type == AuditEvent.EventType.CONNECTION) {
            return "connection";
        }
        if (event.queryTime > Config.qe_slow_log_ms) {
            return "slow_query";
        }
        return "query";
    }

    private static String formatTimestamp(long epochMs) {
        long ms = epochMs > 0 ? epochMs : System.currentTimeMillis();
        return Instant.ofEpochMilli(ms).atZone(ZoneId.systemDefault()).format(DATETIME_FORMATTER);
    }

    // Truncate a string so that its UTF-8 encoding is at most maxBytes, without splitting a
    // multi-byte character.
    @VisibleForTesting
    static String truncateToBytes(String s, int maxBytes) {
        if (s == null) {
            return "";
        }
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= maxBytes) {
            return s;
        }
        int end = maxBytes;
        // Back off if we landed in the middle of a multi-byte character (continuation byte 10xxxxxx).
        while (end > 0 && (bytes[end] & 0xC0) == 0x80) {
            end--;
        }
        return new String(bytes, 0, end, StandardCharsets.UTF_8);
    }

    @VisibleForTesting
    String buildCreateTableSql() throws StarRocksException {
        int replicationNum = AutoInferUtil.calDefaultReplicationNum();
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS `").append(AUDIT_DB_NAME).append("`.`")
                .append(AUDIT_TABLE_NAME).append("` (\n");
        for (int i = 0; i < COLUMN_SPECS.size(); i++) {
            ColumnSpec spec = COLUMN_SPECS.get(i);
            sb.append("  `").append(spec.name()).append("` ").append(spec.sqlType())
                    .append(" COMMENT \"").append(spec.comment()).append("\"")
                    .append(i < COLUMN_SPECS.size() - 1 ? "," : "").append("\n");
        }
        sb.append(") ENGINE = OLAP\n")
                .append("DUPLICATE KEY (`queryId`, `timestamp`, `queryType`)\n")
                .append("COMMENT \"Builtin audit loader table\"\n")
                .append("PARTITION BY date_trunc('day', `timestamp`)\n")
                .append("PROPERTIES (\n")
                .append("  \"replication_num\" = \"").append(replicationNum).append("\",\n")
                .append("  \"partition_live_number\" = \"30\"\n")
                .append(")");
        return sb.toString();
    }
}
