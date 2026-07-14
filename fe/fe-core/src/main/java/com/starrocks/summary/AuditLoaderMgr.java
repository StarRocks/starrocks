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
import com.google.gson.JsonObject;
import com.starrocks.catalog.Database;
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
import com.starrocks.statistic.StatsConstants;
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

/**
 * Builtin audit loader manager. Runs on every FE (leader/follower/observer): each FE buffers its own
 * audit events locally (fed by {@link AuditLoaderPlugin#exec}) and periodically flushes them into the
 * internal table {@code _statistics_.starrocks_audit_tbl} via an internal (credential-free) stream load.
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

    private static final long DAEMON_INTERVAL_MS = 5000;

    // Warn about dropped events at most once per this interval, to avoid log flooding.
    private static final long DROP_WARN_INTERVAL_MS = 60000;

    // Byte width of the stmt VARCHAR column. The stmt value is truncated to this many UTF-8 bytes so
    // an oversized statement is stored truncated instead of being silently dropped by the stream load
    // (a value exceeding the column width fails the row). Keep in sync with buildCreateTableSql().
    private static final int STMT_MAX_BYTES = 1048576;

    // Column order shared by the CREATE TABLE statement, the JSON row keys, and the stream load
    // "columns" header. Keep the three in sync.
    private static final List<String> COLUMNS = List.of(
            "queryId", "timestamp", "queryType", "clientIp", "user", "authorizedUser", "resourceGroup",
            "catalog", "db", "state", "errorCode", "queryTime", "scanBytes", "scanRows", "returnRows",
            "cpuCostNs", "memCostBytes", "stmtId", "isQuery", "feIp", "stmt", "digest", "planCpuCosts",
            "planMemCosts", "pendingTimeMs", "candidateMVs", "hitMvs", "QueriedRelations", "warehouse");

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
        // Byte-bounded: drop when the buffer is full to protect FE memory. Never block the caller.
        if (bufferBytes.get() + rowBytes > Config.audit_loader_batch_max_bytes && !rowQueue.isEmpty()) {
            long dropped = droppedCount.incrementAndGet();
            long now = System.currentTimeMillis();
            if (now - lastDropWarnMs >= DROP_WARN_INTERVAL_MS) {
                lastDropWarnMs = now;
                LOG.warn("audit loader buffer is full ({} bytes), dropped {} events so far",
                        bufferBytes.get(), dropped);
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
     */
    private boolean ensureAuditTable() {
        if (auditTableExists()) {
            return true;
        }
        if (!GlobalStateMgr.getCurrentState().isLeader()) {
            return false;
        }
        try {
            SimpleExecutor.getRepoExecutor().executeDDL(buildCreateTableSql());
        } catch (Throwable t) {
            LOG.warn("failed to create audit table {}.{}", StatsConstants.STATISTICS_DB_NAME,
                    StatsConstants.AUDIT_LOADER_TABLE_NAME, t);
            return false;
        }
        return auditTableExists();
    }

    private boolean auditTableExists() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(StatsConstants.STATISTICS_DB_NAME);
        if (db == null) {
            return false;
        }
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), StatsConstants.AUDIT_LOADER_TABLE_NAME);
        return table != null;
    }

    @VisibleForTesting
    void maybeFlush() {
        long now = System.currentTimeMillis();
        boolean intervalReached = now - lastFlushMs >= Config.audit_loader_load_interval_seconds * 1000;
        boolean bufferLarge = bufferBytes.get() >= Config.audit_loader_batch_max_bytes;
        if (rowQueue.isEmpty() || (!intervalReached && !bufferLarge)) {
            return;
        }
        flush();
        lastFlushMs = System.currentTimeMillis();
    }

    /**
     * Flush buffered rows in byte-bounded batches. For each batch: build the JSON array from a copy of
     * the queue head, stream load it, and only on success remove those rows from the queue. On failure
     * stop and retry on the next cycle (the rows stay queued).
     */
    @VisibleForTesting
    void flush() {
        long batchMaxBytes = Config.audit_loader_batch_max_bytes;
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
                return;
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
                StreamLoader loader = new StreamLoader(StatsConstants.STATISTICS_DB_NAME,
                        StatsConstants.AUDIT_LOADER_TABLE_NAME, COLUMNS);
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
                return;
            }
            // Success: remove exactly the flushed rows from the head (single consumer, FIFO).
            for (int i = 0; i < batch.size(); i++) {
                rowQueue.poll();
            }
            bufferBytes.addAndGet(-batchBytes);
            LOG.debug("audit loader flushed {} rows", batch.size());
        }
    }

    @VisibleForTesting
    String formatRowJson(AuditEvent event) {
        // Every variable-length string is truncated to its column byte width (keep the widths in
        // sync with buildCreateTableSql): a value exceeding the column width fails the whole
        // stream-load batch, and since flush() retries the head batch until it succeeds, one
        // oversized field would wedge the audit pipeline.
        JsonObject obj = new JsonObject();
        obj.addProperty("queryId", truncateToBytes(event.queryId, 64));
        obj.addProperty("timestamp", formatTimestamp(event.timestamp));
        obj.addProperty("queryType", resolveQueryType(event));
        obj.addProperty("clientIp", truncateToBytes(event.clientIp, 64));
        obj.addProperty("user", truncateToBytes(event.user, 64));
        obj.addProperty("authorizedUser", truncateToBytes(event.authorizedUser, 64));
        obj.addProperty("resourceGroup", truncateToBytes(event.resourceGroup, 64));
        obj.addProperty("catalog", truncateToBytes(event.catalog, 32));
        obj.addProperty("db", truncateToBytes(event.db, 96));
        obj.addProperty("state", truncateToBytes(event.state, 8));
        obj.addProperty("errorCode", truncateToBytes(event.errorCode, 512));
        obj.addProperty("queryTime", event.queryTime);
        obj.addProperty("scanBytes", event.scanBytes);
        obj.addProperty("scanRows", event.scanRows);
        obj.addProperty("returnRows", event.returnRows);
        obj.addProperty("cpuCostNs", event.cpuCostNs);
        obj.addProperty("memCostBytes", event.memCostBytes);
        obj.addProperty("stmtId", event.stmtId);
        obj.addProperty("isQuery", event.isQuery ? 1 : 0);
        obj.addProperty("feIp", truncateToBytes(event.feIp, 128));
        // Truncate to the stmt column byte capacity so an oversized statement is stored truncated
        // rather than dropped by the stream load (a value longer than the column width fails the row).
        obj.addProperty("stmt", truncateToBytes(event.stmt, STMT_MAX_BYTES));
        obj.addProperty("digest", truncateToBytes(event.digest, 32));
        obj.addProperty("planCpuCosts", event.planCpuCosts);
        obj.addProperty("planMemCosts", event.planMemCosts);
        obj.addProperty("pendingTimeMs", event.pendingTimeMs);
        obj.addProperty("candidateMVs", truncateToBytes(event.candidateMvs, 65533));
        obj.addProperty("hitMvs", truncateToBytes(event.hitMVs, 65533));
        JsonArray relations = new JsonArray();
        if (event.queriedRelations != null) {
            for (String r : event.queriedRelations) {
                relations.add(truncateToBytes(r, 65533));
            }
        }
        obj.add("QueriedRelations", relations);
        obj.addProperty("warehouse", truncateToBytes(event.warehouse, 32));
        return obj.toString();
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

    private String buildCreateTableSql() throws StarRocksException {
        int replicationNum = AutoInferUtil.calDefaultReplicationNum();
        return "CREATE TABLE IF NOT EXISTS `" + StatsConstants.STATISTICS_DB_NAME + "`.`"
                + StatsConstants.AUDIT_LOADER_TABLE_NAME + "` (\n"
                + "  `queryId` VARCHAR(64) COMMENT \"Unique query id\",\n"
                + "  `timestamp` DATETIME NOT NULL COMMENT \"Query start time\",\n"
                + "  `queryType` VARCHAR(12) COMMENT \"Query type: query, slow_query or connection\",\n"
                + "  `clientIp` VARCHAR(64) COMMENT \"Client host and port; an IPv6 host-port string "
                + "can exceed 32 characters\",\n"
                + "  `user` VARCHAR(64) COMMENT \"Login user\",\n"
                + "  `authorizedUser` VARCHAR(64) COMMENT \"User identity\",\n"
                + "  `resourceGroup` VARCHAR(64) COMMENT \"Resource group\",\n"
                + "  `catalog` VARCHAR(32) COMMENT \"Catalog\",\n"
                + "  `db` VARCHAR(96) COMMENT \"Database\",\n"
                + "  `state` VARCHAR(8) COMMENT \"Query state: EOF, ERR or OK\",\n"
                + "  `errorCode` VARCHAR(512) COMMENT \"Error code\",\n"
                + "  `queryTime` BIGINT COMMENT \"Query latency in milliseconds\",\n"
                + "  `scanBytes` BIGINT COMMENT \"Scanned bytes\",\n"
                + "  `scanRows` BIGINT COMMENT \"Scanned rows\",\n"
                + "  `returnRows` BIGINT COMMENT \"Returned rows\",\n"
                + "  `cpuCostNs` BIGINT COMMENT \"CPU cost in nanoseconds\",\n"
                + "  `memCostBytes` BIGINT COMMENT \"Memory cost in bytes\",\n"
                + "  `stmtId` INT COMMENT \"Incremental statement id\",\n"
                + "  `isQuery` TINYINT COMMENT \"Whether it is a query (1 or 0)\",\n"
                + "  `feIp` VARCHAR(128) COMMENT \"FE IP that executed the statement\",\n"
                + "  `stmt` VARCHAR(" + STMT_MAX_BYTES + ") COMMENT \"Original SQL statement\",\n"
                + "  `digest` VARCHAR(32) COMMENT \"Slow SQL fingerprint\",\n"
                + "  `planCpuCosts` DOUBLE COMMENT \"Planning CPU cost in nanoseconds\",\n"
                + "  `planMemCosts` DOUBLE COMMENT \"Planning memory cost in bytes\",\n"
                + "  `pendingTimeMs` BIGINT COMMENT \"Time pending in queue in milliseconds\",\n"
                + "  `candidateMVs` VARCHAR(65533) NULL COMMENT \"Candidate materialized views\",\n"
                + "  `hitMvs` VARCHAR(65533) NULL COMMENT \"Hit materialized views\",\n"
                + "  `QueriedRelations` ARRAY<VARCHAR(65533)> NULL COMMENT \"Tables and views referenced\",\n"
                + "  `warehouse` VARCHAR(32) NULL COMMENT \"Warehouse name\"\n"
                + ") ENGINE = OLAP\n"
                + "DUPLICATE KEY (`queryId`, `timestamp`, `queryType`)\n"
                + "COMMENT \"Builtin audit loader table\"\n"
                + "PARTITION BY date_trunc('day', `timestamp`)\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"" + replicationNum + "\",\n"
                + "  \"partition_live_number\" = \"30\"\n"
                + ")";
    }
}
