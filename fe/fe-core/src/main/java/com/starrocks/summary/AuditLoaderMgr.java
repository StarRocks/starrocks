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
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonWriter;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.AutoInferUtil;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.plugin.PluginInfo;
import com.starrocks.plugin.PluginInfo.PluginType;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.server.GlobalStateMgr;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
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
 *   <li>Bounded request: the stream load request carries a fixed, short timeout so a BE that accepts
 *       the connection but never answers cannot wedge this daemon thread forever (QueryHistoryMgr's
 *       HTTP client has no such bound).</li>
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

    // Cadence of the periodic buffer status line (see reportStatus). Long enough that a busy FE
    // prints at most one line per minute, short enough that a buffer filling up is visible before
    // it reaches the cap and starts dropping.
    private static final long STATUS_REPORT_INTERVAL_MS = 60000;

    // Table maintenance (replication self-heal) only needs to run occasionally, not on every cycle.
    private static final long MAINTAIN_EVERY_N_CYCLES = 12;

    // Flush as soon as the buffer holds this fraction of the byte cap, instead of waiting for the
    // interval. The threshold has to stay below the cap: offerEvent only admits a row while the
    // buffer would stay within the cap, so a "buffer >= cap" trigger could never fire and every
    // event arriving after the buffer filled up would be dropped until the interval elapsed.
    // The remaining headroom is what absorbs the inflow of one daemon cycle, so with the defaults
    // (50MB cap, 5s cycle) half the cap keeps 25MB of headroom, i.e. about 5MB/s lossless.
    private static final long FLUSH_WATERMARK_DIVISOR = 2;

    // A negative flush interval has no meaning. Zero does: it asks for a flush on every daemon
    // cycle, which the cycle length already bounds, so only negatives are corrected here.
    private static final long MIN_FLUSH_INTERVAL_SECONDS = 0;

    // Give up on a batch that keeps failing, so one permanently rejected batch cannot block the
    // queue head forever. In practice this guards against sustained delivery failures (the target
    // BE/CN unavailable, a network partition) rather than schema mismatches: the load sends no
    // explicit column list, so a column the audit table lacks is silently ignored by automapping
    // instead of rejecting the batch (see the ColumnSpec javadoc below).
    // How long a stuck batch survives depends on how the attempts fail. When the BE is simply
    // unavailable, chooseBENode returns immediately and an attempt costs only the daemon interval,
    // so a batch is discarded after MAX_BATCH_FLUSH_RETRY * 5s = 30s (measured). When instead every
    // request hangs until StreamLoader's timeout, an attempt costs the timeout plus the interval,
    // stretching it to 6 * (10s + 5s) = 90s. Either way, during a sustained outage this does not
    // mean "keep the oldest N seconds of data, then one gap": every failed batch frees its slot in
    // the byte-bounded buffer once discarded, so the buffer keeps rolling (one batch discarded from
    // the head per cycle of attempts while new events are admitted at the tail) for as long as the
    // outage lasts, rather than holding a single contiguous slice. Only when the table itself is
    // missing (see ensureAuditTable) does the buffer stay static and produce one clean contiguous
    // gap, because flush() is never even attempted then.
    private static final int MAX_BATCH_FLUSH_RETRY = 6;

    // Byte width of the stmt VARCHAR column. The stmt value is truncated to this many UTF-8 bytes so
    // an oversized statement is stored truncated instead of being silently dropped by the stream load
    // (a value exceeding the column width fails the row).
    private static final int STMT_MAX_BYTES = 1048576;

    // Byte width shared by the wide text columns (materialized view lists and referenced
    // relations). It matches the table the external audit loader plugin creates, so operators
    // moving over from that plugin keep the schema they already have.
    private static final int WIDE_TEXT_MAX_BYTES = 65533;

    // Floor for the batch-byte cap actually enforced by offerEvent()/flush() (see
    // effectiveBatchMaxBytes()). Both callers always admit at least one row regardless of size, so
    // if audit_loader_batch_max_bytes were honored verbatim, lowering it at runtime below a single
    // row's size (e.g. one already sitting at the head of the queue) would let that one row alone
    // build a batch larger than the configured cap. Sized for one worst-case row dominated by
    // STMT_MAX_BYTES and two WIDE_TEXT_MAX_BYTES columns (candidateMVs, hitMvs), plus headroom for
    // the remaining small fixed-width columns and JSON syntax overhead. Deliberately does not
    // account for QueriedRelations: its size scales with the number of relations one statement
    // touches and is not otherwise capped, so a row with an extreme number of relations can still
    // exceed even this floor, same as it always could regardless of configuration.
    // Note this sums declared column widths, not the length those values take once escaped into
    // JSON, which is what the buffer actually holds: a control character turns into a six-byte
    // escape sequence, so one byte in becomes six bytes buffered. A statement full of them can
    // therefore still be rejected whole by offerEvent() when the cap is left near this floor, which
    // is why the floor guards against a pathological configuration rather than guaranteeing that
    // every legal row fits.
    // Package-private so the tests can size their rows and caps against the floor that is actually
    // enforced: shrinking audit_loader_batch_max_bytes below it has no effect, so a test that wants
    // a row to bump against the cap has to build a row on the floor's scale instead.
    @VisibleForTesting
    static final long MIN_SAFE_BATCH_MAX_BYTES = STMT_MAX_BYTES + 2L * WIDE_TEXT_MAX_BYTES + 4096;

    // Upper bound for the same config. A batch is assembled into one in-memory string before it is
    // sent, so the cap is also the size of a single allocation the FE has to find room for; well
    // before an absurd value could overflow anything, it would simply exhaust the heap. Bounding it
    // keeps the knob within what the flush path can actually carry, and mirrors the floor above so
    // neither end of the range is left undefined.
    @VisibleForTesting
    static final long MAX_SAFE_BATCH_MAX_BYTES = 512L * 1024 * 1024;

    // Upper bound for the flush interval. Anything beyond a day is indistinguishable from "never"
    // for an audit trail, and bounding it keeps the milliseconds conversion far from overflowing.
    private static final long MAX_FLUSH_INTERVAL_SECONDS = 86400;

    /**
     * Writes one column's value straight into the row being serialized. Deliberately not a
     * {@code Function<AuditEvent, JsonElement>}: returning an element per column would allocate a
     * whole JsonObject tree (one node and one primitive per column) per audit event just to
     * serialize and discard it, on a path that has to stay lightweight.
     */
    @FunctionalInterface
    private interface ColumnWriter {
        void write(AuditEvent event, JsonWriter out) throws IOException;
    }

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
    private record ColumnSpec(String name, String sqlType, String comment, ColumnWriter writer) {
    }

    private static ColumnSpec varchar(String name, int maxBytes, String comment,
                                      Function<AuditEvent, String> getter) {
        // The truncation width is the declared column width by construction, so a value this
        // feature itself writes should never exceed it. This only matters if the live table's
        // column has since drifted narrower than declared here (e.g. an operator manually altered
        // it): with StreamLoader's max_filter_ratio that row is filtered out instead of failing
        // the whole batch.
        return new ColumnSpec(name, "VARCHAR(" + maxBytes + ")", comment, (event, out) -> {
            String value = getter.apply(event);
            if (value == null) {
                // NULL rather than "": the table these columns come from declares them nullable, and
                // the external audit loader plugin writes NULL there, so writing an empty string
                // would make IS NULL miss exactly the rows this feature produced.
                out.nullValue();
            } else {
                out.value(truncateToBytes(value, maxBytes));
            }
        });
    }

    private static ColumnSpec number(String name, String sqlType, String comment,
                                     Function<AuditEvent, Number> getter) {
        return new ColumnSpec(name, sqlType, comment, (event, out) -> out.value(finiteOrZero(getter.apply(event))));
    }

    /**
     * Map NaN and the infinities to 0 so the row stays valid JSON. planCpuCosts and planMemCosts
     * carry whatever the optimizer computed, and CostEstimate.INFINITE is a real value there, so
     * this is reachable in normal operation rather than only on corrupt input. Writing the value
     * as-is would cost the whole batch, not just the row: it makes the payload unparseable, and a
     * parse error is not something max_filter_ratio can absorb.
     */
    private static Number finiteOrZero(Number value) {
        if (value instanceof Double doubleValue && !Double.isFinite(doubleValue)) {
            return 0;
        }
        if (value instanceof Float floatValue && !Float.isFinite(floatValue)) {
            return 0;
        }
        return value;
    }

    private static final List<ColumnSpec> COLUMN_SPECS = List.of(
            varchar("queryId", 64, "Unique query id", event -> event.queryId),
            new ColumnSpec("timestamp", "DATETIME NOT NULL", "Query start time",
                    (event, out) -> out.value(formatTimestamp(event.timestamp))),
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
            // BIGINT, not INT: stmtId comes from a process-lifetime AtomicLong shared by the whole
            // FE (StmtExecutor.STMT_ID_GENERATOR), so a long-lived, busy FE can exceed INT range.
            number("stmtId", "BIGINT", "Incremental statement id", event -> event.stmtId),
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
                    "Tables and views referenced", AuditLoaderMgr::writeRelations),
            varchar("warehouse", 32, "Warehouse name", event -> event.warehouse));

    /**
     * Fires (returns true) at most once per intervalMs; false otherwise. Safe for concurrent callers
     * (batchMaxBytesFloorWarnThrottle is reached from both the audit-event worker thread via
     * offerEvent and this daemon's own thread via shouldFlushNow/flush): a lock-free CAS ensures at
     * most one caller wins the race within any given interval.
     */
    private static final class LogThrottle {
        private final long intervalMs;
        private final AtomicLong lastFireMs = new AtomicLong(0);

        LogThrottle(long intervalMs) {
            this.intervalMs = intervalMs;
        }

        boolean tryAcquire() {
            long now = System.currentTimeMillis();
            long prev = lastFireMs.get();
            if (now - prev < intervalMs) {
                return false;
            }
            return lastFireMs.compareAndSet(prev, now);
        }
    }

    // One row already serialized to JSON, paired with its UTF-8 byte length computed once at
    // offerEvent time so clearBuffer/flush never have to re-encode the string just to re-measure it.
    private record BufferedRow(String json, long byteLen) {
    }

    // Buffered rows already serialized to JSON. Producer: the audit event worker thread (offerEvent).
    // Consumer: this daemon thread (flush). A concurrent queue plus an atomic byte counter keeps the
    // cross-thread access safe.
    private final ConcurrentLinkedQueue<BufferedRow> rowQueue = new ConcurrentLinkedQueue<>();
    private final AtomicLong bufferBytes = new AtomicLong(0);
    private final AtomicLong droppedCount = new AtomicLong(0);

    private volatile boolean disabledByConflict = false;

    private final LogThrottle dropWarnThrottle = new LogThrottle(DROP_WARN_INTERVAL_MS);
    private final LogThrottle flushIntervalFloorWarnThrottle = new LogThrottle(DROP_WARN_INTERVAL_MS);
    private final LogThrottle batchMaxBytesCeilingWarnThrottle = new LogThrottle(DROP_WARN_INTERVAL_MS);
    private final LogThrottle flushWarnThrottle = new LogThrottle(DROP_WARN_INTERVAL_MS);
    private final LogThrottle createTableWarnThrottle = new LogThrottle(DROP_WARN_INTERVAL_MS);
    private final LogThrottle tableMissingWarnThrottle = new LogThrottle(DROP_WARN_INTERVAL_MS);
    private final LogThrottle batchMaxBytesFloorWarnThrottle = new LogThrottle(DROP_WARN_INTERVAL_MS);
    private final LogThrottle replicationWarnThrottle = new LogThrottle(DROP_WARN_INTERVAL_MS);
    private final LogThrottle statusReportThrottle = new LogThrottle(STATUS_REPORT_INTERVAL_MS);

    private long lastFlushMs = System.currentTimeMillis();
    private long cycleCount = 0;
    private int consecutiveFlushFailures = 0;
    // Dropped total as of the last status line, so each line can report the delta over its own
    // interval instead of only an ever-growing total that says nothing about the current rate.
    private long lastReportedDropped = 0;
    // Set the first time ensureAuditTable() finds the table missing on a follower, cleared once it
    // shows up. Lets us only warn once the absence has actually persisted, instead of on every
    // normal ~5s gap between the leader creating the database and the table becoming visible here.
    private long tableMissingSinceMs = 0;

    public AuditLoaderMgr() {
        super("AuditLoader", DAEMON_INTERVAL_MS);
    }

    public boolean isDisabledByConflict() {
        return disabledByConflict;
    }

    /**
     * Public (unlike this class's other test hooks) so LogUtilTest, in a different package, can
     * exercise the "an external AUDIT plugin is present" gate in LogUtil without installing a
     * real plugin.
     */
    @VisibleForTesting
    public void setDisabledByConflict(boolean disabledByConflict) {
        this.disabledByConflict = disabledByConflict;
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
    int consecutiveFlushFailures() {
        return consecutiveFlushFailures;
    }

    @VisibleForTesting
    long droppedEvents() {
        return droppedCount.get();
    }

    /**
     * The batch-byte cap actually enforced by {@link #offerEvent} and {@link #flush}: the configured
     * {@link Config#audit_loader_batch_max_bytes}, clamped up to {@link #MIN_SAFE_BATCH_MAX_BYTES}.
     * Both callers always admit at least one row into an otherwise-empty batch regardless of its
     * size, so the enforced cap must never sit below what one worst-case row needs, or a value
     * lowered at runtime (it is mutable) could make a single already-buffered row alone exceed it.
     * Rate-limited warning when the configured value is actually being clamped, so a lowered config
     * that appears to have no effect is not silent.
     */
    @VisibleForTesting
    long effectiveBatchMaxBytes() {
        long configured = Config.audit_loader_batch_max_bytes;
        if (configured < MIN_SAFE_BATCH_MAX_BYTES) {
            if (batchMaxBytesFloorWarnThrottle.tryAcquire()) {
                LOG.warn("audit_loader_batch_max_bytes is set to {} bytes, below the minimum safe batch " +
                                "size of {} bytes (sized for one worst-case row); clamping to the minimum",
                        configured, MIN_SAFE_BATCH_MAX_BYTES);
            }
            return MIN_SAFE_BATCH_MAX_BYTES;
        }
        if (configured > MAX_SAFE_BATCH_MAX_BYTES) {
            if (batchMaxBytesCeilingWarnThrottle.tryAcquire()) {
                LOG.warn("audit_loader_batch_max_bytes is set to {} bytes, above the maximum of {}; "
                        + "clamping to the maximum", configured, MAX_SAFE_BATCH_MAX_BYTES);
            }
            return MAX_SAFE_BATCH_MAX_BYTES;
        }
        return configured;
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
        long rowBytes = utf8Length(row);
        long cap = effectiveBatchMaxBytes();
        // Byte-bounded, never blocking. A single row larger than the cap can never fit a
        // cap-bounded batch, so drop it outright; otherwise drop only when adding it would exceed
        // the cap and the buffer is not already empty (always admit at least one row so the loader
        // can make progress). This keeps the buffer bounded by the cap.
        if (rowBytes > cap || (bufferBytes.get() + rowBytes > cap && !rowQueue.isEmpty())) {
            long dropped = droppedCount.incrementAndGet();
            if (dropWarnThrottle.tryAcquire()) {
                LOG.warn("audit loader buffer is full or event too large ({} bytes cap), "
                        + "dropped {} events so far", cap, dropped);
            }
            return;
        }
        rowQueue.offer(new BufferedRow(row, rowBytes));
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
            // Before the table check on purpose: a missing table is exactly when the buffer grows
            // without anything else reporting it.
            reportStatus();
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
     * Periodic buffer status line, and this feature's only standing observability: dropping audit
     * events is an accepted outcome here, so an operator has to be able to see not just that drops
     * happened (the throttled warnings in offerEvent and discardBatch already say that, after the
     * fact) but how close the buffer is to the cap before it starts dropping.
     *
     * <p>Silent when there is nothing to say - an empty buffer, no new drops, no failing flush - so
     * an idle or healthy FE does not pay a line per interval. WARN when events were dropped since
     * the previous line, INFO otherwise, so a log search can separate "filling up" from "losing
     * data" by level alone.
     *
     * @return whether a line was actually emitted (for the tests; nothing else reads it)
     */
    @VisibleForTesting
    boolean reportStatus() {
        long dropped = droppedCount.get();
        long droppedDelta = dropped - lastReportedDropped;
        // isEmpty() rather than size(): ConcurrentLinkedQueue.size() walks the whole queue, so it is
        // only paid once a line is actually going to be emitted.
        if (rowQueue.isEmpty() && droppedDelta == 0 && consecutiveFlushFailures == 0) {
            return false;
        }
        if (!statusReportThrottle.tryAcquire()) {
            return false;
        }
        lastReportedDropped = dropped;
        long bytes = bufferBytes.get();
        long cap = effectiveBatchMaxBytes();
        String msg = String.format(
                "audit loader status: buffered %d rows / %d bytes (%d%% of the %d byte cap), "
                        + "dropped %d events since the last status line (%d total), %d consecutive flush failures",
                rowQueue.size(), bytes, bytes * 100 / cap, cap, droppedDelta, dropped, consecutiveFlushFailures);
        if (droppedDelta > 0) {
            LOG.warn(msg);
        } else {
            LOG.info(msg);
        }
        return true;
    }

    /**
     * A dynamic (externally installed) AUDIT plugin means an external auditloader may be running.
     * Stay inert to avoid importing audit data twice. This is deliberately conservative: any dynamic
     * AUDIT plugin disables the builtin loader, regardless of its name or target table.
     *
     * <p>The two sides target the same {@code starrocks_audit_db__.starrocks_audit_tbl__}, so an
     * operator installing the external plugin by following its own documentation silently stops
     * this feature. Neither side's documentation cross-references the other yet, so the only signal
     * is that audit rows stop arriving from one of the two producers.
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
        BufferedRow row;
        long discarded = 0;
        while ((row = rowQueue.poll()) != null) {
            bufferBytes.addAndGet(-row.byteLen());
            discarded++;
        }
        if (discarded > 0) {
            // Counted like every other way an event can be lost here: this path throws away up to a
            // whole buffer when the feature is switched off or an external plugin takes over, and
            // leaving it uncounted would hide that loss from droppedEvents and the status line.
            droppedCount.addAndGet(discarded);
        }
        // Otherwise a future, unrelated batch would inherit whatever retry count this one left
        // behind, cutting its own retry budget short.
        consecutiveFlushFailures = 0;
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
     * <p>Creating the database or the table is intentionally not logged, only a failure is, at WARN
     * and throttled to at most once per {@link #DROP_WARN_INTERVAL_MS} (on the leader for a create
     * failure, on a follower once the table has stayed missing for that long). Nothing here reports
     * that an object was recreated, so an operator looking into an audit table that reappeared
     * after a DROP has to read its create time from information_schema rather than the FE log.
     */
    private boolean ensureAuditTable() {
        if (auditTableExists()) {
            tableMissingSinceMs = 0;
            return true;
        }
        if (!GlobalStateMgr.getCurrentState().isLeader()) {
            long now = System.currentTimeMillis();
            if (tableMissingSinceMs == 0) {
                tableMissingSinceMs = now;
            } else if (now - tableMissingSinceMs >= DROP_WARN_INTERVAL_MS && tableMissingWarnThrottle.tryAcquire()) {
                LOG.warn("audit table {}.{} still not visible after {}ms, waiting for the leader to create it",
                        AUDIT_DB_NAME, AUDIT_TABLE_NAME, now - tableMissingSinceMs);
            }
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
            if (createTableWarnThrottle.tryAcquire()) {
                LOG.warn("failed to create audit table {}.{}", AUDIT_DB_NAME, AUDIT_TABLE_NAME, t);
            }
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
     *
     * <p>This converges instead of re-issuing the same ALTER every maintenance cycle, which relies
     * on two properties of the automatic (expression) partitioning this table uses:
     * <ul>
     *   <li>{@code getMinReplicationNum()} answers {@code orElse(1)} for an empty partition set, but
     *       the set is never empty: OlapTableFactory creates {@code $shadow_automatic_partition} at
     *       CREATE time and registers it with the {@code replication_num} from the create
     *       properties, so a table that has not been written to yet already reports the value it was
     *       created with rather than a phantom 1.</li>
     *   <li>{@code MODIFY PARTITION(*)} expands over {@code getPartitions()}, which includes that
     *       shadow partition, and the shadow-partition skip in {@code modifyPartitionsProperty}
     *       only triggers when a data property is being changed. Sending {@code replication_num}
     *       alone leaves that null, so the shadow partition is updated along with the rest and does
     *       not hold {@code getMinReplicationNum()} back at the old value forever.</li>
     * </ul>
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
            // With the automatic (expression) partitioning this table uses, a plain SET would leave
            // the existing partitions untouched: change both those and the default for new ones.
            SimpleExecutor.getRepoExecutor().executeDDL(String.format(
                    "ALTER TABLE `%s`.`%s` MODIFY PARTITION(*) SET ('replication_num'='%d')",
                    AUDIT_DB_NAME, AUDIT_TABLE_NAME, expected));
            SimpleExecutor.getRepoExecutor().executeDDL(String.format(
                    "ALTER TABLE `%s`.`%s` SET ('default.replication_num'='%d')",
                    AUDIT_DB_NAME, AUDIT_TABLE_NAME, expected));
            LOG.info("changed replication_num of audit table {}.{} from {} to {}",
                    AUDIT_DB_NAME, AUDIT_TABLE_NAME, current, expected);
        } catch (Throwable t) {
            // Throttled like every other recurring failure here: a cluster that keeps rejecting the
            // ALTER would otherwise log once per maintenance cycle indefinitely.
            if (replicationWarnThrottle.tryAcquire()) {
                LOG.warn("failed to change replication_num of audit table {}.{} from {} to {}",
                        AUDIT_DB_NAME, AUDIT_TABLE_NAME, current, expected, t);
            }
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
            // Shares createTableWarnThrottle with the CREATE TABLE failure below: both are the same
            // "leader cannot stand up the audit schema" story and a persistent failure of either
            // would otherwise log once per daemon cycle (every DAEMON_INTERVAL_MS) forever.
            if (createTableWarnThrottle.tryAcquire()) {
                LOG.warn("failed to create audit database {}", AUDIT_DB_NAME, t);
            }
            return false;
        }
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(AUDIT_DB_NAME) != null;
    }

    private boolean auditTableExists() {
        return getAuditTable() != null;
    }

    /**
     * {@link Config#audit_loader_load_interval_seconds} in milliseconds, with negative values
     * corrected. Mirrors how {@link #effectiveBatchMaxBytes()} treats its own config, so neither
     * knob can be driven into a state the loader does not define.
     */
    @VisibleForTesting
    long effectiveFlushIntervalMs() {
        long configured = Config.audit_loader_load_interval_seconds;
        if (configured < MIN_FLUSH_INTERVAL_SECONDS) {
            if (flushIntervalFloorWarnThrottle.tryAcquire()) {
                LOG.warn("audit_loader_load_interval_seconds is set to {}, which is negative; "
                        + "treating it as {}", configured, MIN_FLUSH_INTERVAL_SECONDS);
            }
            return MIN_FLUSH_INTERVAL_SECONDS * 1000;
        }
        if (configured > MAX_FLUSH_INTERVAL_SECONDS) {
            if (flushIntervalFloorWarnThrottle.tryAcquire()) {
                LOG.warn("audit_loader_load_interval_seconds is set to {}, above the maximum of {}; "
                        + "clamping to the maximum", configured, MAX_FLUSH_INTERVAL_SECONDS);
            }
            return MAX_FLUSH_INTERVAL_SECONDS * 1000;
        }
        return configured * 1000;
    }

    /**
     * Whether a flush is due: either the interval elapsed, or the buffer already holds enough bytes
     * that waiting for the interval would risk dropping events once it fills up.
     */
    @VisibleForTesting
    boolean shouldFlushNow() {
        if (rowQueue.isEmpty()) {
            return false;
        }
        boolean intervalReached = System.currentTimeMillis() - lastFlushMs >= effectiveFlushIntervalMs();
        boolean bufferLarge =
                bufferBytes.get() >= effectiveBatchMaxBytes() / FLUSH_WATERMARK_DIVISOR;
        return intervalReached || bufferLarge;
    }

    @VisibleForTesting
    void maybeFlush() {
        if (!shouldFlushNow()) {
            return;
        }
        // Advance the clock only on a clean run. flush() reports failure when it leaves a batch
        // behind, so a partial success does not reset the interval: the batch still at the head is
        // retried on the next daemon cycle rather than an interval later.
        if (flush()) {
            lastFlushMs = System.currentTimeMillis();
        }
    }

    /**
     * Flush buffered rows in byte-bounded batches. For each batch: build the JSON array from a copy of
     * the queue head, stream load it, and only on success remove those rows from the queue. On failure
     * stop and retry on the next cycle (the rows stay queued), and discard the batch once it has
     * failed {@link #MAX_BATCH_FLUSH_RETRY} times so it cannot block everything queued behind it.
     *
     * <p>Delivery is at-least-once. A retry builds a new label, so a batch the BE already committed
     * but whose response was lost is written again and leaves duplicate rows in the table. Making
     * retries idempotent would mean deriving the label from the batch content and treating the
     * "label already exists" rejection as success, which risks masking real failures and is
     * deliberately left out here.
     *
     * <p>Returns true only when the call ended without leaving a failed batch behind, so the caller
     * can tell a clean run from one that has to be retried promptly rather than an interval later.
     *
     * <p>The request sets a small {@code max_filter_ratio} (see StreamLoader) so that a handful of
     * rows the table cannot take ends the retry loop on the first attempt instead of holding the
     * batch at the head of the queue while the buffer backs up behind it. Note this does not cover
     * schema drift: with strict mode off, a value a nullable column cannot hold is stored as NULL
     * and the row still loads, so nothing is filtered and the ratio never comes into play there.
     *
     * <p>Known limitation: {@code audit_loader_batch_max_bytes} is mutable at runtime, and
     * consecutiveFlushFailures is not tied to a specific batch's identity. If the config changes
     * while the head of the queue is failing repeatedly, a later retry can rebuild a
     * differently-sized batch from the same queue head, yet the failure count keeps accumulating
     * across that change. The discard-after-N-failures behavior itself is still correct (it bounds
     * how long a stuck head can block the queue); only the "failed N times" figure in the resulting
     * discardBatch log line may not describe one unchanged batch. Not worth tracking batch identity
     * for this rare combination of a live config edit during an ongoing outage.
     */
    @VisibleForTesting
    boolean flush() {
        long batchMaxBytes = effectiveBatchMaxBytes();
        boolean flushedAny = false;
        while (!rowQueue.isEmpty()) {
            // Collect one batch by copying references from the head, without removing yet.
            List<BufferedRow> batch = new ArrayList<>();
            long batchBytes = 0;
            Iterator<BufferedRow> it = rowQueue.iterator();
            while (it.hasNext()) {
                BufferedRow row = it.next();
                if (!batch.isEmpty() && batchBytes + row.byteLen() > batchMaxBytes) {
                    break;
                }
                batch.add(row);
                batchBytes += row.byteLen();
            }
            // Empty-batch guard: never send an empty payload to the stream load.
            if (batch.isEmpty()) {
                return flushedAny;
            }

            // batchBytes is the exact UTF-8 size of the rows, and the separators add one char per
            // row, so this is the final length. Guessing low here is not free: a 50MB batch would
            // otherwise double its backing array a dozen times, copying the whole buffer each time.
            StringBuilder sb = new StringBuilder((int) Math.min(batchBytes + batch.size() + 2, Integer.MAX_VALUE));
            sb.append('[');
            for (int i = 0; i < batch.size(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(batch.get(i).json());
            }
            sb.append(']');

            boolean ok;
            try {
                StreamLoader loader = new StreamLoader(AUDIT_DB_NAME, AUDIT_TABLE_NAME);
                StreamLoader.Response response = loader.loadBatch("audit_loader", sb.toString());
                ok = response != null && response.status() == HttpStatus.SC_OK;
                if (!ok) {
                    warnFlushFailed(batch.size(), String.valueOf(response), null);
                }
            } catch (Throwable t) {
                if (t instanceof InterruptedException) {
                    // Restore the interrupt flag so the daemon can still react to shutdown.
                    Thread.currentThread().interrupt();
                }
                warnFlushFailed(batch.size(), null, t);
                ok = false;
            }

            if (!ok) {
                if (++consecutiveFlushFailures < MAX_BATCH_FLUSH_RETRY) {
                    // Keep the batch queued and retry next cycle. Report failure even when an
                    // earlier batch in this same call landed: the caller only advances the flush
                    // clock on a clean run, and reporting the partial success here would make the
                    // batch still sitting at the head wait a whole interval before its next
                    // attempt, stretching the time to give up on it by that interval each round.
                    return false;
                }
                // The head batch is rejected every time, which would block every later event as
                // well, so discard it and count the loss instead of stalling the pipeline.
                discardBatch(batch, batchBytes);
                consecutiveFlushFailures = 0;
                continue;
            }
            consecutiveFlushFailures = 0;
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

    /**
     * Serialize one event to its JSON row, streaming straight into the output instead of building a
     * JsonObject tree first. Gson still does the escaping, so the bytes are the same as the tree
     * would have produced.
     */
    @VisibleForTesting
    String formatRowJson(AuditEvent event) {
        StringWriter sw = new StringWriter();
        try (JsonWriter out = new JsonWriter(sw)) {
            // Deliberately strict. A lenient writer emits NaN/Infinity as bare tokens, which is not
            // valid JSON: the BE then fails to parse the whole payload, and since that is a parse
            // error rather than a row-level rejection, max_filter_ratio does not cover it and the
            // entire batch is retried and finally discarded. Non-finite values are neutralized in
            // number() instead, so nothing reaches this writer that it would have to reject.
            out.beginObject();
            for (ColumnSpec spec : COLUMN_SPECS) {
                out.name(spec.name());
                spec.writer().write(event, out);
            }
            out.endObject();
        } catch (IOException e) {
            // Unreachable: the sink is an in-memory StringWriter.
            throw new UncheckedIOException(e);
        }
        return sw.toString();
    }

    /**
     * The column is an ARRAY, so an absent relation list is an empty array rather than null (unlike
     * the nullable text columns). A null element inside the list is written as an empty string,
     * since truncateToBytes maps null to "": an ARRAY carrying NULL elements would say nothing
     * useful that an empty string does not.
     */
    private static void writeRelations(AuditEvent event, JsonWriter out) throws IOException {
        out.beginArray();
        if (event.queriedRelations != null) {
            for (String relation : event.queriedRelations) {
                out.value(truncateToBytes(relation, WIDE_TEXT_MAX_BYTES));
            }
        }
        out.endArray();
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

    /**
     * The cluster time_zone, not the JVM default: rows land in one table from every FE and are read
     * back with SQL, so the timeline has to be the cluster's rather than each process's. This runs
     * on the audit worker thread, which carries no ConnectContext, so TimeUtils resolves to the
     * cluster-wide default instead of some session's override.
     */
    private static String formatTimestamp(long epochMs) {
        return formatTimestamp(epochMs, TimeUtils.getTimeZone().toZoneId());
    }

    /**
     * Split out so the formatting itself can be exercised against an explicit zone, without a test
     * having to mutate the cluster-wide time_zone that every other test shares.
     */
    @VisibleForTesting
    static String formatTimestamp(long epochMs, ZoneId zone) {
        long ms = epochMs > 0 ? epochMs : System.currentTimeMillis();
        return Instant.ofEpochMilli(ms).atZone(zone).format(DateUtils.DATE_TIME_FORMATTER);
    }

    /**
     * Rate-limited failure log. A batch that keeps being rejected is retried every cycle, so an
     * unthrottled warning here would flood the FE log until the batch is finally discarded.
     */
    private void warnFlushFailed(int rows, String response, Throwable t) {
        if (!flushWarnThrottle.tryAcquire()) {
            return;
        }
        if (t != null) {
            LOG.warn("audit loader flush failed, batch rows[{}], attempt[{}]", rows, consecutiveFlushFailures + 1, t);
        } else {
            LOG.warn("audit loader flush failed, batch rows[{}], attempt[{}], response[{}]",
                    rows, consecutiveFlushFailures + 1, response);
        }
    }

    /**
     * Drop the head batch after it failed too many times, keeping the same accounting as a
     * successful flush so the byte counter stays in step with the queue. Likely cause is a
     * sustained delivery failure (target BE/CN unavailable, network partition) rather than a
     * schema mismatch: see {@link #MAX_BATCH_FLUSH_RETRY}.
     */
    private void discardBatch(List<BufferedRow> batch, long batchBytes) {
        int rows = batch.size();
        for (int i = 0; i < rows; i++) {
            rowQueue.poll();
        }
        bufferBytes.addAndGet(-batchBytes);
        droppedCount.addAndGet(rows);
        // batch is in queue (FIFO/chronological) order, so the first and last rows bound the time
        // range actually lost. This reuses the one log line discardBatch already emits (paced by
        // the retry cycle above, so at most one line roughly every MAX_BATCH_FLUSH_RETRY attempts)
        // instead of adding another log call, to avoid flooding the log during a sustained outage.
        LOG.error("audit loader discarded {} rows spanning [{}, {}] after {} failed attempts",
                rows, eventTimestamp(batch.get(0).json()), eventTimestamp(batch.get(rows - 1).json()),
                MAX_BATCH_FLUSH_RETRY);
    }

    /** Best-effort extraction of the "timestamp" field from one already-serialized JSON row. */
    private static String eventTimestamp(String row) {
        try {
            return JsonParser.parseString(row).getAsJsonObject().get("timestamp").getAsString();
        } catch (Throwable t) {
            return "unknown";
        }
    }

    /**
     * UTF-8 byte length of a string without encoding it into a throwaway byte array. Exactly matches
     * {@code s.getBytes(StandardCharsets.UTF_8).length}, including the JDK encoder's replacement of
     * an unpaired surrogate by a single-byte {@code '?'}, so the byte accounting stays in step with
     * what the stream load request body actually carries.
     *
     * <p>Worth avoiding the allocation: this is on the audit-event worker thread, once per row in
     * {@link #offerEvent} and once per VARCHAR column in {@link #truncateToBytes}, i.e. tens of
     * throwaway arrays per audit event, one of which can be the megabyte-scale statement text.
     */
    @VisibleForTesting
    static int utf8Length(String s) {
        int len = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < 0x80) {
                len += 1;
            } else if (c < 0x800) {
                len += 2;
            } else if (Character.isHighSurrogate(c) && i + 1 < s.length()
                    && Character.isLowSurrogate(s.charAt(i + 1))) {
                len += 4;
                i++;
            } else if (Character.isSurrogate(c)) {
                len += 1;
            } else {
                len += 3;
            }
        }
        return len;
    }

    // Truncate a string so that its UTF-8 encoding is at most maxBytes, without splitting a
    // multi-byte character.
    @VisibleForTesting
    static String truncateToBytes(String s, int maxBytes) {
        if (s == null) {
            return "";
        }
        // Measure first: only a value that actually has to be truncated pays for the byte[] copy,
        // instead of every VARCHAR column of every audit event allocating one just to find out it
        // already fits.
        if (utf8Length(s) <= maxBytes) {
            return s;
        }
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        int end = maxBytes;
        // Back off if we landed in the middle of a multi-byte character (continuation byte 10xxxxxx).
        while (end > 0 && (bytes[end] & 0xC0) == 0x80) {
            end--;
        }
        return new String(bytes, 0, end, StandardCharsets.UTF_8);
    }

    /**
     * The statement is CREATE TABLE IF NOT EXISTS, so it only ever shapes a table this feature
     * creates itself. A cluster that already built the table by hand from the external audit loader
     * plugin's documentation keeps that older schema forever: clientIp stays VARCHAR(32) and cannot
     * hold an IPv6 host-port string, and stmtId stays INT and overflows on a long-lived FE. Neither
     * shows up as an error -- a value the column cannot hold is stored as NULL, so the column simply
     * goes empty for the affected rows. Widening those columns by hand is the only way to recover
     * them; nothing here alters an existing table.
     */
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
