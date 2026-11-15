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
import com.google.gson.stream.JsonReader;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Builtin audit loader manager. Runs on every FE (leader/follower/observer): each FE buffers its own
 * audit events locally (fed by {@link AuditLoaderPlugin#exec}) and periodically flushes them into the
 * internal table {@code starrocks_audit_db__.starrocks_audit_tbl__} via an internal (credential-free)
 * stream load.
 * The actual load transaction always commits on the leader; followers only originate the stream load.
 *
 * <p>Reliability rules:
 * <ul>
 *   <li>Never send an empty batch: the JSON array is built fresh per flush, and an empty batch returns
 *       early without calling the loader.</li>
 *   <li>Copy-then-remove: rows are removed once committed, including Publish Timeout (committed but
 *       visibility not confirmed); failed loads stay queued for the next cycle.</li>
 *   <li>Table not ready: skip the flush without touching the queue.</li>
 *   <li>Overload: the queue is byte-bounded; events beyond the cap are dropped and counted.</li>
 *   <li>Bounded request: the stream load request carries a fixed, short timeout so a BE that accepts
 *       the connection but never answers cannot wedge this daemon thread forever.</li>
 * </ul>
 */
public class AuditLoaderMgr extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(AuditLoaderMgr.class);

    // Database and table holding the audit rows. The names match the ones used by the external
    // auditloader plugin so operators keep the same workflow.
    public static final String AUDIT_DB_NAME = "starrocks_audit_db__";
    public static final String AUDIT_TABLE_NAME = "starrocks_audit_tbl__";

    private static final long DAEMON_INTERVAL_MS = 5000;

    // Each drop reason warns at most once per interval, independently of counting every drop.
    private static final long DROP_WARN_INTERVAL_MS = 60000;

    // Cadence of the periodic buffer status line (see reportStatus). Long enough that a busy FE
    // prints at most one line per minute, short enough that a buffer filling up is visible before
    // it reaches the cap and starts dropping.
    private static final long STATUS_REPORT_INTERVAL_MS = 60000;

    // Busy periods can run many cycles per second. Maintenance must follow elapsed time, not batches.
    private static final long MAINTAIN_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);

    // Flush as soon as the buffer holds this fraction of the byte cap, instead of waiting for the
    // interval. The threshold has to stay below the cap: offerEvent only admits a row while the
    // buffer would stay within the cap, so a "buffer >= cap" trigger could never fire and every
    // event arriving after the buffer filled up would be dropped until the interval elapsed.
    // The remaining headroom absorbs arrivals while the previous batch is still in flight.
    private static final long FLUSH_WATERMARK_DIVISOR = 2;

    // Zero keeps the 5s polling cadence below the watermark; a large buffer can flush sooner.
    private static final long MIN_FLUSH_INTERVAL_SECONDS = 0;

    // Give up on a batch that keeps failing, so one permanently rejected batch cannot block the
    // queue head forever. In practice this guards against sustained delivery failures (the target
    // BE/CN unavailable, a network partition) rather than schema mismatches: the load sends no
    // explicit column list, so a column the audit table lacks is silently ignored by automapping
    // instead of rejecting the batch (see the ColumnSpec javadoc below).
    // The budget includes the initial attempt and two retries. With 5s gaps between attempts,
    // three immediate failures discard the batch about 10s after its first attempt. If every
    // request consumes StreamLoader's 10s timeout, that becomes about 40s, excluding scheduling
    // and other processing overhead. Either way, during a sustained outage this does not
    // mean "keep the oldest N seconds of data, then one gap": every failed batch frees its slot in
    // the byte-bounded buffer once discarded, so the buffer keeps rolling (one batch discarded from
    // the head per cycle of attempts while new events are admitted at the tail) for as long as the
    // outage lasts, rather than holding a single contiguous slice. Only when the table itself is
    // missing (see ensureAuditTable) does the buffer stay static and produce one clean contiguous
    // gap, because flush() is never even attempted then.
    private static final int MAX_BATCH_FLUSH_RETRY = 3;

    // Byte width of the stmt VARCHAR column. The stmt value is truncated to this many UTF-8 bytes so
    // an oversized statement is stored truncated instead of being silently dropped by the stream load
    // (a value exceeding the column width fails the row).
    private static final int STMT_MAX_BYTES = 1048576;

    // Byte width shared by the wide text columns (materialized view lists and referenced
    // relations). It matches the table the external audit loader plugin creates, so operators
    // moving over from that plugin keep the schema they already have.
    private static final int WIDE_TEXT_MAX_BYTES = 65533;

    // Floor for the batch-byte cap actually enforced by offerEvent()/flush() (see
    // effectiveBatchMaxBytes()). This keeps tiny configured values from rejecting typical large
    // statements. Sized for a row dominated by STMT_MAX_BYTES and two WIDE_TEXT_MAX_BYTES columns
    // (candidateMVs, hitMvs), plus headroom for
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
        // Truncate to the width declared here, which may differ from an existing table's schema.
        // Stream load uses non-strict mode by default: a value too wide for a nullable target column
        // can become NULL while its row still loads successfully. Such conversions do not count as
        // filtered rows; max_filter_ratio applies to rejected rows, not these field-level changes.
        return new ColumnSpec(name, "VARCHAR(" + maxBytes + ")", comment, (event, out) -> {
            String value = getter.apply(event);
            if (value == null) {
                // Preserve a missing value as SQL NULL for these nullable text columns; replacing
                // it with an empty string would change the result of IS NULL queries.
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
            // Keep the optimizer's estimates unchanged for plugin compatibility. CPU cost is a
            // cost-model score, not elapsed planning time; memory cost estimates execution memory.
            number("planCpuCosts", "DOUBLE", "Estimated CPU cost from the optimizer cost model", event -> event.planCpuCosts),
            number("planMemCosts", "DOUBLE", "Estimated execution memory cost in bytes", event -> event.planMemCosts),
            number("pendingTimeMs", "BIGINT", "Time pending in queue in milliseconds", event -> event.pendingTimeMs),
            varchar("candidateMVs", WIDE_TEXT_MAX_BYTES, "Candidate materialized views", event -> event.candidateMvs),
            varchar("hitMvs", WIDE_TEXT_MAX_BYTES, "Hit materialized views", event -> event.hitMVs),
            new ColumnSpec("QueriedRelations", "ARRAY<VARCHAR(" + WIDE_TEXT_MAX_BYTES + ")>",
                    "Tables and views referenced", AuditLoaderMgr::writeRelations),
            varchar("warehouse", 256, "Warehouse name", event -> event.warehouse));

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

    // Keep references to the immutable, still-charged rows, not another cached payload string.
    // A retry must reuse both the complete row set and the label, even after a capacity update.
    private record PendingBatch(List<BufferedRow> rows, long rowBytes, long chars, long capacity, String label) { }

    // Buffered rows already serialized to JSON. Producer: the audit event worker thread (offerEvent).
    // Consumer: this daemon thread (flush). A concurrent queue plus an atomic byte counter keeps the
    // cross-thread access safe.
    private final ConcurrentLinkedQueue<BufferedRow> rowQueue = new ConcurrentLinkedQueue<>();
    private final AtomicLong bufferBytes = new AtomicLong(0);
    private final AtomicLong droppedCount = new AtomicLong(0);
    // Filtered rows are a subset of droppedCount. Their own total keeps throttled batch warnings
    // from hiding filtering behind losses from a full buffer or exhausted retries in the status line.
    private final AtomicLong filteredCount = new AtomicLong(0);
    private final AtomicLong unknownStatsBatches = new AtomicLong(0);
    // Cumulative committed batches whose response did not confirm visibility, not a pending gauge.
    private final AtomicLong publishTimeoutBatches = new AtomicLong(0);
    private final AtomicLong alreadyCommittedBatches = new AtomicLong(0);

    private volatile boolean disabledByConflict = false;

    // Oversized rows are reported by both admission and selection; full-buffer drops are a
    // different cause and must not consume their warning slot (or vice versa).
    private final LogThrottle bufferFullWarnThrottle = new LogThrottle(DROP_WARN_INTERVAL_MS);
    private final LogThrottle oversizedRowWarnThrottle = new LogThrottle(DROP_WARN_INTERVAL_MS);
    private final LogThrottle flushIntervalFloorWarnThrottle = new LogThrottle(DROP_WARN_INTERVAL_MS);
    private final LogThrottle batchMaxBytesCeilingWarnThrottle = new LogThrottle(DROP_WARN_INTERVAL_MS);
    private final LogThrottle flushWarnThrottle = new LogThrottle(DROP_WARN_INTERVAL_MS);
    private final LogThrottle loadStatsWarnThrottle = new LogThrottle(DROP_WARN_INTERVAL_MS);
    private final LogThrottle createTableWarnThrottle = new LogThrottle(DROP_WARN_INTERVAL_MS);
    private final LogThrottle tableMissingWarnThrottle = new LogThrottle(DROP_WARN_INTERVAL_MS);
    private final LogThrottle batchMaxBytesFloorWarnThrottle = new LogThrottle(DROP_WARN_INTERVAL_MS);
    private final LogThrottle replicationWarnThrottle = new LogThrottle(DROP_WARN_INTERVAL_MS);
    private final LogThrottle statusReportThrottle = new LogThrottle(STATUS_REPORT_INTERVAL_MS);

    private final LongSupplier nanoTime;
    private final Semaphore wakeup = new Semaphore(0);
    private volatile boolean stopping;
    private boolean flushAllowed;
    private boolean hasFlushed;
    private boolean hasFailedAttempt;
    private long lastFlushNanos;
    private long lastFailureNanos;
    private long lastMaintenanceNanos;
    private int consecutiveFlushFailures = 0;
    private PendingBatch pendingBatch;
    private boolean pendingResultUnknown;
    // Totals as of the last status line. Advance only when a line is emitted, so throttling never
    // hides the increments since the last report.
    private long lastReportedDropped = 0;
    private long lastReportedFiltered = 0;
    private long lastReportedUnknownStatsBatches = 0;
    private long lastReportedPublishTimeoutBatches = 0;
    private long lastReportedAlreadyCommittedBatches = 0;
    // Set the first time ensureAuditTable() finds the table missing on a follower, cleared once it
    // shows up. Lets us only warn once the absence has actually persisted, instead of on every
    // normal ~5s gap between the leader creating the database and the table becoming visible here.
    private long tableMissingSinceMs = 0;

    public AuditLoaderMgr() {
        this(System::nanoTime);
    }

    @VisibleForTesting
    AuditLoaderMgr(LongSupplier nanoTime) {
        // Waiting is local and wakeable; the shared Daemon must not add another fixed 5s sleep.
        super("AuditLoader", 0);
        this.nanoTime = nanoTime;
        lastFlushNanos = nanoTime.getAsLong();
        lastMaintenanceNanos = lastFlushNanos;
    }

    @Override
    public synchronized void start() {
        if (getState() == State.NEW) {
            // Daemon.start() resets its stop flag on the first start; reset the local one with it.
            stopping = false;
        }
        super.start();
    }

    @Override
    public synchronized void setStop() {
        stopping = true;
        super.setStop();
        wakeup.release();
    }

    @Override
    protected void runOneCycle() {
        long waitMs = DAEMON_INTERVAL_MS;
        flushAllowed = false;
        try {
            // Check readiness once per cycle, so shutdown also works while the catalog is not ready.
            if (!stopping && GlobalStateMgr.getServingState().isReady()) {
                runAfterCatalogReady();
            } else {
                waitMs = 100;
            }
        } finally {
            awaitNextCycle(waitMs);
        }
    }

    @VisibleForTesting
    void awaitNextCycle(long maxWaitMs) {
        long started = nanoTime.getAsLong();
        try {
            if (Thread.currentThread().isInterrupted()) {
                setStop();
                return;
            }
            while (!stopping) {
                // Drain before checking predicates: an arrival before this point is visible in
                // bufferBytes; one after the check leaves a permit for tryAcquire. No lost wakeup.
                wakeup.drainPermits();
                long remaining = TimeUnit.MILLISECONDS.toNanos(maxWaitMs) - (nanoTime.getAsLong() - started);
                if (flushAllowed) {
                    remaining = Math.min(remaining, nextFlushDelayNanos());
                }
                if (remaining <= 0) {
                    return;
                }
                if (!wakeup.tryAcquire(remaining, TimeUnit.NANOSECONDS)) {
                    return;
                }
            }
        } catch (InterruptedException e) {
            // An interrupt is a stop signal, not a normal watermark notification.
            Thread.currentThread().interrupt();
            setStop();
        }
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

    @VisibleForTesting
    long filteredEvents() {
        return filteredCount.get();
    }

    @VisibleForTesting
    long batchesWithUnknownStatistics() {
        return unknownStatsBatches.get();
    }

    @VisibleForTesting
    long batchesWithPublishTimeout() {
        return publishTimeoutBatches.get();
    }

    @VisibleForTesting
    long batchesAlreadyCommitted() {
        return alreadyCommittedBatches.get();
    }

    /**
     * Budget for event admission and new batch selection: {@link Config#audit_loader_batch_max_bytes},
     * clamped to the supported range. A pending batch retains its selected budget across retries.
     * The queue counts row bytes; a batch also counts its JSON brackets and commas. An unselected
     * head that no longer fits alone after a runtime reduction is dropped.
     * Rate-limited warning when the configured value is actually being clamped, so a lowered config
     * that appears to have no effect is not silent.
     */
    @VisibleForTesting
    long effectiveBatchMaxBytes() {
        long configured = Config.audit_loader_batch_max_bytes;
        if (configured < MIN_SAFE_BATCH_MAX_BYTES) {
            if (batchMaxBytesFloorWarnThrottle.tryAcquire()) {
                LOG.warn("audit_loader_batch_max_bytes is set to {} bytes, below the minimum safe batch " +
                                "size of {} bytes; clamping to the minimum",
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
        // A single-row request still needs its array brackets. The queue itself counts only rows.
        if (rowBytes + 2 > cap) {
            warnOversizedRow(rowBytes, cap, droppedCount.incrementAndGet());
            return;
        }
        long buffered = bufferBytes.get();
        if (buffered + rowBytes > cap) {
            long dropped = droppedCount.incrementAndGet();
            if (bufferFullWarnThrottle.tryAcquire()) {
                LOG.warn("audit loader buffer is full: buffered_bytes={}, row_bytes={}, "
                        + "batch_limit_bytes={}, dropped_total={}", buffered, rowBytes, cap, dropped);
            }
            return;
        }
        rowQueue.offer(new BufferedRow(row, rowBytes));
        long bytes = bufferBytes.addAndGet(rowBytes);
        long watermark = cap / FLUSH_WATERMARK_DIVISOR;
        if (bytes >= watermark && bytes - rowBytes < watermark) {
            wakeup.release();
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        flushAllowed = false;
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
            long now = nanoTime.getAsLong();
            if (now - lastMaintenanceNanos >= MAINTAIN_INTERVAL_NANOS) {
                lastMaintenanceNanos = now;
                if (GlobalStateMgr.getCurrentState().isLeader()) {
                    correctReplicationNum();
                }
            }
            flushAllowed = true;
            maybeFlush();
        } catch (Throwable t) {
            flushAllowed = false;
            LOG.warn("audit loader cycle failed", t);
        }
    }

    /**
     * Periodic buffer status line, and this feature's only standing observability: dropping audit
     * events is an accepted outcome here, so an operator has to be able to see not just that drops
     * happened (the throttled warnings in offerEvent and discardBatch already say that, after the
     * fact) but how close the buffer is to the cap before it starts dropping.
     *
     * <p>Silent when there is nothing to say - an empty buffer, no new drops, unknown statistics or
     * publish timeouts, no failing flush. WARN for new drops, incomplete statistics or committed
     * batches whose visibility was not confirmed since the previous line, INFO otherwise.
     *
     * @return whether a line was actually emitted (for the tests; nothing else reads it)
     */
    @VisibleForTesting
    boolean reportStatus() {
        long dropped = droppedCount.get();
        long droppedDelta = dropped - lastReportedDropped;
        long filtered = filteredCount.get();
        long filteredDelta = filtered - lastReportedFiltered;
        long unknownStats = unknownStatsBatches.get();
        long unknownStatsDelta = unknownStats - lastReportedUnknownStatsBatches;
        long publishTimeouts = publishTimeoutBatches.get();
        long publishTimeoutDelta = publishTimeouts - lastReportedPublishTimeoutBatches;
        long alreadyCommitted = alreadyCommittedBatches.get();
        long alreadyCommittedDelta = alreadyCommitted - lastReportedAlreadyCommittedBatches;
        // isEmpty() rather than size(): ConcurrentLinkedQueue.size() walks the whole queue, so it is
        // only paid once a line is actually going to be emitted.
        if (rowQueue.isEmpty() && droppedDelta == 0 && unknownStatsDelta == 0 && publishTimeoutDelta == 0
                && alreadyCommittedDelta == 0
                && consecutiveFlushFailures == 0) {
            return false;
        }
        if (!statusReportThrottle.tryAcquire()) {
            return false;
        }
        lastReportedDropped = dropped;
        lastReportedFiltered = filtered;
        lastReportedUnknownStatsBatches = unknownStats;
        lastReportedPublishTimeoutBatches = publishTimeouts;
        lastReportedAlreadyCommittedBatches = alreadyCommitted;
        long bytes = bufferBytes.get();
        long cap = effectiveBatchMaxBytes();
        String msg = String.format(
                "audit loader status: buffered %d rows / %d bytes (%d%% of the %d byte cap), "
                        + "dropped %d events since the last status line (%d total), %d consecutive flush failures, "
                        + "filtered %d events since the last status line (%d total; included in dropped), "
                        + "%d loads with unknown row statistics since the last status line (%d total), "
                        + "%d loads committed with publish timeout since the last status line (%d total), "
                        + "%d loads confirmed previously committed since the last status line (%d total)",
                rowQueue.size(), bytes, bytes * 100 / cap, cap, droppedDelta, dropped, consecutiveFlushFailures,
                filteredDelta, filtered, unknownStatsDelta, unknownStats, publishTimeoutDelta, publishTimeouts,
                alreadyCommittedDelta, alreadyCommitted);
        if (droppedDelta > 0 || unknownStatsDelta > 0 || publishTimeoutDelta > 0 || alreadyCommittedDelta > 0) {
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
        hasFailedAttempt = false;
        pendingBatch = null;
        pendingResultUnknown = false;
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
    @VisibleForTesting
    boolean ensureAuditTable() {
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
     *
     * <p>The partition and default values are checked independently: the two ALTER statements are not
     * atomic, so a later cycle must finish a default change even if the partition change succeeded.
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
        int currentDefault = olapTable.getDefaultReplicationNum();
        if (current == expected && currentDefault == expected) {
            return;
        }
        String stage = "partitions";
        try {
            // With the automatic (expression) partitioning this table uses, a plain SET would leave
            // the existing partitions untouched: change both those and the default for new ones.
            if (current != expected) {
                SimpleExecutor.getRepoExecutor().executeDDL(String.format(
                        "ALTER TABLE `%s`.`%s` MODIFY PARTITION(*) SET ('replication_num'='%d')",
                        AUDIT_DB_NAME, AUDIT_TABLE_NAME, expected));
            }
            if (currentDefault != expected) {
                stage = "default";
                SimpleExecutor.getRepoExecutor().executeDDL(String.format(
                        "ALTER TABLE `%s`.`%s` SET ('default.replication_num'='%d')",
                        AUDIT_DB_NAME, AUDIT_TABLE_NAME, expected));
            }
            LOG.info("changed replication_num of audit table {}.{}: partitions {} -> {}, default {} -> {}",
                    AUDIT_DB_NAME, AUDIT_TABLE_NAME, current, expected, currentDefault, expected);
        } catch (Throwable t) {
            // Throttled like every other recurring failure here: a cluster that keeps rejecting the
            // ALTER would otherwise log once per maintenance cycle indefinitely.
            if (replicationWarnThrottle.tryAcquire()) {
                LOG.warn("failed to change replication_num of audit table {}.{}: stage={}, expected={}, "
                                + "partition_replication_num_before={}, default_replication_num_before={}",
                        AUDIT_DB_NAME, AUDIT_TABLE_NAME, stage, expected, current, currentDefault, t);
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

    @VisibleForTesting
    long nextFlushDelayNanos() {
        if (rowQueue.isEmpty()) {
            return Long.MAX_VALUE;
        }
        long now = nanoTime.getAsLong();
        if (hasFailedAttempt) {
            long remaining = TimeUnit.MILLISECONDS.toNanos(DAEMON_INTERVAL_MS) - (now - lastFailureNanos);
            if (remaining > 0) {
                return remaining;
            }
        }
        if (consecutiveFlushFailures > 0
                || bufferBytes.get() >= effectiveBatchMaxBytes() / FLUSH_WATERMARK_DIVISOR) {
            return 0;
        }
        long intervalMs = effectiveFlushIntervalMs();
        // Preserve the zero-interval polling cadence for small batches, without delaying a full one.
        if (intervalMs == 0) {
            if (!hasFlushed) {
                return 0;
            }
            intervalMs = DAEMON_INTERVAL_MS;
        }
        return Math.max(0, TimeUnit.MILLISECONDS.toNanos(intervalMs) - (now - lastFlushNanos));
    }

    @VisibleForTesting
    boolean shouldFlushNow() {
        return nextFlushDelayNanos() == 0;
    }

    @VisibleForTesting
    void maybeFlush() {
        if (shouldFlushNow()) {
            flush();
        }
    }

    enum FlushResult {
        EMPTY, SUCCESS, RETRY, DISCARDED, OVERSIZED
    }

    /**
     * Submit at most one batch. Retain its immutable rows, capacity snapshot and label until a
     * committed response or the third failed attempt. New arrivals stay behind that batch.
     * Capacity updates affect admission and subsequent batches, not a previously selected retry.
     * Same-label retries rely on the FE transaction label retention window for deduplication.
     */
    @VisibleForTesting
    FlushResult flush() {
        if (pendingBatch == null) {
            boolean hadRows = !rowQueue.isEmpty();
            pendingBatch = selectBatch();
            if (pendingBatch == null) {
                return hadRows ? FlushResult.OVERSIZED : FlushResult.EMPTY;
            }
            pendingResultUnknown = false;
        }
        PendingBatch selected = pendingBatch;
        List<BufferedRow> batch = selected.rows();
        // Rebuild from immutable strings; retain no extra batch-sized string during backoff.
        StringBuilder sb = new StringBuilder((int) Math.min(selected.chars(), Integer.MAX_VALUE));
        sb.append('[');
        for (int i = 0; i < batch.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(batch.get(i).json());
        }
        sb.append(']');

        AuditStreamLoader.Response response = null;
        boolean ok;
        try {
            AuditStreamLoader loader = new AuditStreamLoader(AUDIT_DB_NAME, AUDIT_TABLE_NAME);
            response = loader.loadBatch(selected.label(), sb.toString());
            ok = response != null && response.committed();
            if (!ok) {
                pendingResultUnknown |= response == null
                        || response.outcome() == AuditStreamLoader.Outcome.IN_PROGRESS
                        || response.outcome() == AuditStreamLoader.Outcome.UNKNOWN;
                warnFlushFailed(batch.size(), String.valueOf(response), null);
            }
        } catch (Throwable t) {
            pendingResultUnknown = true;
            if (t instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            warnFlushFailed(batch.size(), null, t);
            ok = false;
        }
        if (!ok) {
            hasFailedAttempt = true;
            lastFailureNanos = nanoTime.getAsLong();
            if (++consecutiveFlushFailures < MAX_BATCH_FLUSH_RETRY) {
                return FlushResult.RETRY;
            }
            discardBatch(batch, selected.rowBytes());
            pendingBatch = null;
            pendingResultUnknown = false;
            consecutiveFlushFailures = 0;
            // Later batches retain the cooldown and get a new label and full attempt budget.
            return FlushResult.DISCARDED;
        }
        consecutiveFlushFailures = 0;
        hasFailedAttempt = false;
        for (int i = 0; i < batch.size(); i++) {
            rowQueue.poll();
        }
        bufferBytes.addAndGet(-selected.rowBytes());
        pendingBatch = null;
        pendingResultUnknown = false;
        hasFlushed = true;
        lastFlushNanos = nanoTime.getAsLong();
        recordCommittedLoad(batch.size(), response);
        return FlushResult.SUCCESS;
    }

    private PendingBatch selectBatch() {
        long batchMaxBytes = effectiveBatchMaxBytes();
        List<BufferedRow> batch = new ArrayList<>();
        long rowsBytes = 0;
        long payloadBytes = 2; // Array brackets are part of the request, not the buffered rows.
        long batchChars = 2; // Array brackets; commas are added between rows below.
        Iterator<BufferedRow> it = rowQueue.iterator();
        while (it.hasNext()) {
            BufferedRow row = it.next();
            long addedBytes = row.byteLen() + (batch.isEmpty() ? 0L : 1L);
            if (payloadBytes + addedBytes > batchMaxBytes) {
                if (batch.isEmpty()) {
                    rowQueue.poll();
                    bufferBytes.addAndGet(-row.byteLen());
                    consecutiveFlushFailures = 0;
                    // A different head gets a fresh attempt budget, but retains any failure cooldown.
                    warnOversizedRow(row.byteLen(), batchMaxBytes, droppedCount.incrementAndGet());
                    return null;
                }
                break;
            }
            batchChars += row.json().length() + (batch.isEmpty() ? 0L : 1L);
            batch.add(row);
            rowsBytes += row.byteLen();
            payloadBytes += addedBytes;
        }
        if (batch.isEmpty()) {
            return null;
        }

        return new PendingBatch(List.copyOf(batch), rowsBytes, batchChars, batchMaxBytes, AuditStreamLoader.newLabel());
    }

    private void recordCommittedLoad(int submitted, AuditStreamLoader.Response response) {
        AuditStreamLoader.LoadStats stats = response.stats();
        Long total = stats == null ? null : stats.totalRows();
        Long loaded = stats == null ? null : stats.loadedRows();
        Long filtered = stats == null ? null : stats.filteredRows();
        String statisticsIssue = invalidStatisticsReason(submitted, stats);
        if (statisticsIssue != null) {
            // Check even partial statistics before counting any filtering. The commit is terminal.
            total = null;
            loaded = null;
            filtered = null;
        }
        boolean incomplete = total == null || loaded == null || filtered == null || total != submitted;
        if (filtered != null) {
            // Account each committed batch exactly once, independently of log throttling. Do not
            // infer filtering from total-loaded: the response can also contain unselected rows.
            droppedCount.addAndGet(filtered);
            filteredCount.addAndGet(filtered);
        }
        if (incomplete) {
            unknownStatsBatches.incrementAndGet();
        }
        boolean publishTimeout = response.outcome() == AuditStreamLoader.Outcome.COMMITTED_PENDING_VISIBILITY;
        if (publishTimeout) {
            publishTimeoutBatches.incrementAndGet();
        }
        boolean alreadyCommitted = response.outcome() == AuditStreamLoader.Outcome.ALREADY_COMMITTED;
        if (alreadyCommitted) {
            alreadyCommittedBatches.incrementAndGet();
        }
        String errorUrl = stats == null ? null : stats.errorUrl();
        String prefix = alreadyCommitted ? "audit loader load previously committed: "
                : publishTimeout ? "audit loader load committed, awaiting visibility: " : "audit loader load succeeded: ";
        String message = prefix
                + "submitted={}, total={}, loaded={}, filtered={}, error_url={}, row_statistics_incomplete={}, "
                + "statistics_issue={}, raw_statistics={}";
        LOG.debug(message, submitted, countForLog(total), countForLog(loaded), countForLog(filtered), errorUrl, incomplete,
                statisticsIssue, stats);
        if ((alreadyCommitted || publishTimeout || incomplete || (filtered != null && filtered > 0))
                && loadStatsWarnThrottle.tryAcquire()) {
            LOG.warn(message, submitted, countForLog(total), countForLog(loaded), countForLog(filtered), errorUrl, incomplete,
                    statisticsIssue, stats);
        }
    }

    private static String invalidStatisticsReason(int submitted, AuditStreamLoader.LoadStats stats) {
        if (stats == null) {
            return "missing_statistics";
        }
        if (stats.invalidCounts()) {
            return "malformed_row_count";
        }
        Long total = stats.totalRows();
        Long loaded = stats.loadedRows();
        Long filtered = stats.filteredRows();
        for (Long count : new Long[] {total, loaded, filtered}) {
            if (count != null && (count < 0 || count > submitted)) {
                return "row_count_out_of_range";
            }
        }
        if (total != null && ((loaded != null && loaded > total) || (filtered != null && filtered > total))) {
            return "row_count_exceeds_total";
        }
        long limit = total == null ? submitted : total;
        // Counts have been bounded above; subtraction also keeps this safe if their type grows later.
        if (loaded != null && filtered != null && loaded > limit - filtered) {
            return "loaded_plus_filtered_exceeds_limit";
        }
        return null;
    }

    private static Object countForLog(Long count) {
        return count == null ? "unknown" : count;
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

    // Size rejection is local: do not describe it as a failed network attempt or log row contents.
    private void warnOversizedRow(long rowBytes, long cap, long dropped) {
        if (oversizedRowWarnThrottle.tryAcquire()) {
            LOG.warn("audit loader dropped oversized event: row_bytes={}, request_bytes={}, "
                            + "batch_limit_bytes={}, dropped_total={}", rowBytes, rowBytes + 2, cap, dropped);
        }
    }

    /** Rate-limited failure log for actual load attempts. */
    private void warnFlushFailed(int rows, String response, Throwable t) {
        if (!flushWarnThrottle.tryAcquire()) {
            return;
        }
        if (t != null) {
            LOG.warn("audit loader flush failed, batch rows[{}], attempt[{}], label={}, batch_limit_bytes={}",
                    rows, consecutiveFlushFailures + 1, pendingBatch.label(), pendingBatch.capacity(), t);
        } else {
            LOG.warn("audit loader flush failed, batch rows[{}], attempt[{}], label={}, batch_limit_bytes={}, response[{}]",
                    rows, consecutiveFlushFailures + 1, pendingBatch.label(), pendingBatch.capacity(), response);
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
        String first = eventTimestamp(batch.get(0).json());
        String last = rows == 1 ? first : eventTimestamp(batch.get(rows - 1).json());
        LOG.error("audit loader discarded {} rows spanning [{}, {}], label={}, commit_result_unknown={} after {} failed attempts",
                rows, first, last, pendingBatch.label(), pendingResultUnknown, MAX_BATCH_FLUSH_RETRY);
    }

    /** Read only through timestamp, without decoding the SQL or building a JSON object tree. */
    private static String eventTimestamp(String row) {
        try (JsonReader reader = new JsonReader(new StringReader(row))) {
            reader.beginObject();
            while (reader.hasNext()) {
                if ("timestamp".equals(reader.nextName())) {
                    return reader.nextString();
                }
                reader.skipValue();
            }
        } catch (Throwable t) {
            return "unknown";
        }
        return "unknown";
    }

    /**
     * UTF-8 byte length of a string without encoding it into a throwaway byte array. Exactly matches
     * {@code s.getBytes(StandardCharsets.UTF_8).length}, including the JDK encoder's replacement of
     * an unpaired surrogate by a single-byte {@code '?'}, so the byte accounting stays in step with
     * what the stream load request body actually carries.
     *
     * <p>This runs on the audit-event worker thread in {@link #offerEvent}; measuring the final JSON
     * must not allocate another megabyte-scale buffer just to determine its size.
     */
    @VisibleForTesting
    static long utf8Length(String s) {
        long len = 0;
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
        int end = 0;
        int bytes = 0;
        boolean malformed = false;
        while (end < s.length()) {
            char c = s.charAt(end);
            int chars = 1;
            int width;
            if (c < 0x80) {
                width = 1;
            } else if (c < 0x800) {
                width = 2;
            } else if (Character.isHighSurrogate(c) && end + 1 < s.length()
                    && Character.isLowSurrogate(s.charAt(end + 1))) {
                width = 4;
                chars = 2;
            } else if (Character.isSurrogate(c)) {
                width = 1;
            } else {
                width = 3;
            }
            if (bytes > maxBytes - width) {
                String prefix = s.substring(0, end);
                // Preserve the old encoder's '?' replacement when a truncated prefix contains
                // an unpaired surrogate. Encode only this bounded prefix, never the discarded tail.
                return malformed ? new String(prefix.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8) : prefix;
            }
            bytes += width;
            malformed |= chars == 1 && Character.isSurrogate(c);
            end += chars;
        }
        return s;
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
