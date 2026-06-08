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

package com.starrocks.common.util.concurrent;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.starrocks.common.Config;
import com.starrocks.common.util.LogUtil;
import com.starrocks.consistency.LockChecker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/*
 * This Lock is for exposing the getOwner() method,
 * which is a protected method of ReentrantReadWriteLock.
 * And to provide the lock information for debugging, we should
 * call the helper method like sharedLock(), exclusiveLock() instead of
 * directly calling readLock().lock(), writeLock.lock().
 */
public class QueryableReentrantReadWriteLock extends ReentrantReadWriteLock {
    private static final Logger LOG = LogManager.getLogger(QueryableReentrantReadWriteLock.class);

    // threadId -> lockTime
    private final Map<Thread, Long> sharedLockThreads = new ConcurrentHashMap<>();

    AtomicLong exclusiveLockTime = new AtomicLong(-1L);

    // Per-instance throttle gates for the three slow-lock log tiers (see SlowLockLogDecision).
    // Monotonic-clock timestamps (System.nanoTime → ms) so NTP adjustments cannot stretch or
    // short-circuit the intervals. Per-instance scope so one chronically slow lock cannot starve
    // the throttle quota of other lock instances (the production symptom that prompted these
    // throttles). Initialized to the shared GATE_INIT_SENTINEL ("sufficiently in the past") so the
    // first event always wins regardless of where System.nanoTime() anchors its origin.
    private final AtomicLong lastStackPrintMs = new AtomicLong(SlowLockLogDecision.GATE_INIT_SENTINEL); // L1
    private final AtomicLong lastSlowLogMs = new AtomicLong(SlowLockLogDecision.GATE_INIT_SENTINEL);    // L2
    private final AtomicLong lastBriefMs = new AtomicLong(SlowLockLogDecision.GATE_INIT_SENTINEL); // L3

    public QueryableReentrantReadWriteLock() {
        super();
    }

    public QueryableReentrantReadWriteLock(boolean fair) {
        super(fair);
    }

    public void sharedLockDetectingSlowLock(long slowLockThreshold, TimeUnit unit) {
        lockDetectingSlowLock(slowLockThreshold, unit, false);
    }

    public void sharedLock() {
        this.readLock().lock();
        this.sharedLockThreads.put(Thread.currentThread(), System.currentTimeMillis());
    }

    public boolean trySharedLock(long timeout, TimeUnit unit) throws InterruptedException {
        boolean result = this.readLock().tryLock(timeout, unit);
        if (result) {
            this.sharedLockThreads.put(Thread.currentThread(), System.currentTimeMillis());
        }
        return result;
    }

    public void sharedUnlock() {
        this.readLock().unlock();
        this.sharedLockThreads.remove(Thread.currentThread());
    }

    public void exclusiveLockDetectingSlowLock(long slowLockThreshold, TimeUnit unit) {
        lockDetectingSlowLock(slowLockThreshold, unit, true);
    }

    private void lockDetectingSlowLock(long slowLockThreshold, TimeUnit unit, boolean isExclusive) {
        JsonObject currentLockInfo = null;
        boolean interrupted = false;
        long startTime = System.currentTimeMillis();
        SlowLockLogDecision decision = SlowLockLogDecision.SUPPRESS;
        try {
            if (isExclusive) {
                if (this.tryExclusiveLock(slowLockThreshold, unit)) {
                    return;
                }
            } else {
                if (this.trySharedLock(slowLockThreshold, unit)) {
                    return;
                }
            }
            // Slow lock detected. Make the tier decision exactly once (this is the only place the
            // per-instance throttle gates are touched), then render accordingly:
            //   L1/L2 -> capture a lock-info snapshot now (state may shift slightly before we
            //            re-acquire; acceptable for diagnostics),
            //   L3    -> emit only a plain-text brief (built below, no snapshot),
            //   SUPPRESS -> stay silent.
            decision = decideSlowLockLog(hasHolder());
            if (decision.shouldLog() && !decision.isBrief()) {
                currentLockInfo = this.getLockInfoToJson(null, decision);
            }
        } catch (InterruptedException exception) {
            interrupted = true;
        }
        try {
            if (isExclusive) {
                this.exclusiveLock();
            } else {
                this.sharedLock();
            }
            if (decision.shouldLog()) {
                long waitTime = System.currentTimeMillis() - startTime;
                if (decision.isBrief()) {
                    // Floor tier: leave evidence that a slow lock happened without the expensive
                    // JSON/stack payload.
                    LOG.warn("slow lock (throttled detail) detected on lock (isExclusive={}). waitTime: {}ms",
                            isExclusive, waitTime);
                } else {
                    LOG.warn("slow lock detected on lock (isExclusive={}). waitTime: {}ms, lockInfo: {}",
                            isExclusive, waitTime, currentLockInfo);
                }
            }
        } finally {
            if (interrupted) {
                // Restore the thread interrupt status
                Thread.currentThread().interrupt();
            }
        }
    }

    /** Whether this lock currently has an exclusive owner or any shared reader to dump a stack for. */
    boolean hasHolder() {
        return getOwner() != null || !sharedLockThreads.isEmpty();
    }

    /**
     * Make the single per-event slow-lock tier decision for this lock instance, consuming the
     * per-instance throttle gates. See {@link SlowLockLogDecision#decide}.
     */
    SlowLockLogDecision decideSlowLockLog(boolean hasHolder) {
        long monoNowMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        return SlowLockLogDecision.decide(hasHolder, lastStackPrintMs, lastSlowLogMs, lastBriefMs, monoNowMs);
    }

    public void exclusiveLock() {
        this.writeLock().lock();
        this.exclusiveLockTime.set(System.currentTimeMillis());
    }

    public boolean tryExclusiveLock(long timeout, TimeUnit unit) throws InterruptedException {
        boolean result = this.writeLock().tryLock(timeout, unit);
        if (result) {
            this.exclusiveLockTime.set(System.currentTimeMillis());
        }
        return result;
    }

    public void exclusiveUnlock() {
        this.writeLock().unlock();
        this.exclusiveLockTime.set(-1L);
    }

    @Override
    public Thread getOwner() {
        return super.getOwner();
    }

    public List<Long> getSharedLockThreadIds() {
        return sharedLockThreads.keySet().stream().map(Thread::getId).collect(Collectors.toList());
    }

    public Set<Thread> getSharedLockThreads() {
        return sharedLockThreads.keySet();
    }

    public long getSharedLockStartTimeMs(Thread thread) {
        return sharedLockThreads.getOrDefault(thread, -1L);
    }

    public long getExclusiveLockStartTimeMs() {
        return exclusiveLockTime.get();
    }

    @Override
    public Collection<Thread> getQueuedThreads() {
        return super.getQueuedThreads();
    }

    @Override
    public Collection<Thread> getQueuedReaderThreads() {
        return super.getQueuedReaderThreads();
    }

    @Override
    public Collection<Thread> getQueuedWriterThreads() {
        return super.getQueuedWriterThreads();
    }

    public boolean isWriteLockHeldByCurrentThread() {
        return this.writeLock().isHeldByCurrentThread();
    }

    public boolean isReadLockHeldByCurrentThread() {
        return this.getReadHoldCount() > 0;
    }

    private void appendQueuedInfo(StringBuilder sb, List<Long> queuedReaders, List<Long> queuedWriters) {
        sb.append(queuedReaders.size()).append(" queued reader(s): ").append(queuedReaders).append(", ")
                .append(queuedWriters.size()).append(" queued writer(s): ").append(queuedWriters);
    }

    // The following API generate debug info for {@link QueryableReentrantReadWriteLock}
    // which is quite helpful when we met slow lock or deadlock issue.

    /**
     * Get the lock information, includes: isFair, owner name, owner id, queued readers, queued writers,
     * owner or current thread stack trace.
     *
     * @param currThread the thread on which we want to dump the stack,
     *                   if it's null, we dump the owner thread(if exists) of this lock
     * @return The lock information
     */
    public JsonObject getLockInfoToJson(Thread currThread, SlowLockLogDecision decision) {
        JsonObject lockInfoJsonObj = new JsonObject();
        int waiterCap = Config.slow_lock_max_waiter_count_to_log;

        // The stack-capture decision was made once by the caller (decideSlowLockLog) and is read
        // here as decision.captureStack — this method never touches the throttle gates itself, so a
        // caller can pass SUPPRESS to get a cheap, stack-less structural snapshot without consuming
        // any quota.
        boolean captureStack = decision.captureStack;
        if (currThread == null) {
            lockInfoJsonObj.addProperty("isFairLock", isFair());
            JsonObject ownerInfoJsonObj = new JsonObject();
            Thread owner = getOwner();
            if (owner == null) {
                ownerInfoJsonObj.addProperty("status", "shared");
                // For performance reason, only output the stack trace and other info of oldest reader.
                ownerInfoJsonObj.add("oldestReader", getOldestSharedLockHolderInfo(captureStack));
            } else {
                ownerInfoJsonObj.addProperty("status", "exclusive");
                ownerInfoJsonObj.addProperty("id", owner.getId());
                ownerInfoJsonObj.addProperty("name", owner.getName());
                if (captureStack) {
                    ownerInfoJsonObj.add("stack",
                            LogUtil.getStackTraceToJsonArray(owner, 0,
                                    Config.slow_lock_stack_trace_reserve_levels));
                }
            }
            // append owner info
            lockInfoJsonObj.add("holderInfo", ownerInfoJsonObj);
        } else {
            JsonObject currentLockHolderJObj = new JsonObject();
            currentLockHolderJObj.addProperty("id", currThread.getId());
            currentLockHolderJObj.addProperty("name", currThread.getName());
            if (captureStack) {
                currentLockHolderJObj.add("stack",
                        LogUtil.getStackTraceToJsonArray(currThread, 6,
                                Config.slow_lock_stack_trace_reserve_levels));
            }
            // append current lock holder info
            lockInfoJsonObj.add("holderInfo", currentLockHolderJObj);
        }

        // append waiters info, capped by slow_lock_max_waiter_count_to_log to bound
        // log line size under extreme contention; remainder is summarized as a trailer.
        lockInfoJsonObj.add("queuedReaders",
                LockChecker.getLockWaiterInfoJsonArray(getQueuedReaderThreads(), waiterCap));
        lockInfoJsonObj.add("queuedWriters",
                LockChecker.getLockWaiterInfoJsonArray(getQueuedWriterThreads(), waiterCap));

        return lockInfoJsonObj;
    }

    public JsonArray getCurrReadersInfoToJsonArray(boolean onlyLogSlow, boolean dumpStack, int reserveLevels) {
        JsonArray readerInfos = new JsonArray();
        for (Map.Entry<Thread, Long> entry : sharedLockThreads.entrySet()) {
            Thread thread = entry.getKey();
            long lockStartTimeMs = entry.getValue();
            long lockHeldTimeMs = computeLockHeldTime(lockStartTimeMs);
            if (!onlyLogSlow || lockHeldTimeMs > Config.slow_lock_threshold_ms) {
                readerInfos.add(getReaderInfo(dumpStack, thread, lockHeldTimeMs, reserveLevels));
            }
        }

        return readerInfos;
    }

    private JsonObject getOldestSharedLockHolderInfo(boolean captureStack) {
        Thread oldestReaderThread = getSharedLockThreads().stream().max((a, b) ->
                (int) (computeLockHeldTime(getSharedLockStartTimeMs(a)) -
                        computeLockHeldTime(getSharedLockStartTimeMs(b)))).orElse(null);
        if (oldestReaderThread == null) {
            return new JsonObject();
        }
        return getReaderInfo(
                captureStack,
                oldestReaderThread,
                computeLockHeldTime(getSharedLockStartTimeMs(oldestReaderThread)),
                Config.slow_lock_stack_trace_reserve_levels);
    }

    private JsonObject getReaderInfo(boolean dumpStack, Thread thread, long lockHeldTimeMs, int reserveLevels) {
        JsonObject readerInfo = new JsonObject();
        readerInfo.addProperty("id", thread.getId());
        readerInfo.addProperty("name", thread.getName());
        readerInfo.addProperty("heldFor", lockHeldTimeMs + "ms");
        if (dumpStack) {
            readerInfo.add("stack",
                    LogUtil.getStackTraceToJsonArray(thread, 0, reserveLevels));
        }

        return readerInfo;
    }

    public static long computeLockHeldTime(long startTimeMs) {
        long result = -1L;
        if (startTimeMs > 0L) {
            result = System.currentTimeMillis() - startTimeMs;
        }

        return result;
    }
}
