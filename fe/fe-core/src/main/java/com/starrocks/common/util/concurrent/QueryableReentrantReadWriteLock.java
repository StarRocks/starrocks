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

    // Per-instance throttle gates. Monotonic-clock timestamps (System.nanoTime → ms) so that
    // NTP adjustments cannot stretch or short-circuit the intervals. Both default to 0L, which
    // is "far in the past" against any normal nanoTime-derived ms value, so the first slow-lock
    // event always wins the CAS. Per-instance scope so that one chronically slow lock cannot
    // starve the throttle quota of other lock instances (the production symptom that prompted
    // these throttles in the first place).
    private final AtomicLong lastSlowLogMs = new AtomicLong(0L);
    private final AtomicLong lastStackPrintMs = new AtomicLong(0L);

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
            // Slow lock detected. Gate the diagnostic snapshot AND the warn behind a per-instance
            // time-window so a chronically slow lock cannot spam logs — getLockInfoToJson() itself
            // is expensive (it captures owner / queued-thread stacks), so we skip it when throttled
            // rather than building the JSON only to discard it.
            // NOTE: Between checking timeout and calling getLockInfoToJson(), lock state might change.
            // Current state captured may not match final acquisition state. However, this is acceptable
            // for debugging purposes (approximately accurate is good enough)
            if (shouldEmitSlowLog()) {
                currentLockInfo = this.getLockInfoToJson(null);
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
            if (currentLockInfo != null) {
                long waitTime = System.currentTimeMillis() - startTime;
                LOG.warn("slow lock detected on lock (isExclusive={}). waitTime: {}ms, lockInfo: {}",
                        isExclusive, waitTime, currentLockInfo);
            }
        } finally {
            if (interrupted) {
                // Restore the thread interrupt status
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Per-instance time-window gate for the entire slow-lock warn emit (snapshot + log). Uses a
     * monotonic clock and a CAS so concurrent emits from different threads on the same lock collapse
     * into at most one warn per {@code Config.slow_lock_log_every_ms}. Set the config to 0 (or less)
     * to disable the gate and emit every slow event.
     */
    private boolean shouldEmitSlowLog() {
        long interval = Config.slow_lock_log_every_ms;
        if (interval <= 0) {
            return true;
        }
        long monoNowMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        long last = lastSlowLogMs.get();
        return monoNowMs - last >= interval && lastSlowLogMs.compareAndSet(last, monoNowMs);
    }

    /**
     * Per-instance gate for capturing thread stacks inside {@link #getLockInfoToJson}. False when
     * {@code slow_lock_print_stack} is off; otherwise CAS-gated by
     * {@code slow_lock_stack_print_interval_ms} on a monotonic clock. The all-or-nothing decision
     * is made once per call so every stack within the same JSON snapshot is captured or omitted
     * together (avoids half-captured pictures).
     */
    private boolean shouldCaptureStack() {
        if (!Config.slow_lock_print_stack) {
            return false;
        }
        long interval = Config.slow_lock_stack_print_interval_ms;
        if (interval <= 0) {
            return true;
        }
        long monoNowMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        long last = lastStackPrintMs.get();
        return monoNowMs - last >= interval && lastStackPrintMs.compareAndSet(last, monoNowMs);
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
    public JsonObject getLockInfoToJson(Thread currThread) {
        JsonObject lockInfoJsonObj = new JsonObject();
        boolean captureStack = shouldCaptureStack();
        int waiterCap = Config.slow_lock_max_waiter_count_to_log;

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
        return oldestReaderThread == null ? new JsonObject() : getReaderInfo(
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
