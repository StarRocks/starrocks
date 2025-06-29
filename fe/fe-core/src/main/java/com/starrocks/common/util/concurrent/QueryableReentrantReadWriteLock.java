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
    // threadId -> lockTime
    private final Map<Thread, Long> sharedLockThreads = new ConcurrentHashMap<>();

    AtomicLong exclusiveLockTime = new AtomicLong(-1L);

    public QueryableReentrantReadWriteLock(boolean fair) {
        super(fair);
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

        if (currThread == null) {
            lockInfoJsonObj.addProperty("isFairLock", isFair());
            JsonObject ownerInfoJsonObj = new JsonObject();
            Thread owner = getOwner();
            if (owner == null) {
                ownerInfoJsonObj.addProperty("status", "shared");
                // For performance reason, only output the stack trace and other info of oldest reader.
                ownerInfoJsonObj.add("oldestReader", getOldestSharedLockHolderInfo());
            } else {
                ownerInfoJsonObj.addProperty("status", "exclusive");
                ownerInfoJsonObj.addProperty("id", owner.getId());
                ownerInfoJsonObj.addProperty("name", owner.getName());
                ownerInfoJsonObj.add("stack",
                        LogUtil.getStackTraceToJsonArray(owner, 0,
                                Config.slow_lock_stack_trace_reserve_levels));
            }
            // append owner info
            lockInfoJsonObj.add("holderInfo", ownerInfoJsonObj);
        } else {
            JsonObject currentLockHolderJObj = new JsonObject();
            currentLockHolderJObj.addProperty("id", currThread.getId());
            currentLockHolderJObj.addProperty("name", currThread.getName());
            currentLockHolderJObj.add("stack",
                    LogUtil.getStackTraceToJsonArray(currThread, 6,
                            Config.slow_lock_stack_trace_reserve_levels));
            // append current lock holder info
            lockInfoJsonObj.add("holderInfo", currentLockHolderJObj);
        }

        // append waiters info
        lockInfoJsonObj.add("queuedReaders",
                LockChecker.getLockWaiterInfoJsonArray(getQueuedReaderThreads()));
        lockInfoJsonObj.add("queuedWriters",
                LockChecker.getLockWaiterInfoJsonArray(getQueuedWriterThreads()));

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

    private JsonObject getOldestSharedLockHolderInfo() {
        Thread oldestReaderThread = getSharedLockThreads().stream().max((a, b) ->
                (int) (computeLockHeldTime(getSharedLockStartTimeMs(a)) -
                        computeLockHeldTime(getSharedLockStartTimeMs(b)))).orElse(null);
        return oldestReaderThread == null ? new JsonObject() : getReaderInfo(
                true,
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
