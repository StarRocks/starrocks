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

import com.google.common.collect.Lists;
import com.starrocks.common.util.LogUtil;

import java.util.Collection;
import java.util.List;
import java.util.Map;
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
    Map<Long, Long> sharedLockThreads = new ConcurrentHashMap<>();

    AtomicLong exclusiveLockTime = new AtomicLong(-1L);

    public QueryableReentrantReadWriteLock(boolean fair) {
        super(fair);
    }

    public void sharedLock() {
        this.readLock().lock();
        this.sharedLockThreads.put(Thread.currentThread().getId(), System.currentTimeMillis());
    }

    public boolean trySharedLock(long timeout, TimeUnit unit) throws InterruptedException {
        boolean result = this.readLock().tryLock(timeout, unit);
        if (result) {
            this.sharedLockThreads.put(Thread.currentThread().getId(), System.currentTimeMillis());
        }
        return result;
    }

    public void sharedUnlock() {
        this.readLock().unlock();
        this.sharedLockThreads.remove(Thread.currentThread().getId());
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
        return Lists.newArrayList(sharedLockThreads.keySet());
    }

    public long getSharedLockTime(long threadId) {
        return sharedLockThreads.getOrDefault(threadId, -1L);
    }

    public long getExclusiveLockTime() {
        return exclusiveLockTime.get();
    }

    @Override
    public Collection<Thread> getQueuedThreads() {
        return super.getQueuedThreads();
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

    /**
     * Get the lock information, includes: isFair, owner name, owner id, queued readers, queued writers,
     * owner or current thread stack trace.
     *
     * @param currThread the thread on which we want to dump the stack,
     *              if it's null, we dump the owner thread(if exists) of this lock
     * @return The lock information
     */
    private String getLockInfo(Thread currThread) {
        StringBuilder sb = new StringBuilder();
        List<Long> queuedReaders = getQueuedReaderThreads().stream().map(Thread::getId).collect(Collectors.toList());
        List<Long> queuedWriters = getQueuedWriterThreads().stream().map(Thread::getId).collect(Collectors.toList());
        if (currThread == null) {
            sb.append("isFair: ").append(isFair()).append(", ");
            Thread owner = getOwner();
            if (owner == null) {
                sb.append("no owner(writer), ").append(getReadLockCount()).append(" reader(s) holding lock, ");
                appendQueuedInfo(sb, queuedReaders, queuedWriters);
            } else {
                sb.append("lock owner id: ").append(owner.getId())
                        .append(", name: ").append(owner.getName()).append(", ");
                appendQueuedInfo(sb, queuedReaders, queuedWriters);
                sb.append(", owner stack: ")
                        .append(LogUtil.getStackTraceToList(owner, 0, 15));
            }
        } else {
            appendQueuedInfo(sb, queuedReaders, queuedWriters);
            sb.append(", ").append("current thread id: ").append(currThread.getId())
                    .append(", name: ").append(currThread.getName()).append(", ")
                    .append("thread stack: ")
                    .append(LogUtil.getCurrentStackTraceToList(5, 10));
        }

        return sb.toString();
    }

    public String getLockInfoWithOwnerStack() {
        return getLockInfo(null);
    }

    public String getLockInfoWithCurrStack() {
        return getLockInfo(Thread.currentThread());
    }
}
