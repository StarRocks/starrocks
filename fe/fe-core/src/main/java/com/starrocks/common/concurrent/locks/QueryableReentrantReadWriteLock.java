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


package com.starrocks.common.concurrent.locks;

import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/*
 * This Lock is for exposing the getOwner() method,
 * which is a protected method of ReentrantReadWriteLock
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
        boolean succ = this.readLock().tryLock(timeout, unit);
        if (succ) {
            this.sharedLockThreads.put(Thread.currentThread().getId(), System.currentTimeMillis());
        }
        return succ;
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
        boolean succ = this.writeLock().tryLock(timeout, unit);
        if (succ) {
            this.exclusiveLockTime.set(System.currentTimeMillis());
        }
        return succ;
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
}
