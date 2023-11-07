// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.common.util;

import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/*
 * This Lock is for exposing the getOwner() method,
 * which is a protected method of ReentrantReadWriteLock
 */
public class QueryableReentrantReadWriteLock extends ReentrantReadWriteLock {
    // threadId -> lockTime
    Map<Long, Long> sharedLockThreads = new ConcurrentHashMap<>();

    public QueryableReentrantReadWriteLock(boolean fair) {
        super(fair);
    }

    public void sharedLock() {
        this.readLock().lock();
        sharedLockThreads.put(Thread.currentThread().getId(), System.currentTimeMillis());
    }

    public boolean trySharedLock(long timeout, TimeUnit unit) throws InterruptedException {
        boolean succ = this.readLock().tryLock(timeout, unit);
        sharedLockThreads.put(Thread.currentThread().getId(), System.currentTimeMillis());
        return succ;
    }

    public void sharedUnlock() {
        this.readLock().unlock();
        sharedLockThreads.remove(Thread.currentThread().getId());
    }

    @Override
    public Thread getOwner() {
        return super.getOwner();
    }

    public List<Long> getSharedLockThreadIds() {
        return Lists.newArrayList(sharedLockThreads.keySet());
    }

    public long getSharedLockHoldTime(long threadId) {
        return sharedLockThreads.getOrDefault(threadId, -1L);
    }

    @Override
    public Collection<Thread> getQueuedThreads() {
        return super.getQueuedThreads();
    }
}
