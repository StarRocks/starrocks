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
    Map<Long, Boolean> sharedLockThreadIds = new ConcurrentHashMap<>();

    public QueryableReentrantReadWriteLock(boolean fair) {
        super(fair);
    }

    public void sharedLock() {
        this.readLock().lock();
        sharedLockThreadIds.put(Thread.currentThread().getId(), true);
    }

    public boolean trySharedLock(long timeout, TimeUnit unit) throws InterruptedException {
        boolean succ = this.readLock().tryLock(timeout, unit);
        sharedLockThreadIds.put(Thread.currentThread().getId(), true);
        return succ;
    }

    public void sharedUnlock() {
        this.readLock().unlock();
        sharedLockThreadIds.remove(Thread.currentThread().getId());
    }

    @Override
    public Thread getOwner() {
        return super.getOwner();
    }

    public List<Long> getSharedLockThreads() {
        return Lists.newArrayList(sharedLockThreadIds.keySet());
    }

    @Override
    public Collection<Thread> getQueuedThreads() {
        return super.getQueuedThreads();
    }
}
