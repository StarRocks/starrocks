// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.common.util;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
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

    public List<ThreadInfo> getSharedLockThreads() {
        List<ThreadInfo> threadInfos = new ArrayList<>();
        for (long sharedLockThreadId : sharedLockThreadIds.keySet()){
            ThreadMXBean tmx =  ManagementFactory.getThreadMXBean();
            if (tmx != null) {
                ThreadInfo threadInfo = tmx.getThreadInfo(sharedLockThreadId);
                if (threadInfo != null) {
                    threadInfos.add(threadInfo);
                }
            }
        }
        return threadInfos;
    }

    @Override
    public Collection<Thread> getQueuedThreads() {
        return super.getQueuedThreads();
    }
}
