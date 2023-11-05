// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.common.util;

import java.util.Collection;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/*
 * This Lock is for exposing the getOwner() method,
 * which is a protected method of ReentrantReadWriteLock
 */
public class QueryableReentrantReadWriteLock extends ReentrantReadWriteLock {

    public QueryableReentrantReadWriteLock(boolean fair) {
        super(fair);
    }

    @Override
    public Thread getOwner() {
        return super.getOwner();
    }

    @Override
    public Collection<Thread> getQueuedThreads() {
        return super.getQueuedThreads();
    }
}
