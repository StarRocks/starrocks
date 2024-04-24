package com.starrocks.common.util.concurrent;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FairReentrantReadWriteLock extends ReentrantReadWriteLock {
    public FairReentrantReadWriteLock() {
        super(true);
    }
}
