package com.starrocks.common.util.concurrent;

import java.util.concurrent.locks.ReentrantLock;

public class FairReentrantLock extends ReentrantLock {
    public FairReentrantLock() {
        super(true);
    }
}
