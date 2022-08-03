// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common.util;

import com.starrocks.meta.MetaContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Daemon extends Thread {
    private static final Logger LOG = LogManager.getLogger(Daemon.class);
    private static final int DEFAULT_INTERVAL_SECONDS = 30; // 30 seconds

    private final Lock lock = new ReentrantLock();
    private final Condition ready = lock.newCondition();
    private final AtomicBoolean isStop;
    private final AtomicBoolean isStart = new AtomicBoolean(false);
    private long intervalMs;

    private MetaContext metaContext = null;

    {
        setDaemon(true);
    }

    public Daemon() {
        super();
        intervalMs = DEFAULT_INTERVAL_SECONDS * 1000L;
        isStop = new AtomicBoolean(false);
    }

    public Daemon(String name) {
        this(name, DEFAULT_INTERVAL_SECONDS * 1000L);
    }

    public Daemon(String name, long intervalMs) {
        this(intervalMs);
        this.setName(name);
    }

    public Daemon(long intervalMs) {
        this();
        this.intervalMs = intervalMs;
    }

    @Override
    public synchronized void start() {
        if (isStart.compareAndSet(false, true)) {
            super.start();
        }
    }

    public void setMetaContext(MetaContext metaContext) {
        this.metaContext = metaContext;
    }

    public void exit() {
        isStop.set(true);
    }

    public long getInterval() {
        return this.intervalMs;
    }

    public void setInterval(long intervalMs) {
        this.intervalMs = intervalMs;
    }

    public void wakeup() {
        lock.lock();
        try {
            this.ready.signal();
        } finally {
            lock.unlock();
        }
    }

    private void await() {
        lock.lock();
        try {
            ready.await(intervalMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {
            // do nothing
        } finally {
            lock.unlock();
        }
    }

    /**
     * implement in child
     */
    protected void runOneCycle() {

    }

    @Override
    public void run() {
        if (metaContext != null) {
            metaContext.setThreadLocalInfo();
        }

        while (!isStop.get()) {
            try {
                runOneCycle();
            } catch (Throwable e) {
                LOG.error("daemon thread got exception. name: {}", getName(), e);
            }
            await();
        }

        if (metaContext != null) {
            MetaContext.remove();
        }
        LOG.error("daemon thread exits. name=" + this.getName());
    }
}
