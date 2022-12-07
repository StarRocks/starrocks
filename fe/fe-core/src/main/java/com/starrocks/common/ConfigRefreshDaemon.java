package com.starrocks.common;

import com.starrocks.common.util.LeaderDaemon;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;

public class ConfigRefreshDaemon extends LeaderDaemon {
    private final List<ConfigRefreshListener> listeners = new ArrayList<>();
    private final Lock lock = new ReentrantLock();

    public ConfigRefreshDaemon() {
        super("config-refresh-daemon", 10000);
    }

    @Override
    protected void runAfterCatalogReady() {
        lock.lock();
        try {
            for(ConfigRefreshListener listener : listeners) {
                listener.refresh();
            }
        } finally {
            lock.unlock();
        }
    }

    public void registerListener(ConfigRefreshListener listener) {
        lock.lock();
        try {
            listeners.add(listener);
        } finally {
            lock.unlock();
        }
    }
}
