// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.iceberg;

import com.starrocks.external.iceberg.cost.IcebergMetaCache;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class IcebergRepository {
    private IcebergMetaCache icebergMetaCache;
    ReadWriteLock metaCacheLock = new ReentrantReadWriteLock();

    Executor executor = Executors.newFixedThreadPool(50);

    public IcebergMetaCache getMetaCache() {
        IcebergMetaCache metaCache;
        metaCacheLock.readLock().lock();
        try {
            metaCache = icebergMetaCache;
        } finally {
            metaCacheLock.readLock().unlock();
        }
        if (metaCache != null) {
            return metaCache;
        }

        metaCacheLock.writeLock().lock();
        try {
            if (icebergMetaCache != null) {
                return icebergMetaCache;
            }

            icebergMetaCache = new IcebergMetaCache(executor);
            return icebergMetaCache;
        } finally {
            metaCacheLock.writeLock().unlock();
        }
    }

    public void clearCache() {
        metaCacheLock.writeLock().lock();
        try {
            icebergMetaCache = null;
        } finally {
            metaCacheLock.writeLock().unlock();
        }
    }
}
