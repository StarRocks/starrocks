// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load.lock;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class LevelLockUtils {
    public static boolean tryLockLevelLocks(List<LevelLock> locks) {
        // sort lock
        sortLocks(locks);
        // try to acquire locks
        List<LevelLock> acquiredLocks = Lists.newArrayList();
        boolean ret = false;
        for (LevelLock lock : locks) {
            if (lock == null) {
                continue;
            }
            ret = lock.tryLock();
            if (!ret) {
                break;
            }
            acquiredLocks.add(lock);
        }
        if (!ret) {
            for (LevelLock lock : acquiredLocks) {
                lock.unlock();
            }
        }
        return ret;
    }

    public static void unlockLevelLocks(List<LevelLock> locks) {
        for (LevelLock lock : locks) {
            if (lock == null) {
                continue;
            }
            lock.unlock();
        }
    }

    private static void sortLocks(List<LevelLock> levelLocks) {
        Collections.sort(levelLocks, new Comparator<LevelLock>() {
            @Override
            public int compare(LevelLock o1, LevelLock o2) {
                if (o1 == o2) {
                    return 0;
                }
                if (o1 == null && o2 == null) {
                    return 0;
                } else if (o1 == null) {
                    return -1;
                } else if (o2 == null) {
                    return 1;
                }
                int cmp = 0;
                int size = Math.min(o1.getPathIds().length, o2.getPathIds().length);
                for (int i = 0; i < size; ++i) {
                    cmp = o1.getPathIds()[i].compareTo(o2.getPathIds()[i]);
                    if (cmp != 0) {
                        return cmp;
                    }
                }
                cmp = o1.getPathIds().length - o2.getPathIds().length;
                if (cmp == 0) {
                    if (o1.isExclusive() == o2.isExclusive()) {
                        return cmp;
                    }
                    if (o1.isExclusive()) {
                        return -1;
                    }
                    return 1;
                }
                return cmp;
            }
        });
    }
}
