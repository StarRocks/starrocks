// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.alter;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.MaterializedView;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// a manager for materialized view
// it has the following functions:
// 1. manage the relations between manterialized view and base tables
public class MaterializedViewManager {
    // mv name -> materialized view
    private Map<String, MaterializedView> materializedViews;

    // table id -> mv set
    // every table that has mvs will be recorded here
    private Map<Long, Set<MaterializedView>> materializedViewsForTable;

    private ReentrantReadWriteLock lock;

    public MaterializedViewManager() {
        this.materializedViews = Maps.newHashMap();
        this.materializedViewsForTable = Maps.newHashMap();
        this.lock = new ReentrantReadWriteLock();
    }

    // return true for registering successfully, or return false
    public boolean registerMaterializedView(MaterializedView mv) {
        lock.writeLock().lock();
        try {
            if (materializedViews.containsKey(mv.getName())) {
                return false;
            }
            materializedViews.put(mv.getName(), mv);
            List<Long> baseTableIds = mv.getBaseTableIds();
            for (Long baseTableId : baseTableIds) {
                Set<MaterializedView> mvSet = materializedViewsForTable.getOrDefault(baseTableId, Sets.newHashSet());
                mvSet.add(mv);
                materializedViewsForTable.put(baseTableId, mvSet);
            }
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void deregisterMaterializedView(MaterializedView mv) {
        lock.writeLock().lock();
        try {
            materializedViews.remove(mv.getName());
            List<Long> baseTableIds = mv.getBaseTableIds();
            for (Long baseTableId : baseTableIds) {
                Set<MaterializedView> mvSet = materializedViewsForTable.get(baseTableId);
                if (mvSet != null) {
                    mvSet.remove(mv);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean containMv(String mvName) {
        lock.readLock().lock();
        try {
            return materializedViews.containsKey(mvName);
        } finally {
            lock.readLock().unlock();
        }
    }

    // get materialized view for the specific name
    public MaterializedView getMaterializedView(String mvName) {
        lock.readLock().lock();
        try {
            return materializedViews.get(mvName);
        } finally {
            lock.readLock().unlock();
        }
    }

    public Set<MaterializedView> getMaterializedViewSetForTableId(long tableId) {
        lock.readLock().lock();
        try {
            return materializedViewsForTable.getOrDefault(tableId, null);
        } finally {
            lock.readLock().unlock();
        }
    }
}
