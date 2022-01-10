// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.starrocks.analysis.AlterWorkGroupStmt;
import com.starrocks.analysis.CreateWorkGroupStmt;
import com.starrocks.analysis.DropWorkGroupStmt;
import com.starrocks.persist.WorkGroupEntry;
import com.starrocks.thrift.TWorkGroup;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
// WorkGroupMgr is employed by Catalog to manage WorkGroup in FE.
public class WorkGroupMgr {
    private Catalog catalog;
    private Map<String, WorkGroup> workGroupMap = new HashMap<>();
    private Map<Long, WorkGroup> id2WorkGroupMap = new HashMap<>();
    private Map<Long, WorkGroupClassifier> classifierMap = new HashMap<>();
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    WorkGroupMgr(Catalog catalog) {
        this.catalog = catalog;
    }

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    public void createWorkGroup(CreateWorkGroupStmt stmt) {
        writeLock();
        try {
            WorkGroup wg = stmt.getWorkgroup();
            wg.setId(catalog.getCurrentCatalog().getNextId());
            for (WorkGroupClassifier classifier : wg.getClassifiers()) {
                classifier.setWorkgroupId(wg.getId());
                classifier.setId(catalog.getCurrentCatalog().getNextId());
                classifierMap.put(classifier.getId(), classifier);
            }
            workGroupMap.put(wg.getName(), wg);
            id2WorkGroupMap.put(wg.getId(), wg);
            catalog.getEditLog().logCreateWorkGroup(new WorkGroupEntry(wg.toThrift()));
        } finally {
            writeUnlock();
        }
    }

    public void alterWorkGroup(AlterWorkGroupStmt stmt) {

    }

    public void dropWorkGroup(DropWorkGroupStmt stmt) {

    }

    public void replayCreateWorkGroup(WorkGroupEntry entry) {
        writeLock();
        try {
            addWorkGroupInternal(new WorkGroup(entry.getWorkGroup()));
        } finally {
            writeUnlock();
        }
    }

    public void replayAlterWorkGroup(WorkGroupEntry entry) {
        TWorkGroup twg = entry.getWorkGroup();
        writeLock();
        try {
            removeWorkGroupInternal(twg.getName());
            addWorkGroupInternal(new WorkGroup(twg));
        } finally {
            writeUnlock();
        }
    }

    public void replayDropWorkGroup(WorkGroupEntry entry) {
        TWorkGroup twg = entry.getWorkGroup();
        String name = twg.getName();
        writeLock();
        try {
            removeWorkGroupInternal(name);
        } finally {
            writeUnlock();
        }
    }

    private void removeWorkGroupInternal(String name) {
        WorkGroup wg = workGroupMap.remove(name);
        id2WorkGroupMap.remove(wg.getId());
        for (WorkGroupClassifier classifier : wg.classifiers) {
            classifierMap.remove(classifier.getId());
        }
    }

    private void addWorkGroupInternal(WorkGroup wg) {
        workGroupMap.put(wg.getName(), wg);
        id2WorkGroupMap.put(wg.getId(), wg);
        for (WorkGroupClassifier classifier : wg.classifiers) {
            classifierMap.put(classifier.getId(), classifier);
        }
    }
}
