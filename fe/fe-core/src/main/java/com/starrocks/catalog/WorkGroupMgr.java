// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.AlterWorkGroupStmt;
import com.starrocks.analysis.CreateWorkGroupStmt;
import com.starrocks.analysis.DropWorkGroupStmt;
import com.starrocks.analysis.ShowWorkGroupStmt;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.WorkGroupOpEntry;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TWorkGroup;
import com.starrocks.thrift.TWorkGroupOp;
import com.starrocks.thrift.TWorkGroupOpType;
import com.starrocks.thrift.TWorkGroupType;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

// WorkGroupMgr is employed by Catalog to manage WorkGroup in FE.
public class WorkGroupMgr implements Writable {
    private Catalog catalog;
    private Map<String, WorkGroup> workGroupMap = new HashMap<>();
    private Map<Long, WorkGroup> id2WorkGroupMap = new HashMap<>();
    private Map<Long, WorkGroupClassifier> classifierMap = new HashMap<>();
    private List<TWorkGroupOp> workGroupOps = new ArrayList<>();
    private Map<Long, Map<Long, TWorkGroup>> activeWorkGroupsPerBe = new HashMap<>();
    private Map<Long, Long> minVersionPerBe = new HashMap<>();
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

    public void createWorkGroup(CreateWorkGroupStmt stmt) throws DdlException {
        writeLock();
        try {
            WorkGroup wg = stmt.getWorkgroup();
            if (workGroupMap.containsKey(wg.getName())) {
                // create resource_group or replace <name> ...
                if (stmt.isReplaceIfExists()) {
                    dropWorkGroupUnlocked(wg.getName());
                } else if (!stmt.isIfNotExists()) {
                    throw new DdlException(String.format("RESOURCE_GROUP(%s) already exists", wg.getName()));
                } else {
                    return;
                }
            }
            wg.setId(catalog.getCurrentCatalog().getNextId());
            for (WorkGroupClassifier classifier : wg.getClassifiers()) {
                classifier.setWorkgroupId(wg.getId());
                classifier.setId(catalog.getCurrentCatalog().getNextId());
                classifierMap.put(classifier.getId(), classifier);
            }
            workGroupMap.put(wg.getName(), wg);
            id2WorkGroupMap.put(wg.getId(), wg);
            wg.setVersion(wg.getId());
            WorkGroupOpEntry workGroupOp = new WorkGroupOpEntry(TWorkGroupOpType.WORKGROUP_OP_CREATE, wg);
            catalog.getCurrentCatalog().getEditLog().logWorkGroupOp(workGroupOp);
            workGroupOps.add(workGroupOp.toThrift());
        } finally {
            writeUnlock();
        }
    }

    public List<List<String>> showWorkGroup(ShowWorkGroupStmt stmt) {
        List<List<String>> rows;
        if (stmt.getName() != null) {
            rows = Catalog.getCurrentCatalog().getWorkGroupMgr().showOneWorkGroup(stmt.getName());
        } else {
            rows = Catalog.getCurrentCatalog().getWorkGroupMgr().showAllWorkGroups(stmt.isListAll());
        }
        return rows;
    }

    public List<List<String>> showAllWorkGroups(Boolean isListAll) {
        readLock();
        try {
            List<WorkGroup> workGroupList = new ArrayList<>(workGroupMap.values());
            if (isListAll || ConnectContext.get() == null) {
                workGroupList.sort(Comparator.comparing(WorkGroup::getName));
                return workGroupList.stream().map(WorkGroup::show)
                        .flatMap(Collection::stream).collect(Collectors.toList());
            } else {
                String qualifiedUser = ConnectContext.get().getCurrentUserIdentity().getQualifiedUser();
                //default_cluster:test
                String[] userParts = qualifiedUser.split(":");
                String user = userParts[userParts.length - 1];

                String roleName = null;
                String qualifiedRoleName = Catalog.getCurrentCatalog().getAuth()
                        .getRoleName(ConnectContext.get().getCurrentUserIdentity());
                if (qualifiedRoleName != null) {
                    //default_cluster:role
                    String[] roleParts = qualifiedRoleName.split(":");
                    roleName = roleParts[roleParts.length - 1];
                }
                String role = roleName;
                String remoteIp = ConnectContext.get().getRemoteIP();
                return workGroupList.stream().map(w -> w.showVisible(user, role, remoteIp))
                        .flatMap(Collection::stream).collect(Collectors.toList());
            }
        } finally {
            readUnlock();
        }
    }

    public List<List<String>> showOneWorkGroup(String name) {
        readLock();
        try {
            if (!workGroupMap.containsKey(name)) {
                return Collections.emptyList();
            } else {
                return workGroupMap.get(name).show();
            }
        } finally {
            readUnlock();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        List<WorkGroup> workGroups = workGroupMap.values().stream().collect(Collectors.toList());
        SerializeData data = new SerializeData();
        data.workGroups = workGroups;

        String s = GsonUtils.GSON.toJson(data);
        Text.writeString(out, s);
    }

    public void readFields(DataInputStream dis) throws IOException {
        String s = Text.readString(dis);
        SerializeData data = GsonUtils.GSON.fromJson(s, SerializeData.class);
        if (null != data && null != data.workGroups) {
            data.workGroups.sort(Comparator.comparing(WorkGroup::getVersion));
            for (WorkGroup workgroup : data.workGroups) {
                replayAddWorkGroup(workgroup);
            }
        }
    }

    private void replayAddWorkGroup(WorkGroup workgroup) {
        addWorkGroupInternal(workgroup);
        WorkGroupOpEntry op = new WorkGroupOpEntry(TWorkGroupOpType.WORKGROUP_OP_CREATE, workgroup);
        workGroupOps.add(op.toThrift());
    }

    public WorkGroup getWorkGroup(String name) {
        readLock();
        try {
            if (workGroupMap.containsKey(name)) {
                return workGroupMap.get(name);
            } else {
                return null;
            }
        } finally {
            readUnlock();
        }
    }

    public void alterWorkGroup(AlterWorkGroupStmt stmt) throws DdlException {
        writeLock();
        try {
            String name = stmt.getName();
            if (!workGroupMap.containsKey(name)) {
                throw new DdlException("RESOURCE_GROUP(" + name + ") does not exist");
            }
            WorkGroup wg = workGroupMap.get(name);
            AlterWorkGroupStmt.SubCommand cmd = stmt.getCmd();
            if (cmd instanceof AlterWorkGroupStmt.AddClassifiers) {
                List<WorkGroupClassifier> newAddedClassifiers = stmt.getNewAddedClassifiers();
                for (WorkGroupClassifier classifier : newAddedClassifiers) {
                    classifier.setWorkgroupId(wg.getId());
                    classifier.setId(catalog.getCurrentCatalog().getNextId());
                    classifierMap.put(classifier.getId(), classifier);
                }
                wg.getClassifiers().addAll(newAddedClassifiers);
            } else if (cmd instanceof AlterWorkGroupStmt.AlterProperties) {
                WorkGroup changedProperties = stmt.getChangedProperties();
                Integer cpuCoreLimit = changedProperties.getCpuCoreLimit();
                if (cpuCoreLimit != null) {
                    wg.setCpuCoreLimit(cpuCoreLimit);
                }
                Double memLimit = changedProperties.getMemLimit();
                if (memLimit != null) {
                    wg.setMemLimit(memLimit);
                }
                Integer concurrentLimit = changedProperties.getConcurrencyLimit();
                if (concurrentLimit != null) {
                    wg.setConcurrencyLimit(concurrentLimit);
                }
                TWorkGroupType workGroupType = changedProperties.getWorkGroupType();
                if (workGroupType != null) {
                    wg.setWorkGroupType(workGroupType);
                }
            } else if (cmd instanceof AlterWorkGroupStmt.DropClassifiers) {
                Set<Long> classifierToDrop = stmt.getClassifiersToDrop().stream().collect(Collectors.toSet());
                Iterator<WorkGroupClassifier> classifierIterator = wg.getClassifiers().iterator();
                while (classifierIterator.hasNext()) {
                    if (classifierToDrop.contains(classifierIterator.next().getId())) {
                        classifierIterator.remove();
                    }
                }
                for (Long classifierId : classifierToDrop) {
                    classifierMap.remove(classifierId);
                }
            } else if (cmd instanceof AlterWorkGroupStmt.DropAllClassifiers) {
                List<WorkGroupClassifier> classifierList = wg.getClassifiers();
                for (WorkGroupClassifier classifier : classifierList) {
                    classifierMap.remove(classifier.getId());
                }
                classifierList.clear();
            }
            // only when changing properties, version is required to update. because changing classifiers needs not
            // propagate to BE.
            if (cmd instanceof AlterWorkGroupStmt.AlterProperties) {
                wg.setVersion(catalog.getCurrentCatalog().getNextId());
            }
            WorkGroupOpEntry workGroupOp = new WorkGroupOpEntry(TWorkGroupOpType.WORKGROUP_OP_ALTER, wg);
            catalog.getCurrentCatalog().getEditLog().logWorkGroupOp(workGroupOp);
            workGroupOps.add(workGroupOp.toThrift());
        } finally {
            writeUnlock();
        }
    }

    public void dropWorkGroup(DropWorkGroupStmt stmt) throws DdlException {
        writeLock();
        try {
            String name = stmt.getName();
            if (!workGroupMap.containsKey(name)) {
                throw new DdlException("RESOURCE_GROUP(" + name + ") does not exist");
            }
            dropWorkGroupUnlocked(name);
        } finally {
            writeUnlock();
        }
    }

    public void dropWorkGroupUnlocked(String name) {
        WorkGroup wg = workGroupMap.get(name);
        removeWorkGroupInternal(name);
        wg.setVersion(catalog.getCurrentCatalog().getNextId());
        WorkGroupOpEntry workGroupOp = new WorkGroupOpEntry(TWorkGroupOpType.WORKGROUP_OP_DELETE, wg);
        catalog.getCurrentCatalog().getEditLog().logWorkGroupOp(workGroupOp);
        workGroupOps.add(workGroupOp.toThrift());
    }

    public void replayWorkGroupOp(WorkGroupOpEntry entry) {
        writeLock();
        try {
            WorkGroup workgroup = entry.getWorkgroup();
            TWorkGroupOpType opType = entry.getOpType();
            switch (opType) {
                case WORKGROUP_OP_CREATE:
                    addWorkGroupInternal(workgroup);
                    break;
                case WORKGROUP_OP_DELETE:
                    removeWorkGroupInternal(workgroup.getName());
                    break;
                case WORKGROUP_OP_ALTER:
                    removeWorkGroupInternal(workgroup.getName());
                    addWorkGroupInternal(workgroup);
                    break;
            }
            workGroupOps.add(entry.toThrift());
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

    public List<TWorkGroupOp> getWorkGroupsNeedToDeliver(Long beId) {
        readLock();
        try {
            List<TWorkGroupOp> currentWorkGroupOps = new ArrayList<>();
            if (!activeWorkGroupsPerBe.containsKey(beId)) {
                currentWorkGroupOps.addAll(workGroupOps);
                return currentWorkGroupOps;
            }
            Long minVersion = minVersionPerBe.get(beId);
            Map<Long, TWorkGroup> activeWorkGroup = activeWorkGroupsPerBe.get(beId);
            for (TWorkGroupOp op : workGroupOps) {
                TWorkGroup twg = op.getWorkgroup();
                if (twg.getVersion() < minVersion) {
                    continue;
                }
                if (!activeWorkGroup.containsKey(twg.getId()) ||
                        twg.getVersion() > activeWorkGroup.get(twg.getId()).getVersion()) {
                    currentWorkGroupOps.add(op);
                }
            }
            return currentWorkGroupOps;
        } finally {
            readUnlock();
        }
    }

    public void saveActiveWorkGroupsForBe(Long beId, List<TWorkGroup> workGroups) {
        writeLock();
        try {
            Map<Long, TWorkGroup> workGroupOnBe = new HashMap<>();
            Long minVersion = Long.MAX_VALUE;
            for (TWorkGroup workgroup : workGroups) {
                workGroupOnBe.put(workgroup.getId(), workgroup);
                if (workgroup.getVersion() < minVersion) {
                    minVersion = workgroup.getVersion();
                }
            }
            activeWorkGroupsPerBe.put(beId, workGroupOnBe);
            minVersionPerBe.put(beId, minVersion == Long.MAX_VALUE ? Long.MIN_VALUE : minVersion);
        } finally {
            writeUnlock();
        }
    }

    public WorkGroup chooseWorkGroup(String user, String roleName, WorkGroupClassifier.QueryType queryType, String ip) {

        List<WorkGroupClassifier> classifierList = classifierMap.values().stream()
                .filter(f -> f.isSatisfied(user, roleName, queryType, ip)).collect(Collectors.toList());
        classifierList.sort(Comparator.comparingDouble(WorkGroupClassifier::weight));
        if (classifierList.isEmpty()) {
            return null;
        } else {
            return id2WorkGroupMap.get(classifierList.get(0).getWorkgroupId());
        }
    }

    private static class SerializeData {
        @SerializedName("WorkGroups")
        public List<WorkGroup> workGroups;
    }
}
