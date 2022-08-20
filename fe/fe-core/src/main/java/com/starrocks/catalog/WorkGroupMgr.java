// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.AlterWorkGroupStmt;
import com.starrocks.analysis.CreateWorkGroupStmt;
import com.starrocks.analysis.DropWorkGroupStmt;
import com.starrocks.analysis.ShowWorkGroupStmt;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.WorkGroupOpEntry;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TWorkGroup;
import com.starrocks.thrift.TWorkGroupOp;
import com.starrocks.thrift.TWorkGroupOpType;
import com.starrocks.thrift.TWorkGroupType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
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

// WorkGroupMgr is employed by GlobalStateMgr to manage WorkGroup in FE.
public class WorkGroupMgr implements Writable {
    private static final Logger LOG = LogManager.getLogger(WorkGroupMgr.class);

    private GlobalStateMgr globalStateMgr;
    private Map<String, WorkGroup> workGroupMap = new HashMap<>();
    private Map<Long, WorkGroup> id2WorkGroupMap = new HashMap<>();

    // Record the current short_query resource group.
    // There can be only one realtime resource group.
    private WorkGroup shortQueryResourceGroup = null;

    private Map<Long, WorkGroupClassifier> classifierMap = new HashMap<>();
    private List<TWorkGroupOp> workGroupOps = new ArrayList<>();
    private Map<Long, Map<Long, TWorkGroup>> activeWorkGroupsPerBe = new HashMap<>();
    private Map<Long, Long> minVersionPerBe = new HashMap<>();
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public WorkGroupMgr(GlobalStateMgr globalStateMgr) {
        this.globalStateMgr = globalStateMgr;
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

            if (wg.getWorkGroupType() == TWorkGroupType.WG_SHORT_QUERY && shortQueryResourceGroup != null) {
                throw new DdlException(
                        String.format("There can be only one short_query RESOURCE_GROUP (%s)",
                                shortQueryResourceGroup.getName()));
            }

            wg.setId(globalStateMgr.getCurrentState().getNextId());
            wg.setVersion(wg.getId());
            for (WorkGroupClassifier classifier : wg.getClassifiers()) {
                classifier.setWorkgroupId(wg.getId());
                classifier.setId(globalStateMgr.getCurrentState().getNextId());
            }
            addWorkGroupInternal(wg);
            WorkGroupOpEntry workGroupOp = new WorkGroupOpEntry(TWorkGroupOpType.WORKGROUP_OP_CREATE, wg);
            globalStateMgr.getCurrentState().getEditLog().logWorkGroupOp(workGroupOp);
            workGroupOps.add(workGroupOp.toThrift());
        } finally {
            writeUnlock();
        }
    }

    public List<List<String>> showWorkGroup(ShowWorkGroupStmt stmt) throws AnalysisException {
        if (stmt.getName() != null && !workGroupMap.containsKey(stmt.getName())) {
            ErrorReport.reportAnalysisException(ErrorCode.ERROR_NO_WG_ERROR, stmt.getName());
        }

        List<List<String>> rows;
        if (stmt.getName() != null) {
            rows = GlobalStateMgr.getCurrentState().getWorkGroupMgr().showOneWorkGroup(stmt.getName());
        } else {
            rows = GlobalStateMgr.getCurrentState().getWorkGroupMgr()
                    .showAllWorkGroups(ConnectContext.get(), stmt.isListAll());
        }
        return rows;
    }

    private String getUnqualifiedUser(ConnectContext ctx) {
        Preconditions.checkArgument(ctx != null);
        String qualifiedUser = ctx.getQualifiedUser();
        //default_cluster:test
        String[] userParts = qualifiedUser.split(":");
        return userParts[userParts.length - 1];
    }

    private String getUnqualifiedRole(ConnectContext ctx) {
        Preconditions.checkArgument(ctx != null);
        String roleName = null;
        String qualifiedRoleName = GlobalStateMgr.getCurrentState().getAuth()
                .getRoleName(ctx.getCurrentUserIdentity());
        if (qualifiedRoleName != null) {
            //default_cluster:role
            String[] roleParts = qualifiedRoleName.split(":");
            roleName = roleParts[roleParts.length - 1];
        }
        return roleName;
    }

    public List<List<String>> showAllWorkGroups(ConnectContext ctx, Boolean isListAll) {
        readLock();
        try {
            List<WorkGroup> workGroupList = new ArrayList<>(workGroupMap.values());
            if (isListAll || ConnectContext.get() == null) {
                workGroupList.sort(Comparator.comparing(WorkGroup::getName));
                return workGroupList.stream().map(WorkGroup::show)
                        .flatMap(Collection::stream).collect(Collectors.toList());
            } else {
                String user = getUnqualifiedUser(ctx);
                String role = getUnqualifiedRole(ctx);
                String remoteIp = ctx.getRemoteIP();
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

    public long loadWorkGroups(DataInputStream dis, long checksum) throws IOException {
        try {
            readFields(dis);
            LOG.info("finished replaying WorkGroups from image");
        } catch (EOFException e) {
            LOG.info("no WorkGroups to replay.");
        }
        return checksum;
    }

    public long saveWorkGroups(DataOutputStream dos, long checksum) throws IOException {
        write(dos);
        return checksum;
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
                    classifier.setId(globalStateMgr.getCurrentState().getNextId());
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

                Long bigQueryMemLimit = changedProperties.getBigQueryMemLimit();
                if (bigQueryMemLimit != null) {
                    wg.setBigQueryMemLimit(bigQueryMemLimit);
                }

                Long bigQueryScanRowsLimit = changedProperties.getBigQueryScanRowsLimit();
                if (bigQueryScanRowsLimit != null) {
                    wg.setBigQueryScanRowsLimit(bigQueryScanRowsLimit);
                }

                Long bigQueryCpuCoreSecondLimit = changedProperties.getBigQueryCpuSecondLimit();
                if (bigQueryCpuCoreSecondLimit != null) {
                    wg.setBigQueryCpuSecondLimit(bigQueryCpuCoreSecondLimit);
                }

                Integer concurrentLimit = changedProperties.getConcurrencyLimit();
                if (concurrentLimit != null) {
                    wg.setConcurrencyLimit(concurrentLimit);
                }

                // Type is guaranteed to be immutable during the analyzer phase.
                TWorkGroupType workGroupType = changedProperties.getWorkGroupType();
                Preconditions.checkState(workGroupType == null);
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
                wg.setVersion(globalStateMgr.getCurrentState().getNextId());
            }
            WorkGroupOpEntry workGroupOp = new WorkGroupOpEntry(TWorkGroupOpType.WORKGROUP_OP_ALTER, wg);
            globalStateMgr.getCurrentState().getEditLog().logWorkGroupOp(workGroupOp);
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
        wg.setVersion(globalStateMgr.getCurrentState().getNextId());
        WorkGroupOpEntry workGroupOp = new WorkGroupOpEntry(TWorkGroupOpType.WORKGROUP_OP_DELETE, wg);
        globalStateMgr.getCurrentState().getEditLog().logWorkGroupOp(workGroupOp);
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
        if (wg.getWorkGroupType() == TWorkGroupType.WG_SHORT_QUERY) {
            shortQueryResourceGroup = null;
        }
    }

    private void addWorkGroupInternal(WorkGroup wg) {
        workGroupMap.put(wg.getName(), wg);
        id2WorkGroupMap.put(wg.getId(), wg);
        for (WorkGroupClassifier classifier : wg.classifiers) {
            classifierMap.put(classifier.getId(), classifier);
        }
        if (wg.getResourceGroupType() == TWorkGroupType.WG_SHORT_QUERY) {
            shortQueryResourceGroup = wg;
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
                boolean active = activeWorkGroup.containsKey(twg.getId());
                if ((!active && id2WorkGroupMap.containsKey(twg.getId())) ||
                        (active && twg.getVersion() > activeWorkGroup.get(twg.getId()).getVersion())) {
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

    public WorkGroup chooseWorkGroupByName(String wgName) {
        readLock();
        try {
            return workGroupMap.get(wgName);
        } finally {
            readUnlock();
        }
    }

    public WorkGroup chooseWorkGroup(ConnectContext ctx, WorkGroupClassifier.QueryType queryType, Set<Long> databases) {
        String user = getUnqualifiedUser(ctx);
        String role = getUnqualifiedRole(ctx);
        String remoteIp = ctx.getRemoteIP();
        List<WorkGroupClassifier> classifierList = classifierMap.values().stream()
                .filter(f -> f.isSatisfied(user, role, queryType, remoteIp, databases))
                .sorted(Comparator.comparingDouble(WorkGroupClassifier::weight))
                .collect(Collectors.toList());
        if (classifierList.isEmpty()) {
            return null;
        } else {
            return id2WorkGroupMap.get(classifierList.get(classifierList.size() - 1).getWorkgroupId());
        }
    }

    private static class SerializeData {
        @SerializedName("WorkGroups")
        public List<WorkGroup> workGroups;
    }
}
