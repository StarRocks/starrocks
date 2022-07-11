// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.AlterResourceGroupStmt;
import com.starrocks.analysis.CreateResourceGroupStmt;
import com.starrocks.analysis.DropResourceGroupStmt;
import com.starrocks.analysis.ShowResourceGroupStmt;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.ResourceGroupOpEntry;
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
public class ResourceGroupMgr implements Writable {
    private static final Logger LOG = LogManager.getLogger(ResourceGroupMgr.class);

    private GlobalStateMgr globalStateMgr;
    private Map<String, ResourceGroup> workGroupMap = new HashMap<>();
    private Map<Long, ResourceGroup> id2WorkGroupMap = new HashMap<>();
    private Map<Long, ResourceGroupClassifier> classifierMap = new HashMap<>();
    private List<TWorkGroupOp> workGroupOps = new ArrayList<>();
    private Map<Long, Map<Long, TWorkGroup>> activeWorkGroupsPerBe = new HashMap<>();
    private Map<Long, Long> minVersionPerBe = new HashMap<>();
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public ResourceGroupMgr(GlobalStateMgr globalStateMgr) {
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

    public void createWorkGroup(CreateResourceGroupStmt stmt) throws DdlException {
        writeLock();
        try {
            ResourceGroup wg = stmt.getWorkgroup();
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
            wg.setId(globalStateMgr.getCurrentState().getNextId());
            for (ResourceGroupClassifier classifier : wg.getClassifiers()) {
                classifier.setWorkgroupId(wg.getId());
                classifier.setId(globalStateMgr.getCurrentState().getNextId());
                classifierMap.put(classifier.getId(), classifier);
            }
            workGroupMap.put(wg.getName(), wg);
            id2WorkGroupMap.put(wg.getId(), wg);
            wg.setVersion(wg.getId());
            ResourceGroupOpEntry workGroupOp = new ResourceGroupOpEntry(TWorkGroupOpType.WORKGROUP_OP_CREATE, wg);
            globalStateMgr.getCurrentState().getEditLog().logWorkGroupOp(workGroupOp);
            workGroupOps.add(workGroupOp.toThrift());
        } finally {
            writeUnlock();
        }
    }

    public List<List<String>> showWorkGroup(ShowResourceGroupStmt stmt) throws AnalysisException {
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
            List<ResourceGroup> resourceGroupList = new ArrayList<>(workGroupMap.values());
            if (isListAll || ConnectContext.get() == null) {
                resourceGroupList.sort(Comparator.comparing(ResourceGroup::getName));
                return resourceGroupList.stream().map(ResourceGroup::show)
                        .flatMap(Collection::stream).collect(Collectors.toList());
            } else {
                String user = getUnqualifiedUser(ctx);
                String role = getUnqualifiedRole(ctx);
                String remoteIp = ctx.getRemoteIP();
                return resourceGroupList.stream().map(w -> w.showVisible(user, role, remoteIp))
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
        List<ResourceGroup> resourceGroups = workGroupMap.values().stream().collect(Collectors.toList());
        SerializeData data = new SerializeData();
        data.resourceGroups = resourceGroups;

        String s = GsonUtils.GSON.toJson(data);
        Text.writeString(out, s);
    }

    public void readFields(DataInputStream dis) throws IOException {
        String s = Text.readString(dis);
        SerializeData data = GsonUtils.GSON.fromJson(s, SerializeData.class);
        if (null != data && null != data.resourceGroups) {
            data.resourceGroups.sort(Comparator.comparing(ResourceGroup::getVersion));
            for (ResourceGroup workgroup : data.resourceGroups) {
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

    private void replayAddWorkGroup(ResourceGroup workgroup) {
        addWorkGroupInternal(workgroup);
        ResourceGroupOpEntry op = new ResourceGroupOpEntry(TWorkGroupOpType.WORKGROUP_OP_CREATE, workgroup);
        workGroupOps.add(op.toThrift());
    }

    public ResourceGroup getWorkGroup(String name) {
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

    public void alterWorkGroup(AlterResourceGroupStmt stmt) throws DdlException {
        writeLock();
        try {
            String name = stmt.getName();
            if (!workGroupMap.containsKey(name)) {
                throw new DdlException("RESOURCE_GROUP(" + name + ") does not exist");
            }
            ResourceGroup wg = workGroupMap.get(name);
            AlterResourceGroupStmt.SubCommand cmd = stmt.getCmd();
            if (cmd instanceof AlterResourceGroupStmt.AddClassifiers) {
                List<ResourceGroupClassifier> newAddedClassifiers = stmt.getNewAddedClassifiers();
                for (ResourceGroupClassifier classifier : newAddedClassifiers) {
                    classifier.setWorkgroupId(wg.getId());
                    classifier.setId(globalStateMgr.getCurrentState().getNextId());
                    classifierMap.put(classifier.getId(), classifier);
                }
                wg.getClassifiers().addAll(newAddedClassifiers);
            } else if (cmd instanceof AlterResourceGroupStmt.AlterProperties) {
                ResourceGroup changedProperties = stmt.getChangedProperties();
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
                TWorkGroupType workGroupType = changedProperties.getWorkGroupType();
                if (workGroupType != null) {
                    wg.setWorkGroupType(workGroupType);
                }
            } else if (cmd instanceof AlterResourceGroupStmt.DropClassifiers) {
                Set<Long> classifierToDrop = stmt.getClassifiersToDrop().stream().collect(Collectors.toSet());
                Iterator<ResourceGroupClassifier> classifierIterator = wg.getClassifiers().iterator();
                while (classifierIterator.hasNext()) {
                    if (classifierToDrop.contains(classifierIterator.next().getId())) {
                        classifierIterator.remove();
                    }
                }
                for (Long classifierId : classifierToDrop) {
                    classifierMap.remove(classifierId);
                }
            } else if (cmd instanceof AlterResourceGroupStmt.DropAllClassifiers) {
                List<ResourceGroupClassifier> classifierList = wg.getClassifiers();
                for (ResourceGroupClassifier classifier : classifierList) {
                    classifierMap.remove(classifier.getId());
                }
                classifierList.clear();
            }
            // only when changing properties, version is required to update. because changing classifiers needs not
            // propagate to BE.
            if (cmd instanceof AlterResourceGroupStmt.AlterProperties) {
                wg.setVersion(globalStateMgr.getCurrentState().getNextId());
            }
            ResourceGroupOpEntry workGroupOp = new ResourceGroupOpEntry(TWorkGroupOpType.WORKGROUP_OP_ALTER, wg);
            globalStateMgr.getCurrentState().getEditLog().logWorkGroupOp(workGroupOp);
            workGroupOps.add(workGroupOp.toThrift());
        } finally {
            writeUnlock();
        }
    }

    public void dropWorkGroup(DropResourceGroupStmt stmt) throws DdlException {
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
        ResourceGroup wg = workGroupMap.get(name);
        removeWorkGroupInternal(name);
        wg.setVersion(globalStateMgr.getCurrentState().getNextId());
        ResourceGroupOpEntry workGroupOp = new ResourceGroupOpEntry(TWorkGroupOpType.WORKGROUP_OP_DELETE, wg);
        globalStateMgr.getCurrentState().getEditLog().logWorkGroupOp(workGroupOp);
        workGroupOps.add(workGroupOp.toThrift());
    }

    public void replayWorkGroupOp(ResourceGroupOpEntry entry) {
        writeLock();
        try {
            ResourceGroup workgroup = entry.getResourceGroup();
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
        ResourceGroup wg = workGroupMap.remove(name);
        id2WorkGroupMap.remove(wg.getId());
        for (ResourceGroupClassifier classifier : wg.classifiers) {
            classifierMap.remove(classifier.getId());
        }
    }

    private void addWorkGroupInternal(ResourceGroup wg) {
        workGroupMap.put(wg.getName(), wg);
        id2WorkGroupMap.put(wg.getId(), wg);
        for (ResourceGroupClassifier classifier : wg.classifiers) {
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

    public ResourceGroup chooseWorkGroupByName(String wgName) {
        readLock();
        try {
            return workGroupMap.get(wgName);
        } finally {
            readUnlock();
        }
    }

    public ResourceGroup chooseWorkGroup(ConnectContext ctx, ResourceGroupClassifier.QueryType queryType, Set<Long> databases) {
        String user = getUnqualifiedUser(ctx);
        String role = getUnqualifiedRole(ctx);
        String remoteIp = ctx.getRemoteIP();
        List<ResourceGroupClassifier> classifierList = classifierMap.values().stream()
                .filter(f -> f.isSatisfied(user, role, queryType, remoteIp, databases))
                .sorted(Comparator.comparingDouble(ResourceGroupClassifier::weight))
                .collect(Collectors.toList());
        if (classifierList.isEmpty()) {
            return null;
        } else {
            return id2WorkGroupMap.get(classifierList.get(classifierList.size() - 1).getWorkgroupId());
        }
    }

    private static class SerializeData {
        @SerializedName("WorkGroups")
        public List<ResourceGroup> resourceGroups;
    }
}
