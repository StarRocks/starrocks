// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.ResourceGroupOpEntry;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.privilege.AuthorizationMgr;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.privilege.RolePrivilegeCollectionV2;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterResourceGroupStmt;
import com.starrocks.sql.ast.CreateResourceGroupStmt;
import com.starrocks.sql.ast.DropResourceGroupStmt;
import com.starrocks.sql.ast.ShowResourceGroupStmt;
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
    private final Map<String, ResourceGroup> resourceGroupMap = new HashMap<>();

    // Record the current short_query resource group.
    // There can be only one short_query resource group.
    private ResourceGroup shortQueryResourceGroup = null;

    private final Map<Long, ResourceGroup> id2ResourceGroupMap = new HashMap<>();
    private final Map<Long, ResourceGroupClassifier> classifierMap = new HashMap<>();
    private final List<TWorkGroupOp> resourceGroupOps = new ArrayList<>();
    private final Map<Long, Map<Long, TWorkGroup>> activeResourceGroupsPerBe = new HashMap<>();
    private final Map<Long, Long> minVersionPerBe = new HashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

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

    public void createResourceGroup(CreateResourceGroupStmt stmt) throws DdlException {
        writeLock();
        try {
            ResourceGroup wg = stmt.getResourceGroup();
            if (resourceGroupMap.containsKey(wg.getName())) {
                // create resource_group or replace <name> ...
                if (stmt.isReplaceIfExists()) {
                    dropResourceGroupUnlocked(wg.getName());
                } else if (!stmt.isIfNotExists()) {
                    throw new DdlException(String.format("RESOURCE_GROUP(%s) already exists", wg.getName()));
                } else {
                    return;
                }
            }

            if (wg.getResourceGroupType() == TWorkGroupType.WG_SHORT_QUERY && shortQueryResourceGroup != null) {
                throw new DdlException(
                        String.format("There can be only one short_query RESOURCE_GROUP (%s)",
                                shortQueryResourceGroup.getName()));
            }

            if (wg.getClassifiers() != null && !wg.getClassifiers().isEmpty() &&
                    wg.getResourceGroupType().equals(TWorkGroupType.WG_MV)) {
                throw new DdlException("MV Resource Group not support classifiers.");
            }

            if (wg.getClassifiers() == null || wg.getClassifiers().isEmpty() &&
                    !wg.getResourceGroupType().equals(TWorkGroupType.WG_MV)) {
                throw new DdlException("This type Resource Group need define classifiers.");
            }

            wg.setId(GlobalStateMgr.getCurrentState().getNextId());
            wg.setVersion(wg.getId());
            for (ResourceGroupClassifier classifier : wg.getClassifiers()) {
                classifier.setResourceGroupId(wg.getId());
                classifier.setId(GlobalStateMgr.getCurrentState().getNextId());
            }
            addResourceGroupInternal(wg);

            ResourceGroupOpEntry workGroupOp = new ResourceGroupOpEntry(TWorkGroupOpType.WORKGROUP_OP_CREATE, wg);
            GlobalStateMgr.getCurrentState().getEditLog().logResourceGroupOp(workGroupOp);
            resourceGroupOps.add(workGroupOp.toThrift());
        } finally {
            writeUnlock();
        }
    }

    public List<List<String>> showResourceGroup(ShowResourceGroupStmt stmt) throws AnalysisException {
        if (stmt.getName() != null && !resourceGroupMap.containsKey(stmt.getName())) {
            ErrorReport.reportAnalysisException(ErrorCode.ERROR_NO_RG_ERROR, stmt.getName());
        }

        List<List<String>> rows;
        if (stmt.getName() != null) {
            rows = GlobalStateMgr.getCurrentState().getResourceGroupMgr().showOneResourceGroup(stmt.getName());
        } else {
            rows = GlobalStateMgr.getCurrentState().getResourceGroupMgr()
                    .showAllResourceGroups(ConnectContext.get(), stmt.isListAll());
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

    private List<String> getUnqualifiedRole(ConnectContext ctx) {
        Preconditions.checkArgument(ctx != null);

        try {
            AuthorizationMgr manager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
            List<String> validRoles = new ArrayList<>();

            Set<Long> activeRoles = ctx.getCurrentRoleIds();
            if (activeRoles == null) {
                activeRoles = manager.getRoleIdsByUser(ctx.getCurrentUserIdentity());
            }

            for (Long roleId : activeRoles) {
                RolePrivilegeCollectionV2 rolePrivilegeCollection =
                        manager.getRolePrivilegeCollectionUnlocked(roleId, false);
                if (rolePrivilegeCollection != null) {
                    validRoles.add(rolePrivilegeCollection.getName());
                }
            }

            return validRoles.stream().filter(r -> !PrivilegeBuiltinConstants.BUILT_IN_ROLE_NAMES.contains(r))
                    .collect(Collectors.toList());
        } catch (PrivilegeException e) {
            LOG.info("getUnqualifiedRole failed for resource group, error message: " + e.getMessage());
            return null;
        }
    }

    public List<List<String>> showAllResourceGroups(ConnectContext ctx, Boolean isListAll) {
        readLock();
        try {
            List<ResourceGroup> resourceGroupList = new ArrayList<>(resourceGroupMap.values());
            if (isListAll || ConnectContext.get() == null) {
                resourceGroupList.sort(Comparator.comparing(ResourceGroup::getName));
                return resourceGroupList.stream().map(ResourceGroup::show)
                        .flatMap(Collection::stream).collect(Collectors.toList());
            } else {
                String user = getUnqualifiedUser(ctx);
                List<String> activeRoles = getUnqualifiedRole(ctx);
                String remoteIp = ctx.getRemoteIP();
                return resourceGroupList.stream().map(rg -> rg.showVisible(user, activeRoles, remoteIp))
                        .flatMap(Collection::stream).collect(Collectors.toList());
            }
        } finally {
            readUnlock();
        }
    }

    public List<List<String>> showOneResourceGroup(String name) {
        readLock();
        try {
            if (!resourceGroupMap.containsKey(name)) {
                return Collections.emptyList();
            } else {
                return resourceGroupMap.get(name).show();
            }
        } finally {
            readUnlock();
        }
    }

    public Set<String> getAllResourceGroupNames() {
        readLock();
        try {
            return resourceGroupMap.keySet();
        } finally {
            readUnlock();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        List<ResourceGroup> resourceGroups = resourceGroupMap.values().stream().collect(Collectors.toList());
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
                replayAddResourceGroup(workgroup);
            }
        }
    }

    public long loadResourceGroups(DataInputStream dis, long checksum) throws IOException {
        try {
            readFields(dis);
            LOG.info("finished replaying ResourceGroups from image");
        } catch (EOFException e) {
            LOG.info("no ResourceGroups to replay.");
        }
        return checksum;
    }

    public long saveResourceGroups(DataOutputStream dos, long checksum) throws IOException {
        write(dos);
        return checksum;
    }

    private void replayAddResourceGroup(ResourceGroup workgroup) {
        addResourceGroupInternal(workgroup);
        ResourceGroupOpEntry op = new ResourceGroupOpEntry(TWorkGroupOpType.WORKGROUP_OP_CREATE, workgroup);
        resourceGroupOps.add(op.toThrift());
    }

    public ResourceGroup getResourceGroup(String name) {
        readLock();
        try {
            if (resourceGroupMap.containsKey(name)) {
                return resourceGroupMap.get(name);
            } else {
                return null;
            }
        } finally {
            readUnlock();
        }
    }

    public ResourceGroup getResourceGroup(long id) {
        readLock();
        try {
            return id2ResourceGroupMap.getOrDefault(id, null);
        } finally {
            readUnlock();
        }
    }

    public void alterResourceGroup(AlterResourceGroupStmt stmt) throws DdlException {
        writeLock();
        try {
            String name = stmt.getName();
            if (!resourceGroupMap.containsKey(name)) {
                throw new DdlException("RESOURCE_GROUP(" + name + ") does not exist");
            }
            ResourceGroup wg = resourceGroupMap.get(name);
            AlterResourceGroupStmt.SubCommand cmd = stmt.getCmd();
            if (wg.getResourceGroupType() == TWorkGroupType.WG_MV &&
                    !(cmd instanceof AlterResourceGroupStmt.AlterProperties)) {
                throw new DdlException("MV Resource Group not support classifiers.");
            }
            if (cmd instanceof AlterResourceGroupStmt.AddClassifiers) {
                List<ResourceGroupClassifier> newAddedClassifiers = stmt.getNewAddedClassifiers();
                for (ResourceGroupClassifier classifier : newAddedClassifiers) {
                    classifier.setResourceGroupId(wg.getId());
                    classifier.setId(GlobalStateMgr.getCurrentState().getNextId());
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

                // Type is guaranteed to be immutable during the analyzer phase.
                TWorkGroupType workGroupType = changedProperties.getResourceGroupType();
                Preconditions.checkState(workGroupType == null);
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
                wg.setVersion(GlobalStateMgr.getCurrentState().getNextId());
            }
            ResourceGroupOpEntry workGroupOp = new ResourceGroupOpEntry(TWorkGroupOpType.WORKGROUP_OP_ALTER, wg);
            GlobalStateMgr.getCurrentState().getEditLog().logResourceGroupOp(workGroupOp);
            resourceGroupOps.add(workGroupOp.toThrift());
        } finally {
            writeUnlock();
        }
    }

    public void dropResourceGroup(DropResourceGroupStmt stmt) throws DdlException {
        writeLock();
        try {
            String name = stmt.getName();
            if (!resourceGroupMap.containsKey(name)) {
                throw new DdlException("RESOURCE_GROUP(" + name + ") does not exist");
            }
            dropResourceGroupUnlocked(name);
        } finally {
            writeUnlock();
        }
    }

    public void dropResourceGroupUnlocked(String name) {
        ResourceGroup wg = resourceGroupMap.get(name);
        removeResourceGroupInternal(name);
        wg.setVersion(GlobalStateMgr.getCurrentState().getNextId());
        ResourceGroupOpEntry workGroupOp = new ResourceGroupOpEntry(TWorkGroupOpType.WORKGROUP_OP_DELETE, wg);
        GlobalStateMgr.getCurrentState().getEditLog().logResourceGroupOp(workGroupOp);
        resourceGroupOps.add(workGroupOp.toThrift());
    }

    public void replayResourceGroupOp(ResourceGroupOpEntry entry) {
        writeLock();
        try {
            ResourceGroup workgroup = entry.getResourceGroup();
            TWorkGroupOpType opType = entry.getOpType();
            switch (opType) {
                case WORKGROUP_OP_CREATE:
                    addResourceGroupInternal(workgroup);
                    break;
                case WORKGROUP_OP_DELETE:
                    removeResourceGroupInternal(workgroup.getName());
                    break;
                case WORKGROUP_OP_ALTER:
                    removeResourceGroupInternal(workgroup.getName());
                    addResourceGroupInternal(workgroup);
                    break;
            }
            resourceGroupOps.add(entry.toThrift());
        } finally {
            writeUnlock();
        }
    }

    private void removeResourceGroupInternal(String name) {
        ResourceGroup wg = resourceGroupMap.remove(name);
        id2ResourceGroupMap.remove(wg.getId());
        for (ResourceGroupClassifier classifier : wg.classifiers) {
            classifierMap.remove(classifier.getId());
        }
        if (wg.getResourceGroupType() == TWorkGroupType.WG_SHORT_QUERY) {
            shortQueryResourceGroup = null;
        }
    }

    private void addResourceGroupInternal(ResourceGroup wg) {
        resourceGroupMap.put(wg.getName(), wg);
        id2ResourceGroupMap.put(wg.getId(), wg);
        for (ResourceGroupClassifier classifier : wg.classifiers) {
            classifierMap.put(classifier.getId(), classifier);
        }
        if (wg.getResourceGroupType() == TWorkGroupType.WG_SHORT_QUERY) {
            shortQueryResourceGroup = wg;
        }
    }

    public List<TWorkGroupOp> getResourceGroupsNeedToDeliver(Long beId) {
        readLock();
        try {
            List<TWorkGroupOp> currentResourceGroupOps = new ArrayList<>();
            if (!activeResourceGroupsPerBe.containsKey(beId)) {
                currentResourceGroupOps.addAll(resourceGroupOps);
                return currentResourceGroupOps;
            }
            Long minVersion = minVersionPerBe.get(beId);
            Map<Long, TWorkGroup> activeResourceGroup = activeResourceGroupsPerBe.get(beId);
            for (TWorkGroupOp op : resourceGroupOps) {
                TWorkGroup twg = op.getWorkgroup();
                if (twg.getVersion() < minVersion) {
                    continue;
                }
                boolean active = activeResourceGroup.containsKey(twg.getId());
                if ((!active && id2ResourceGroupMap.containsKey(twg.getId())) ||
                        (active && twg.getVersion() > activeResourceGroup.get(twg.getId()).getVersion())) {
                    currentResourceGroupOps.add(op);
                }
            }
            return currentResourceGroupOps;
        } finally {
            readUnlock();
        }
    }

    public void saveActiveResourceGroupsForBe(Long beId, List<TWorkGroup> workGroups) {
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
            activeResourceGroupsPerBe.put(beId, workGroupOnBe);
            minVersionPerBe.put(beId, minVersion == Long.MAX_VALUE ? Long.MIN_VALUE : minVersion);
        } finally {
            writeUnlock();
        }
    }

    public TWorkGroup chooseResourceGroupByName(String wgName) {
        readLock();
        try {
            ResourceGroup rg = resourceGroupMap.get(wgName);
            if (rg == null) {
                return null;
            }
            return rg.toThrift();
        } finally {
            readUnlock();
        }
    }

    public TWorkGroup chooseResourceGroupByID(long wgID) {
        readLock();
        try {
            ResourceGroup rg = id2ResourceGroupMap.get(wgID);
            if (rg == null) {
                return null;
            }
            return rg.toThrift();
        } finally {
            readUnlock();
        }
    }

    public TWorkGroup chooseResourceGroup(ConnectContext ctx, ResourceGroupClassifier.QueryType queryType, Set<Long> databases) {
        List<String> activeRoles = getUnqualifiedRole(ctx);

        readLock();
        try {
            String user = getUnqualifiedUser(ctx);
            String remoteIp = ctx.getRemoteIP();

            // check short query first
            if (shortQueryResourceGroup != null) {
                List<ResourceGroupClassifier> shortQueryClassifierList =
                        shortQueryResourceGroup.classifiers.stream().filter(
                                        f -> f.isSatisfied(user, activeRoles, queryType, remoteIp, databases))
                                .sorted(Comparator.comparingDouble(ResourceGroupClassifier::weight))
                                .collect(Collectors.toList());
                if (!shortQueryClassifierList.isEmpty()) {
                    return shortQueryResourceGroup.toThrift();
                }
            }

            List<ResourceGroupClassifier> classifierList =
                    classifierMap.values().stream().filter(f -> f.isSatisfied(user, activeRoles, queryType, remoteIp, databases))
                            .sorted(Comparator.comparingDouble(ResourceGroupClassifier::weight))
                            .collect(Collectors.toList());
            if (classifierList.isEmpty()) {
                return null;
            } else {
                ResourceGroup rg =
                        id2ResourceGroupMap.get(classifierList.get(classifierList.size() - 1).getResourceGroupId());
                if (rg == null) {
                    return null;
                }
                return rg.toThrift();
            }
        } finally {
            readUnlock();
        }
    }

    private static class SerializeData {
        @SerializedName("WorkGroups")
        public List<ResourceGroup> resourceGroups;
    }

    public void save(DataOutputStream dos) throws IOException, SRMetaBlockException {
        int numJson = 1 + resourceGroupMap.size();
        SRMetaBlockWriter writer = new SRMetaBlockWriter(dos, SRMetaBlockID.RESOURCE_GROUP_MGR, numJson);
        writer.writeJson(resourceGroupMap.size());
        for (ResourceGroup resourceGroup : resourceGroupMap.values()) {
            writer.writeJson(resourceGroup);
        }

        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        int numJson = reader.readInt();
        List<ResourceGroup> resourceGroups = new ArrayList<>();
        for (int i = 0; i < numJson; ++i) {
            ResourceGroup resourceGroup = reader.readJson(ResourceGroup.class);
            resourceGroups.add(resourceGroup);
        }
        resourceGroups.sort(Comparator.comparing(ResourceGroup::getVersion));
        resourceGroups.forEach(this::replayAddResourceGroup);
    }
}
