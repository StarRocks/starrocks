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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.authorization.PrivilegeException;
import com.starrocks.authorization.RolePrivilegeCollectionV2;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.AlterResourceGroupLog;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.ResourceGroupOpEntry;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterResourceGroupStmt;
import com.starrocks.sql.ast.CreateResourceGroupStmt;
import com.starrocks.sql.ast.DropResourceGroupStmt;
import com.starrocks.sql.ast.ShowResourceGroupStmt;
import com.starrocks.sql.optimizer.cost.feature.CostPredictor;
import com.starrocks.system.BackendResourceStat;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TWorkGroup;
import com.starrocks.thrift.TWorkGroupOp;
import com.starrocks.thrift.TWorkGroupOpType;
import com.starrocks.thrift.TWorkGroupType;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static com.starrocks.server.WarehouseManager.DEFAULT_WAREHOUSE_ID;

// WorkGroupMgr is employed by GlobalStateMgr to manage WorkGroup in FE.
public class ResourceGroupMgr implements Writable {
    private static final Logger LOG = LogManager.getLogger(ResourceGroupMgr.class);

    public static final String SHORT_QUERY_SET_EXCLUSIVE_CPU_CORES_ERR_MSG =
            "'short_query' ResourceGroup cannot set 'exclusive_cpu_cores', " +
                    "since it use 'cpu_weight' as 'exclusive_cpu_cores'";

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
    private volatile boolean hasCreatedDefaultResourceGroups = false;

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
            boolean needReplace = false;
            if (resourceGroupMap.containsKey(wg.getName())) {
                if (stmt.isReplaceIfExists()) {
                    needReplace = true;
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

            if ((wg.getClassifiers() == null || wg.getClassifiers().isEmpty()) &&
                    !ResourceGroup.BUILTIN_WG_NAMES.contains(wg.getName()) &&
                    !wg.getResourceGroupType().equals(TWorkGroupType.WG_MV)) {
                throw new DdlException("This type Resource Group need define classifiers.");
            }

            validateExclusiveCpuCoresInlock(
                    wg.getNormalizedExclusiveCpuCores(), wg.getExclusiveCpuPercent(), wg.getWarehouses(), wg);

            if (needReplace) {
                dropResourceGroupUnlocked(wg.getName());
            }

            wg.normalizeCpuWeight();

            if (ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME.equals(wg.getName())) {
                wg.setId(ResourceGroup.DEFAULT_WG_ID);
            } else if (ResourceGroup.DEFAULT_MV_RESOURCE_GROUP_NAME.equals(wg.getName())) {
                wg.setId(ResourceGroup.DEFAULT_MV_WG_ID);
            } else {
                wg.setId(GlobalStateMgr.getCurrentState().getNextId());
            }

            wg.setVersion(wg.getId());
            for (ResourceGroupClassifier classifier : wg.getClassifiers()) {
                classifier.setResourceGroupId(wg.getId());
                classifier.setId(GlobalStateMgr.getCurrentState().getNextId());
            }

            if (!wg.hasDefaultMemPool() && !resourceGroupInMemPoolHaveSameMemLimit(wg)) {
                throw new DdlException(
                        "Property `mem_limit` must be equal for all resource groups using the mem_pool [" + wg.getMemPool() +
                                "].");
            }
            addResourceGroupInternal(wg);

            ResourceGroupOpEntry workGroupOp = new ResourceGroupOpEntry(TWorkGroupOpType.WORKGROUP_OP_CREATE, wg);
            GlobalStateMgr.getCurrentState().getEditLog().logResourceGroupOp(workGroupOp);
            resourceGroupOps.add(workGroupOp.toThrift());
        } finally {
            writeUnlock();
        }
    }

    public List<List<String>> showResourceGroup(ShowResourceGroupStmt stmt) {
        if (stmt.getName() != null && !resourceGroupMap.containsKey(stmt.getName())) {
            ErrorReport.reportSemanticException(ErrorCode.ERROR_NO_RG_ERROR, stmt.getName());
        }

        List<List<String>> rows;
        if (stmt.getName() != null) {
            rows = showOneResourceGroup(stmt.getName(), stmt.isVerbose());
        } else {
            rows = showAllResourceGroups(ConnectContext.get(), stmt.isVerbose(), stmt.isListAll());
        }
        return rows;
    }

    public List<Long> getResourceGroupIds() {
        readLock();
        try {
            return new ArrayList<>(id2ResourceGroupMap.keySet());
        } finally {
            readUnlock();
        }
    }

    private boolean resourceGroupInMemPoolHaveSameMemLimit(ResourceGroup wg) {
        if (wg.hasDefaultMemPool()) {
            return true;
        }
        return resourceGroupMap.entrySet().stream().allMatch(entry -> !wg.getMemPool().equals(entry.getValue().getMemPool()) ||
                wg.getMemLimit().equals(entry.getValue().getMemLimit()));
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

    public List<List<String>> showAllResourceGroups(ConnectContext ctx, boolean verbose, boolean isListAll) {
        readLock();
        try {
            List<ResourceGroup> resourceGroupList = new ArrayList<>(resourceGroupMap.values());
            if (isListAll || ConnectContext.get() == null) {
                resourceGroupList.sort(Comparator.comparing(ResourceGroup::getName));
                return resourceGroupList.stream()
                        .map(rg -> rg.show(verbose))
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList());
            } else {
                String user = getUnqualifiedUser(ctx);
                List<String> activeRoles = getUnqualifiedRole(ctx);
                String remoteIp = ctx.getRemoteIP();
                return resourceGroupList.stream()
                        .map(rg -> rg.showVisible(user, activeRoles, remoteIp, verbose))
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList());
            }
        } finally {
            readUnlock();
        }
    }

    public List<List<String>> showOneResourceGroup(String name, boolean verbose) {
        readLock();
        try {
            if (!resourceGroupMap.containsKey(name)) {
                return Collections.emptyList();
            } else {
                return resourceGroupMap.get(name).show(verbose);
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




    private void replayAddResourceGroup(ResourceGroup workgroup) {
        addResourceGroupInternal(workgroup);
        ResourceGroupOpEntry op = new ResourceGroupOpEntry(TWorkGroupOpType.WORKGROUP_OP_CREATE, workgroup);
        resourceGroupOps.add(op.toThrift());
    }

    public ResourceGroup getResourceGroup(String name) {
        readLock();
        try {
            return resourceGroupMap.getOrDefault(name, null);
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

    private int getExclusiveCpuCores(Integer exclusiveCpuCores, Integer exclusiveCpuPercent, int minCoreNum) {
        if (exclusiveCpuCores != null && exclusiveCpuCores > 0) {
            return exclusiveCpuCores;
        } else if (exclusiveCpuPercent != null && exclusiveCpuPercent > 0) {
            return minCoreNum * exclusiveCpuPercent / 100;
        } else {
            return 0;
        }
    }

    private static class WarehouseCoresInfo {
        private final int minCores;
        private int sumExclusiveCpuCores = 0;

        private WarehouseCoresInfo(int minCores) {
            this.minCores = minCores;
        }
    }

    /**
     * For each warehouse, the sum of the exclusive CPU cores of all effective resource groups on that warehouse plus one must
     * not exceed the number of CPU cores of the smallest BE in that resource group.
     *
     * <p> The resource groups that are effective on a warehouse are defined as follows:
     * - For a warehouse with bound resource groups: both the resource groups bound to that warehouse and the resource groups not
     * bound to any warehouse.
     * - For a warehouse without bound resource groups: the resource groups not bound to any warehouse.
     */
    private void validateExclusiveCpuCoresInlock(Integer exclusiveCpuCores, Integer exclusiveCpuPercent, List<String> warehouses,
                                                 ResourceGroup wg)
            throws DdlException {
        Set<Long> boundWhIds = Sets.newHashSet();
        Map<String, WarehouseCoresInfo> boundWhToItem = new HashMap<>();

        List<ResourceGroup> groups = new ArrayList<>(resourceGroupMap.values());
        if (!resourceGroupMap.containsKey(wg.getName())) {
            groups.add(wg);
        }

        // First, iterate over the resource groups that are bound to a warehouse to determine
        // which warehouses have resource groups bound to them.
        for (ResourceGroup group : groups) {
            if (group.getWarehouses() == null || group.getWarehouses().isEmpty()) {
                continue;
            }

            Integer curExclusiveCpuCores;
            Integer curExclusiveCpuPercent;
            List<String> curWarehouses;
            if (group.getName().equals(wg.getName())) {
                curExclusiveCpuCores = exclusiveCpuCores;
                curExclusiveCpuPercent = exclusiveCpuPercent;
                curWarehouses = warehouses;
            } else {
                curExclusiveCpuCores = group.getNormalizedExclusiveCpuCores();
                curExclusiveCpuPercent = group.getExclusiveCpuPercent();
                curWarehouses = group.getWarehouses();
            }

            for (String warehouseName : curWarehouses) {
                WarehouseCoresInfo item = boundWhToItem.get(warehouseName);
                if (item == null) {
                    Warehouse wh = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseAllowNull(warehouseName);
                    if (wh == null) {
                        continue;
                    }
                    boundWhIds.add(wh.getId());
                    int minCores = BackendResourceStat.getInstance().getMinNumCoresOfBe(wh.getId());
                    item = new WarehouseCoresInfo(minCores);
                    boundWhToItem.put(warehouseName, item);
                }

                int exclusiveCores = getExclusiveCpuCores(curExclusiveCpuCores, curExclusiveCpuPercent, item.minCores);
                item.sumExclusiveCpuCores += exclusiveCores;
            }
        }

        // Then, iterate over the resource groups that are not bound to any warehouse.
        final int nonBoundWhMinCores = BackendResourceStat.getInstance().getMinNumCoresOfBeExceptWarehouses(boundWhIds);
        int nonBoundWhSumExclusiveCores = 0;
        for (ResourceGroup group : groups) {
            if (group.getWarehouses() != null && !group.getWarehouses().isEmpty()) {
                continue;
            }

            Integer curExclusiveCpuCores;
            Integer curExclusiveCpuPercent;
            if (group.getName().equals(wg.getName())) {
                curExclusiveCpuCores = exclusiveCpuCores;
                curExclusiveCpuPercent = exclusiveCpuPercent;
            } else {
                curExclusiveCpuCores = group.getNormalizedExclusiveCpuCores();
                curExclusiveCpuPercent = group.getExclusiveCpuPercent();
            }

            int nonUsedWhExclusiveCores = getExclusiveCpuCores(curExclusiveCpuCores, curExclusiveCpuPercent, nonBoundWhMinCores);
            nonBoundWhSumExclusiveCores += nonUsedWhExclusiveCores;

            for (WarehouseCoresInfo item : boundWhToItem.values()) {
                int exclusiveCores = getExclusiveCpuCores(curExclusiveCpuCores, curExclusiveCpuPercent, item.minCores);
                item.sumExclusiveCpuCores += exclusiveCores;
            }
        }

        if (nonBoundWhSumExclusiveCores + 1 > nonBoundWhMinCores) {
            throw new DdlException(String.format("The effective exclusive CPU allocation (%d) exceeds the available cores " +
                    "(%d, that is, total cores minus one reserved for non-exclusive groups) on the smallest BE " +
                    "not assigned to any warehouse.", nonBoundWhSumExclusiveCores, nonBoundWhMinCores - 1));
        }
        for (Map.Entry<String, WarehouseCoresInfo> entry : boundWhToItem.entrySet()) {
            String warehouseName = entry.getKey();
            WarehouseCoresInfo item = entry.getValue();
            if (item.sumExclusiveCpuCores + 1 > item.minCores) {
                throw new DdlException(String.format("The effective exclusive CPU allocation (%d) exceeds the available cores " +
                        "(%d, that is, total cores minus one reserved for non-exclusive groups) on the smallest BE " +
                        "of warehouse %s.", item.sumExclusiveCpuCores, item.minCores - 1, warehouseName));
            }
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

                Integer cpuWeight = changedProperties.getRawCpuWeight();
                if (cpuWeight == null) {
                    cpuWeight = wg.getNormalizedCpuWeight();
                }
                Integer cpuWeightPercent = changedProperties.getCpuWeightPercent();
                if (cpuWeightPercent == null) {
                    cpuWeightPercent = wg.getCpuWeightPercent();
                }
                Integer exclusiveCpuCores = changedProperties.getExclusiveCpuCores();
                if (exclusiveCpuCores == null) {
                    exclusiveCpuCores = wg.getExclusiveCpuCores();
                }
                Integer exclusiveCpuPercent = changedProperties.getExclusiveCpuPercent();
                if (exclusiveCpuPercent == null) {
                    exclusiveCpuPercent = wg.getExclusiveCpuPercent();
                }
                List<String> warehouses = changedProperties.getWarehouses();
                if (warehouses == null) {
                    warehouses = wg.getWarehouses();
                }
                Integer maxCpuCores = changedProperties.getMaxCpuCores();
                if (maxCpuCores == null) {
                    maxCpuCores = wg.getMaxCpuCores();
                }
                ResourceGroup.validateCpuParameters(cpuWeight, cpuWeightPercent,
                        exclusiveCpuCores, exclusiveCpuPercent, maxCpuCores, wg.getResourceGroupType(), warehouses);
                if ((exclusiveCpuCores != null && exclusiveCpuCores > 0) ||
                        (exclusiveCpuPercent != null && exclusiveCpuPercent > 0)) {
                    validateExclusiveCpuCoresInlock(exclusiveCpuCores, exclusiveCpuPercent, warehouses, wg);
                    if (wg.getResourceGroupType() == TWorkGroupType.WG_SHORT_QUERY) {
                        throw new SemanticException(SHORT_QUERY_SET_EXCLUSIVE_CPU_CORES_ERR_MSG);
                    }
                }
<<<<<<< HEAD
                // NOTE that validate cpu parameters should be called before setting properties.

                if (cpuWeight != null) {
                    wg.setCpuWeight(cpuWeight);
                }
                wg.normalizeCpuWeight();
=======
>>>>>>> df7b521d15 ([Feature] Support warehouses, cpu_weight_percent, exclusive_cpu_weight for resource group (#66947))

                String memPool = wg.getMemPool();
                if (wg.hasDefaultMemPool()) {
                    memPool = ResourceGroup.DEFAULT_MEM_POOL;
                }
<<<<<<< HEAD

                if (exclusiveCpuCores != null) {
                    sumExclusiveCpuCores -= wg.getNormalizedExclusiveCpuCores();
                    wg.setExclusiveCpuCores(exclusiveCpuCores);
                    sumExclusiveCpuCores += wg.getNormalizedExclusiveCpuCores();
                }

                Integer maxCpuCores = changedProperties.getMaxCpuCores();
                if (maxCpuCores != null) {
                    wg.setMaxCpuCores(maxCpuCores);
                }
=======
>>>>>>> df7b521d15 ([Feature] Support warehouses, cpu_weight_percent, exclusive_cpu_weight for resource group (#66947))
                if (changedProperties.getMemPool() != null && !changedProperties.getMemPool().equals(memPool)) {
                    throw new DdlException("Property `mem_pool` cannot be altered [" + wg.getMemPool() + "].");
                }
                if (!wg.hasDefaultMemPool() &&
                        changedProperties.getMemLimit() != null &&
                        !wg.getMemLimit().equals(changedProperties.getMemLimit())) {
                    throw new DdlException(
                            "Property `mem_limit` cannot be altered for resource groups with mem_pool [" +
                                    wg.getMemPool() + "].");
                }

                // NOTE that validate parameters should be called before setting properties.

                cpuWeightPercent = changedProperties.getCpuWeightPercent();
                if (cpuWeightPercent != null) {
                    alterResourceGroupLog.setCpuWeightPercent(cpuWeightPercent);
                }
                cpuWeight = changedProperties.getRawCpuWeight();
                if (cpuWeight != null) {
                    alterResourceGroupLog.setCpuWeight(cpuWeight);
                }
                exclusiveCpuCores = changedProperties.getExclusiveCpuCores();
                if (exclusiveCpuCores != null) {
                    alterResourceGroupLog.setExclusiveCpuCores(exclusiveCpuCores);
                }
                exclusiveCpuPercent = changedProperties.getExclusiveCpuPercent();
                if (exclusiveCpuPercent != null) {
                    alterResourceGroupLog.setExclusiveCpuPercent(exclusiveCpuPercent);
                }

                maxCpuCores = changedProperties.getMaxCpuCores();
                if (maxCpuCores != null) {
                    alterResourceGroupLog.setMaxCpuCores(maxCpuCores);
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

                Double spillMemLimitThreshold = changedProperties.getSpillMemLimitThreshold();
                if (spillMemLimitThreshold != null) {
                    wg.setSpillMemLimitThreshold(spillMemLimitThreshold);
                }

                warehouses = changedProperties.getWarehouses();
                if (warehouses != null) {
                    alterResourceGroupLog.setWarehouses(warehouses);
                }

                // Type is guaranteed to be immutable during the analyzer phase.
                TWorkGroupType workGroupType = changedProperties.getResourceGroupType();
                Preconditions.checkState(workGroupType == null);
            } else if (cmd instanceof AlterResourceGroupStmt.DropClassifiers) {
                Set<Long> classifierToDrop = new HashSet<>(stmt.getClassifiersToDrop());
                wg.getClassifiers().removeIf(classifier -> classifierToDrop.contains(classifier.getId()));
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

<<<<<<< HEAD
=======
    private void updateResourceGroup(ResourceGroup wg, AlterResourceGroupLog log) {
        if (log.getClassifiers() != null) {
            List<ResourceGroupClassifier> oldClassifiers = wg.getClassifiers();
            Set<Long> newClassifierIds = log.getClassifiers().stream()
                    .map(ResourceGroupClassifier::getId).collect(Collectors.toSet());
            for (ResourceGroupClassifier classifier : oldClassifiers) {
                if (!newClassifierIds.contains(classifier.getId())) {
                    classifierMap.remove(classifier.getId());
                }
            }
            for (ResourceGroupClassifier classifier : log.getClassifiers()) {
                classifierMap.put(classifier.getId(), classifier);
            }
            wg.setClassifiers(log.getClassifiers());
        }
        if (log.getCpuWeight() != null) {
            wg.setCpuWeight(log.getCpuWeight());
            wg.normalizeCpuWeight();
        }
        if (log.getCpuWeightPercent() != null) {
            wg.setCpuWeightPercent(log.getCpuWeightPercent());
        }
        if (log.getExclusiveCpuCores() != null) {
            wg.setExclusiveCpuCores(log.getExclusiveCpuCores());
        }
        if (log.getExclusiveCpuPercent() != null) {
            wg.setExclusiveCpuPercent(log.getExclusiveCpuPercent());
        }
        if (log.getMaxCpuCores() != null) {
            wg.setMaxCpuCores(log.getMaxCpuCores());
        }
        if (log.getMemLimit() != null) {
            wg.setMemLimit(log.getMemLimit());
        }
        if (log.getBigQueryMemLimit() != null) {
            wg.setBigQueryMemLimit(log.getBigQueryMemLimit());
        }
        if (log.getBigQueryScanRowsLimit() != null) {
            wg.setBigQueryScanRowsLimit(log.getBigQueryScanRowsLimit());
        }
        if (log.getBigQueryCpuSecondLimit() != null) {
            wg.setBigQueryCpuSecondLimit(log.getBigQueryCpuSecondLimit());
        }
        if (log.getConcurrencyLimit() != null) {
            wg.setConcurrencyLimit(log.getConcurrencyLimit());
        }
        if (log.getSpillMemLimitThreshold() != null) {
            wg.setSpillMemLimitThreshold(log.getSpillMemLimitThreshold());
        }
        if (log.getWarehouses() != null) {
            wg.setWarehouses(log.getWarehouses());
        }
        if (log.getVersion() != 0) {
            wg.setVersion(log.getVersion());
        }
    }

>>>>>>> df7b521d15 ([Feature] Support warehouses, cpu_weight_percent, exclusive_cpu_weight for resource group (#66947))
    public void dropResourceGroup(DropResourceGroupStmt stmt) throws DdlException {
        writeLock();
        try {
            String name = stmt.getName();
            if (!resourceGroupMap.containsKey(name)) {
                if (!stmt.isIfExists()) {
                    throw new DdlException("RESOURCE_GROUP(" + name + ") does not exist");
                }
                return;
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
        if (ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME.equals(wg.getName())) {
            hasCreatedDefaultResourceGroups = true;
        }
    }

    /**
     * If a resource group is bound to specific warehouses, and the warehouse that the current BE belongs to is not among those
     * bound warehouses, then the TWorkGroupOp sent to that BE will be marked as inactive, meaning this resource group will not
     * take effect on that BE.
     *
     * <p> We separate the logic of pushing resource groups to BEs from the logic of deciding whether they should be inactive,
     * to avoid complicated handling when the set of warehouses bound to a resource group changes.
     *
     * @param op            the resource group operation will be sent to this BE
     * @param warehouseName the warehouse name that the current BE belongs to
     * @return the TWorkGroupOp that may be marked as inactive
     */
    private TWorkGroupOp setInactiveOp(TWorkGroupOp op, String warehouseName) {
        List<String> warehouses = op.getWorkgroup().getWarehouses();
        if (warehouseName == null || warehouses == null || warehouses.isEmpty() || warehouses.contains(warehouseName)) {
            return op;
        }

        // Only when we need to set `inactive` do we create a copied instance.
        // In all other cases, all BEs share the same TWorkGroupOp instance.
        TWorkGroupOp newOp = op.deepCopy();
        newOp.getWorkgroup().setInactive(true);
        return newOp;
    }

    public List<TWorkGroupOp> getResourceGroupsNeedToDeliver(Long beId) {
        ComputeNode computeNode = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(beId);
        String warehouseName = null;
        if (computeNode != null) {
            Warehouse wh = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseAllowNull(computeNode.getWarehouseId());
            if (wh != null) {
                warehouseName = wh.getName();
            }
        }

        readLock();
        try {
            List<TWorkGroupOp> currentResourceGroupOps = new ArrayList<>();
            if (!activeResourceGroupsPerBe.containsKey(beId)) {
                for (TWorkGroupOp op : resourceGroupOps) {
                    currentResourceGroupOps.add(setInactiveOp(op, warehouseName));
                }
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
                    currentResourceGroupOps.add(setInactiveOp(op, warehouseName));
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
            final double planCpuCost = ctx.getAuditEventBuilder().build().planCpuCosts;
            final double planMemCost = CostPredictor.getServiceBasedCostPredictor().isAvailable() ?
                    ctx.getAuditEventBuilder().build().predictMemBytes :
                    ctx.getAuditEventBuilder().build().planMemCosts;

            // check short query first
            if (shortQueryResourceGroup != null) {
                List<ResourceGroupClassifier> shortQueryClassifierList = shortQueryResourceGroup.classifiers.stream()
                        .filter(f -> f.isSatisfied(user, activeRoles, queryType, remoteIp, databases, planCpuCost, planMemCost))
                        .sorted(Comparator.comparingDouble(ResourceGroupClassifier::weight))
                        .collect(Collectors.toList());
                if (!shortQueryClassifierList.isEmpty()) {
                    return shortQueryResourceGroup.toThrift();
                }
            }

            List<ResourceGroupClassifier> classifierList =
                    classifierMap.values().stream()
                            .filter(f -> f.isSatisfied(user, activeRoles, queryType, remoteIp, databases, planCpuCost,
                                    planMemCost))
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

    public void createBuiltinResourceGroupsIfNotExist() {
        try {
            if (hasCreatedDefaultResourceGroups) {
                return;
            }

            // Create default resource groups only when there are BEs.
            // Otherwise, we cannot get the number of cores of BE as `cpu_weight`.
            if (BackendResourceStat.getInstance().getNumBes(DEFAULT_WAREHOUSE_ID) <= 0) {
                return;
            }

            ResourceGroup defaultWg = getResourceGroup(ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME);
            if (defaultWg != null) {
                return;
            }

            Map<String, String> defaultWgProperties = ImmutableMap.of(
                    ResourceGroup.CPU_WEIGHT_PERCENT, "100",
                    ResourceGroup.MEM_LIMIT, "1.0"
            );
            CreateResourceGroupStmt defaultWgStmt = new CreateResourceGroupStmt(ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME,
                    true, false, Collections.emptyList(), defaultWgProperties);
            defaultWgStmt.analyze();
            createResourceGroup(defaultWgStmt);

            Map<String, String> defaultMvWgProperties = ImmutableMap.of(
                    ResourceGroup.CPU_WEIGHT_PERCENT, "1",
                    ResourceGroup.MEM_LIMIT, "0.8",
                    ResourceGroup.SPILL_MEM_LIMIT_THRESHOLD, "0.8"
            );
            CreateResourceGroupStmt defaultMvWgStmt = new CreateResourceGroupStmt(ResourceGroup.DEFAULT_MV_RESOURCE_GROUP_NAME,
                    true, false, Collections.emptyList(), defaultMvWgProperties);
            defaultMvWgStmt.analyze();
            createResourceGroup(defaultMvWgStmt);
        } catch (Exception e) {
            LOG.warn("failed to create builtin resource groups", e);
        }
    }

    private void updateResourceGroup(ResourceGroup wg, AlterResourceGroupLog log) {
        if (log.getClassifiers() != null) {
            List<ResourceGroupClassifier> oldClassifiers = wg.getClassifiers();
            Set<Long> newClassifierIds = log.getClassifiers().stream()
                    .map(ResourceGroupClassifier::getId).collect(Collectors.toSet());
            for (ResourceGroupClassifier classifier : oldClassifiers) {
                if (!newClassifierIds.contains(classifier.getId())) {
                    classifierMap.remove(classifier.getId());
                }
            }
            for (ResourceGroupClassifier classifier : log.getClassifiers()) {
                classifierMap.put(classifier.getId(), classifier);
            }
            wg.setClassifiers(log.getClassifiers());
        }
        if (log.getCpuWeight() != null) {
            wg.setCpuWeight(log.getCpuWeight());
            wg.normalizeCpuWeight();
        }
        if (log.getExclusiveCpuCores() != null) {
            sumExclusiveCpuCores -= wg.getNormalizedExclusiveCpuCores();
            wg.setExclusiveCpuCores(log.getExclusiveCpuCores());
            sumExclusiveCpuCores += wg.getNormalizedExclusiveCpuCores();
        }
        if (log.getMaxCpuCores() != null) {
            wg.setMaxCpuCores(log.getMaxCpuCores());
        }
        if (log.getMemLimit() != null) {
            wg.setMemLimit(log.getMemLimit());
        }
        if (log.getBigQueryMemLimit() != null) {
            wg.setBigQueryMemLimit(log.getBigQueryMemLimit());
        }
        if (log.getBigQueryScanRowsLimit() != null) {
            wg.setBigQueryScanRowsLimit(log.getBigQueryScanRowsLimit());
        }
        if (log.getBigQueryCpuSecondLimit() != null) {
            wg.setBigQueryCpuSecondLimit(log.getBigQueryCpuSecondLimit());
        }
        if (log.getConcurrencyLimit() != null) {
            wg.setConcurrencyLimit(log.getConcurrencyLimit());
        }
        if (log.getSpillMemLimitThreshold() != null) {
            wg.setSpillMemLimitThreshold(log.getSpillMemLimitThreshold());
        }
        if (log.getVersion() != 0) {
            wg.setVersion(log.getVersion());
        }
    }

    public void replayAlterResourceGroup(AlterResourceGroupLog log) {
        writeLock();
        try {
            ResourceGroup wg = resourceGroupMap.get(log.getName());
            if (wg == null) {
                return;
            }
            updateResourceGroup(wg, log);
        } finally {
            writeUnlock();
        }
    }

    private static class SerializeData {
        @SerializedName("WorkGroups")
        public List<ResourceGroup> resourceGroups;
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        int numJson = 1 + resourceGroupMap.size();
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.RESOURCE_GROUP_MGR, numJson);
        writer.writeInt(resourceGroupMap.size());
        for (ResourceGroup resourceGroup : resourceGroupMap.values()) {
            writer.writeJson(resourceGroup);
        }

        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        List<ResourceGroup> resourceGroups = new ArrayList<>();
        reader.readCollection(ResourceGroup.class, resourceGroups::add);
        resourceGroups.sort(Comparator.comparing(ResourceGroup::getVersion));
        resourceGroups.forEach(this::replayAddResourceGroup);
    }
}
